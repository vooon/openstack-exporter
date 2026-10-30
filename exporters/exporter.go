package exporters

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"sync"

	"log/slog"

	gophercloudv2 "github.com/gophercloud/gophercloud/v2"
	clientutilsv2 "github.com/gophercloud/utils/v2/client"
	clientconfigv2 "github.com/gophercloud/utils/v2/openstack/clientconfig"
	"github.com/hashicorp/go-uuid"
	"github.com/mitchellh/go-homedir"
	"github.com/openstack-exporter/openstack-exporter/utils"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	//nolint: deadcode, unused
	BYTE = 1 << (10 * iota)
	//nolint: deadcode, unused
	KILOBYTE
	MEGABYTE
	GIGABYTE
	//nolint: deadcode, unused
	TERABYTE
)

// SupportedExporters lists the service names in the order the service flags
// are shown. RegisterExporter appends any service that is not listed here.
var SupportedExporters = []string{"network", "compute", "image", "volume", "identity", "object-store", "load-balancer", "container-infra", "dns", "baremetal", "gnocchi", "database", "orchestration", "placement", "sharev2"}

type OpenStackExporter interface {
	prometheus.Collector

	GetName() string
	// IsMetricEnabled reports whether any of the named metrics is collected.
	IsMetricEnabled(names ...string) bool
}

func EnableExporter(service string, opts ExporterOptions, logger *slog.Logger) (*OpenStackExporter, error) {
	exporter, err := NewExporter(service, opts, logger)
	if err != nil {
		return nil, err
	}
	return &exporter, nil
}

// ExporterOptions holds all user-supplied configuration for an exporter.
// It is passed to EnableExporter / NewExporter and embedded in ExporterConfig,
// which adds the resolved service client and service name.
type ExporterOptions struct {
	// Cloud is the name of the cloud entry in clouds.yaml to scrape.
	Cloud string
	// Prefix is the metric name prefix (default: "openstack").
	Prefix string
	// DisabledMetrics lists "exporter-metric" keys to suppress entirely
	// (e.g. "cinder-snapshots"). It wins over EnabledMetrics.
	DisabledMetrics []string
	// EnabledMetrics lists "exporter-metric" keys that stay enabled despite
	// DisableSlowMetrics or DisableDeprecatedMetrics (e.g. "nova-limits_vcpus_max").
	EnabledMetrics []string
	// EndpointType selects the OpenStack endpoint to connect to
	// ("public", "internal" or "admin").
	EndpointType string
	// CollectTime enables emission of per-metric collection duration.
	CollectTime bool
	// DisableSlowMetrics suppresses metrics marked Slow in their definition.
	DisableSlowMetrics bool
	// DisableDeprecatedMetrics suppresses metrics that carry a DeprecatedVersion.
	DisableDeprecatedMetrics bool
	// DisableCinderAgentUUID disables UUID generation for Cinder agent metrics.
	DisableCinderAgentUUID bool
	// DomainID restricts metric collection to a single Keystone domain.
	// Empty means all domains.
	DomainID string
	// TenantID restricts metric collection to a single project.
	// Empty means all projects.
	TenantID string
	// NovaMetadataMapping maps Nova server metadata keys to extra labels on
	// the openstack_nova_server_status metric.
	NovaMetadataMapping *utils.LabelMappingFlag
	// DnsConcurrentCount bounds concurrent requests when collecting DNS
	// recordsets.
	DnsConcurrentCount int
	// APIDetailConcurrentCount bounds concurrent per-resource detail requests
	// (per-project limits and quotas, per-router L3 agents). Zero or negative
	// selects the default.
	APIDetailConcurrentCount int
	// PlacementConcurrentCount bounds concurrent per-provider Placement
	// requests (inventories, usages, allocations). Zero or negative selects
	// the default.
	PlacementConcurrentCount int
	// UUIDGenFunc generates UUIDs for Cinder agents. Defaults to
	// uuid.GenerateUUID when nil.
	UUIDGenFunc func() (string, error)
}

type ExporterConfig struct {
	ExporterOptions
	ClientV2    *gophercloudv2.ServiceClient
	ServiceName string
}

type BaseOpenStackExporter struct {
	ExporterConfig
	Name   string
	logger *slog.Logger

	// DAG exporter state, populated by SetupExporter.
	metricEnabled       map[string]bool    // resolved policy, keyed by local metric name
	allDescs            []*prometheus.Desc // descriptors created by registerDescs and RegisterDesc
	upDesc              *prometheus.Desc
	scrapesTotal        prometheus.Counter
	scrapeErrors        prometheus.Counter
	sourceFetchDuration *prometheus.HistogramVec
}

var (
	endpointOptsV2   map[string]gophercloudv2.EndpointOpts
	endpointOptsV2Mu sync.Mutex
)

func (exporter *BaseOpenStackExporter) GetName() string {
	return fmt.Sprintf("%s_%s", exporter.Prefix, exporter.Name)
}

// log returns the exporter logger, falling back to the default logger when
// the exporter was built without one, as happens in unit tests.
func (exporter *BaseOpenStackExporter) log() *slog.Logger {
	if exporter.logger == nil {
		return slog.Default()
	}
	return exporter.logger
}

func (exporter *BaseOpenStackExporter) Describe(ch chan<- *prometheus.Desc) {
	if exporter.upDesc != nil {
		ch <- exporter.upDesc
		ch <- exporter.scrapesTotal.Desc()
		ch <- exporter.scrapeErrors.Desc()
	}
	if exporter.sourceFetchDuration != nil {
		exporter.sourceFetchDuration.Describe(ch)
	}
	for _, d := range exporter.allDescs {
		ch <- d
	}
}

func (exporter *BaseOpenStackExporter) GetDnsConcurrencyCount() int {
	return exporter.DnsConcurrentCount
}

const (
	defaultAPIDetailConcurrency = 10
	defaultPlacementConcurrency = 10
)

// GetAPIDetailConcurrencyCount returns the fan-out limit for per-resource
// detail requests.
func (exporter *BaseOpenStackExporter) GetAPIDetailConcurrencyCount() int {
	if exporter.APIDetailConcurrentCount <= 0 {
		return defaultAPIDetailConcurrency
	}
	return exporter.APIDetailConcurrentCount
}

// GetPlacementConcurrencyCount returns the fan-out limit for per-provider
// Placement requests.
func (exporter *BaseOpenStackExporter) GetPlacementConcurrencyCount() int {
	if exporter.PlacementConcurrentCount <= 0 {
		return defaultPlacementConcurrency
	}
	return exporter.PlacementConcurrentCount
}

// took from here:
// https://github.com/gophercloud/utils/blob/4c0f6d93d3a9b027a21d9206b6bdd09123de7a09/internal/util.go#L87
func pathOrContents(poc string) ([]byte, bool, error) {
	if len(poc) == 0 {
		return nil, false, nil
	}

	path := poc
	if path[0] == '~' {
		var err error
		path, err = homedir.Expand(path)
		if err != nil {
			return []byte(path), true, err
		}
	}

	if _, err := os.Stat(path); err == nil {
		contents, err := os.ReadFile(path)
		if err != nil {
			return contents, true, err
		}
		return contents, true, nil
	}

	return []byte(poc), false, nil
}

func NewExporter(name string, opts ExporterOptions, logger *slog.Logger) (OpenStackExporter, error) {
	var err error
	var transport http.RoundTripper
	var tlsConfig tls.Config

	optsv2 := clientconfigv2.ClientOpts{Cloud: opts.Cloud}

	config, err := clientconfigv2.GetCloudFromYAML(&optsv2)
	if err != nil {
		return nil, err
	}

	var configureTransport = false
	if !*config.Verify {
		logger.Info("SSL verification disabled on transport")
		tlsConfig.InsecureSkipVerify = true
		configureTransport = true
	} else if config.CACertFile != "" {
		certPool, err := additionalTLSTrust(config.CACertFile, logger)
		if err != nil {
			logger.Error("Failed to include additional certificates to ca-trust", "err", err)
		}
		tlsConfig.RootCAs = certPool
		configureTransport = true
	}

	// took from here:
	// https://github.com/gophercloud/utils/blob/4c0f6d93d3a9b027a21d9206b6bdd09123de7a09/internal/util.go#L65
	if config.ClientCertFile != "" && config.ClientKeyFile != "" {
		clientCert, _, err := pathOrContents(config.ClientCertFile)
		if err != nil {
			return nil, fmt.Errorf("error reading Client Cert: %s", err)
		}
		clientKey, _, err := pathOrContents(config.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("error reading Client Key: %s", err)
		}
		cert, err := tls.X509KeyPair(clientCert, clientKey)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
		configureTransport = true
	}
	if configureTransport {
		transport = &http.Transport{TLSClientConfig: &tlsConfig}
	}

	if _, ok := os.LookupEnv("OS_DEBUG"); ok {
		if transport == nil {
			transport = http.DefaultTransport
		}

		transport = &clientutilsv2.RoundTripper{
			Rt:     transport,
			Logger: &clientutilsv2.DefaultLogger{},
		}
	}

	clientV2, err := NewServiceClientV2(name, &optsv2, transport, opts.EndpointType)
	if err != nil {
		return nil, err
	}

	if opts.UUIDGenFunc == nil {
		opts.UUIDGenFunc = uuid.GenerateUUID
	}

	exporterConfig := ExporterConfig{
		ExporterOptions: opts,
		ClientV2:        clientV2,
		ServiceName:     name,
	}

	factory, ok := exporterRegistry[name]
	if !ok {
		return nil, fmt.Errorf("couldn't find a handler for %s exporter", name)
	}
	return factory(&exporterConfig, logger)
}
