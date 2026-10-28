package exporters

import (
	"fmt"
	"net/http"
	"os"
	"sync"

	"log/slog"

	gophercloudv2 "github.com/gophercloud/gophercloud/v2"
	clientutilsv2 "github.com/gophercloud/utils/v2/client"
	clientconfigv2 "github.com/gophercloud/utils/v2/openstack/clientconfig"
	"github.com/hashicorp/go-uuid"
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
	capabilities        *Capabilities      // what Bind found on the cloud; nil before the first bind
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

func NewExporter(name string, opts ExporterOptions, logger *slog.Logger) (OpenStackExporter, error) {
	var err error
	var transport http.RoundTripper

	optsv2 := clientconfigv2.ClientOpts{Cloud: opts.Cloud}

	if _, ok := os.LookupEnv("OS_DEBUG"); ok {
		transport = &clientutilsv2.RoundTripper{
			Rt:     getNewHttpTransport(),
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
