package exporters

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	gophercloudv2 "github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/baremetal/v1/nodes"
	"github.com/openstack-exporter/openstack-exporter/utils"
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	RegisterTypedExporter("baremetal", NewIronicExporter)
}

const (
	ironicLatestSupportedMicroversion = "1.90"
	// The detailed node endpoint enforces a positive limit and may omit
	// nodes_links even where its server-side default truncates the result, so
	// AllPages has nothing to follow and silently returns a partial node list.
	// listAllNodes follows the marker itself instead.
	ironicNodePageSize = 1000
)

// IronicExporter : extends BaseOpenStackExporter
type IronicExporter struct {
	BaseOpenStackExporter
	descs ironicDescs
	sched Schedule[ironicScrape]
}

type ironicDescs struct {
	Node                   *prometheus.Desc `metric:"node"                      labels:"id,name,provision_state,power_state,maintenance,maintenance_reason,conductor_group,traits,instance_uuid,lessee,last_error,serial_number,console_enabled,resource_class,deploy_kernel,deploy_ramdisk,retired,retired_reason"`
	NodeUpdatedAt          *prometheus.Desc `metric:"node_updated_at"           labels:"id,name,provision_state"`
	NodeProvisionUpdatedAt *prometheus.Desc `metric:"node_provision_updated_at" labels:"id,name,provision_state"`
}

type ironicScrape struct {
	nodes []nodes.Node
}

// NewIronicExporter : returns a pointer to IronicExporter
func NewIronicExporter(config *ExporterConfig, logger *slog.Logger) (*IronicExporter, error) {
	ctx := context.TODO()

	// NOTE(Sharpz7) Gophercloud V2 adds this new field ResourceBase.
	// For whatever reason, it adds a v1 field to the URL,
	// so it sends requests to /v1/v1 if left unfixed.
	//config.ClientV2.ResourceBase = config.ClientV2.Endpoint

	err := utils.SetupClientMicroversionV2(ctx, config.ClientV2, "OS_BAREMETAL_API_VERSION", ironicLatestSupportedMicroversion, logger)
	if err != nil {
		return nil, err
	}

	e := &IronicExporter{
		BaseOpenStackExporter: BaseOpenStackExporter{
			Name:           "ironic",
			ExporterConfig: *config,
			logger:         logger,
		},
	}
	graph := Graph[ironicScrape]{
		Sources: []Source[ironicScrape]{
			{Name: "nodes", Fetch: e.fetchNodes},
		},
		Emitters: []Emitter[ironicScrape]{
			{Name: "nodes", Metrics: []string{"node", "node_updated_at", "node_provision_updated_at"}, Sources: []string{"nodes"}, Emit: e.emitNodes},
		},
	}
	sched, err := SetupExporter(&e.BaseOpenStackExporter, &e.descs, &graph)
	if err != nil {
		return nil, err
	}
	e.sched = sched
	return e, nil
}

func (e *IronicExporter) Collect(ch chan<- prometheus.Metric) {
	CollectSchedule(&e.BaseOpenStackExporter, &e.sched, new(ironicScrape), ch)
}

// listAllNodes returns every node, paging on an explicit marker under a stable
// sort so that deployments with more nodes than the API returns in one response
// are collected completely.
func listAllNodes(ctx context.Context, client *gophercloudv2.ServiceClient) ([]nodes.Node, error) {
	var allNodes []nodes.Node
	var marker string

	for {
		allPagesNodes, err := nodes.ListDetail(client, nodes.ListOpts{
			Limit:   ironicNodePageSize,
			Marker:  marker,
			SortKey: "id",
			SortDir: "asc",
		}).AllPages(ctx)
		if err != nil {
			return nil, err
		}

		page, err := nodes.ExtractNodes(allPagesNodes)
		if err != nil {
			return nil, err
		}

		if len(page) == 0 {
			return allNodes, nil
		}

		allNodes = append(allNodes, page...)
		marker = page[len(page)-1].UUID
		if marker == "" {
			return nil, fmt.Errorf("ironic: node page of %d entries ends without a UUID", len(page))
		}
	}
}

func (e *IronicExporter) fetchNodes(ctx context.Context, s *ironicScrape) error {
	var err error
	s.nodes, err = listAllNodes(ctx, e.ClientV2)
	return err
}

func (e *IronicExporter) emitNodes(_ context.Context, s *ironicScrape, ch chan<- prometheus.Metric) error {
	for i := range s.nodes {
		node := &s.nodes[i]
		deployKernel := getDriverInfoString(node.DriverInfo, "deploy_kernel")
		deployRamdisk := getDriverInfoString(node.DriverInfo, "deploy_ramdisk")
		serialNumber := getNestedExtraString(node.Extra, "system_vendor", "serial_number")

		emitGauge(ch, e.descs.Node, 1.0, node.UUID, node.Name, node.ProvisionState, node.PowerState,
			strconv.FormatBool(node.Maintenance), sanitizeMetricString(node.MaintenanceReason), node.ConductorGroup, strings.Join(node.Traits, " "),
			node.InstanceUUID, node.Lessee, sanitizeMetricString(node.LastError), serialNumber, strconv.FormatBool(node.ConsoleEnabled), node.ResourceClass,
			deployKernel, deployRamdisk, strconv.FormatBool(node.Retired), node.RetiredReason)

		if !node.UpdatedAt.IsZero() {
			emitGauge(ch, e.descs.NodeUpdatedAt, float64(node.UpdatedAt.Unix()), node.UUID, node.Name, node.ProvisionState)
		}

		if !node.ProvisionUpdatedAt.IsZero() {
			emitGauge(ch, e.descs.NodeProvisionUpdatedAt, float64(node.ProvisionUpdatedAt.Unix()), node.UUID, node.Name, node.ProvisionState)
		}
	}

	return nil
}

func getExtraString(extra map[string]any, key string) string {
	if extra == nil {
		return ""
	}

	value, ok := extra[key]
	if !ok {
		return ""
	}

	s, ok := value.(string)
	if !ok {
		return ""
	}

	return s
}

func getNestedExtraString(extra map[string]any, key string, nestedKey string) string {
	if extra == nil {
		return ""
	}

	nested, ok := extra[key].(map[string]any)
	if !ok {
		return ""
	}

	return getExtraString(nested, nestedKey)
}

func sanitizeMetricString(value string) string {
	return strings.TrimSpace(strings.ReplaceAll(value, "\n", " "))
}

func getDriverInfoString(driverInfo map[string]any, key string) string {
	v, ok := driverInfo[key]
	if !ok {
		return ""
	}

	s, ok := v.(string)
	if !ok {
		return ""
	}

	return s
}
