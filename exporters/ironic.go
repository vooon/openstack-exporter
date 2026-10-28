package exporters

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
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
	// nodeDesc carries the fixed node labels plus the operator-configured
	// IronicNodeExtraLabels, so it is built at construction instead of from a
	// struct tag.
	nodeDesc   *prometheus.Desc
	nodeExtras *utils.LabelMappingFlag
}

type ironicDescs struct {
	NodeUpdatedAt          *prometheus.Desc `metric:"node_updated_at"           labels:"id,name,provision_state"`
	NodeProvisionUpdatedAt *prometheus.Desc `metric:"node_provision_updated_at" labels:"id,name,provision_state"`
}

// ironicNodeBaseLabels are the fixed labels of the node metric, before the
// operator-configured labels are appended.
var ironicNodeBaseLabels = []string{"id", "name", "provision_state", "power_state", "maintenance", "maintenance_reason",
	"conductor_group", "traits", "instance_uuid", "lessee", "last_error", "serial_number", "console_enabled", "resource_class",
	"retired", "retired_reason"}

// DefaultIronicNodeExtraLabels reproduces the deploy_kernel and deploy_ramdisk
// labels that openstack_ironic_node carried before the mapping existed, so the
// default output is unchanged. Operators who do not want these labels can drop
// them by passing an empty value.
const DefaultIronicNodeExtraLabels = "driver_info.deploy_kernel,driver_info.deploy_ramdisk"

// NewIronicNodeExtraLabels returns the default node label mapping, for callers
// that build ExporterOptions without going through the CLI flag.
func NewIronicNodeExtraLabels() *utils.LabelMappingFlag {
	m := &utils.LabelMappingFlag{DeriveLabelFromLeaf: true}
	if err := m.Set(DefaultIronicNodeExtraLabels); err != nil {
		panic(err) // constant input, cannot fail
	}
	return m
}

// ironicNodeInfoMaps are the node dictionaries whose keys may be mapped to
// labels. A mapping key is qualified with one of these names, for example
// `driver_info.deploy_kernel`.
var ironicNodeInfoMaps = []string{"driver_info", "instance_info", "extra", "properties", "driver_internal_info"}

// IronicNodeInfoMaps returns the node dictionaries whose keys can be mapped to
// labels, for use in the CLI flag help.
func IronicNodeInfoMaps() []string {
	return slices.Clone(ironicNodeInfoMaps)
}

// ironicNodeNestedMaps exposes the allowlisted node dictionaries to the label
// mapping helper, which owns the lookup and redaction rules.
func ironicNodeNestedMaps(node *nodes.Node) []utils.NestedMap {
	return []utils.NestedMap{
		{Name: "driver_info", Values: node.DriverInfo},
		{Name: "instance_info", Values: node.InstanceInfo},
		{Name: "extra", Values: node.Extra},
		{Name: "properties", Values: node.Properties},
		{Name: "driver_internal_info", Values: node.DriverInternalInfo},
	}
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
		nodeExtras: config.IronicNodeExtraLabels,
	}
	if e.nodeExtras == nil {
		e.nodeExtras = NewIronicNodeExtraLabels()
	}
	if err := e.nodeExtras.ValidateNestedKeys(ironicNodeInfoMaps...); err != nil {
		return nil, fmt.Errorf("ironic.node-extra-labels: %w", err)
	}
	if err := e.nodeExtras.ValidateAgainst(ironicNodeBaseLabels); err != nil {
		return nil, fmt.Errorf("ironic.node-extra-labels: %w", err)
	}

	graph := Graph[ironicScrape]{
		Sources: []Source[ironicScrape]{
			{Name: "nodes", Fetch: e.fetchNodes},
		},
		Emitters: []Emitter[ironicScrape]{
			{Name: "nodes", Metrics: []string{"node", "node_updated_at", "node_provision_updated_at"}, Sources: []string{"nodes"}, Emit: e.emitNodes},
		},
	}
	// node carries operator-configured labels, so its descriptor cannot come
	// from a struct tag and is declared dynamically.
	nodeMetric := WithDynamicMetrics([]string{"node"}, func(base *BaseOpenStackExporter) {
		if !base.IsMetricEnabled("node") {
			return
		}
		labels := append(slices.Clone(ironicNodeBaseLabels), e.nodeExtras.Labels...)
		e.nodeDesc = prometheus.NewDesc(prometheus.BuildFQName(base.GetName(), "", "node"), "node", labels, nil)
		base.RegisterDesc(e.nodeDesc)
	})
	sched, err := SetupExporter(&e.BaseOpenStackExporter, &e.descs, &graph, nodeMetric)
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
		serialNumber := getNestedExtraString(node.Extra, "system_vendor", "serial_number")

		labelValues := []string{node.UUID, node.Name, node.ProvisionState, node.PowerState,
			strconv.FormatBool(node.Maintenance), sanitizeMetricString(node.MaintenanceReason), node.ConductorGroup, strings.Join(node.Traits, " "),
			node.InstanceUUID, node.Lessee, sanitizeMetricString(node.LastError), serialNumber, strconv.FormatBool(node.ConsoleEnabled), node.ResourceClass,
			strconv.FormatBool(node.Retired), node.RetiredReason}
		labelValues = append(labelValues, e.nodeExtras.ExtractNestedAny(ironicNodeNestedMaps(node)...)...)
		emitGauge(ch, e.nodeDesc, 1.0, labelValues...)

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
