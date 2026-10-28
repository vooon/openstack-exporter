package exporters

import (
	"context"
	"log/slog"

	gophercloudv2 "github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/placement/v1/resourceproviders"
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	RegisterTypedExporter("placement", NewPlacementExporter)
}

type PlacementExporter struct {
	BaseOpenStackExporter
	descs placementDescs
	sched Schedule[placementScrape]
}

type placementDescs struct {
	ResourceTotal               *prometheus.Desc `metric:"resource_total"                labels:"hostname,resourcetype"`
	ResourceAllocationRatio     *prometheus.Desc `metric:"resource_allocation_ratio"     labels:"hostname,resourcetype"`
	ResourceGeneration          *prometheus.Desc `metric:"resource_generation"           labels:"hostname,resourcetype"`
	ResourceReserved            *prometheus.Desc `metric:"resource_reserved"             labels:"hostname,resourcetype"`
	ResourceUsage               *prometheus.Desc `metric:"resource_usage"                labels:"hostname,resourcetype"`
	ResourceProviderAllocations *prometheus.Desc `metric:"resource_provider_allocations" labels:"hostname,uuid,resourcetype"`
}

// placementScrape holds the provider list and, indexed like it, the per
// provider results. The per-provider slices are filled by independent
// sources, so each lives in its own field.
type placementScrape struct {
	providers   []resourceproviders.ResourceProvider
	inventories []*resourceproviders.ResourceProviderInventories
	usages      []*resourceproviders.ResourceProviderUsage
	allocations []*resourceproviders.ResourceProviderAllocations
}

func NewPlacementExporter(config *ExporterConfig, logger *slog.Logger) (*PlacementExporter, error) {
	e := &PlacementExporter{
		BaseOpenStackExporter: BaseOpenStackExporter{
			Name:           "placement",
			ExporterConfig: *config,
			logger:         logger,
		},
	}
	graph := Graph[placementScrape]{
		Sources: []Source[placementScrape]{
			{Name: "providers", Fetch: e.fetchProviders},
			{Name: "inventories", DependsOn: []string{"providers"}, Fetch: e.fetchInventories},
			{Name: "usages", DependsOn: []string{"providers"}, Fetch: e.fetchUsages},
			{Name: "allocations", DependsOn: []string{"providers"}, Fetch: e.fetchAllocations},
		},
		Emitters: []Emitter[placementScrape]{
			{Name: "inventories", Metrics: []string{"resource_total", "resource_allocation_ratio", "resource_generation", "resource_reserved"}, Sources: []string{"inventories"}, Emit: e.emitInventories},
			{Name: "usages", Metrics: []string{"resource_usage"}, Sources: []string{"usages"}, Emit: e.emitUsages},
			{Name: "allocations", Metrics: []string{"resource_provider_allocations"}, Sources: []string{"allocations"}, Emit: e.emitAllocations},
		},
	}
	sched, err := SetupExporter(&e.BaseOpenStackExporter, &e.descs, &graph)
	if err != nil {
		return nil, err
	}
	e.sched = sched
	return e, nil
}

func (e *PlacementExporter) Collect(ch chan<- prometheus.Metric) {
	CollectSchedule(&e.BaseOpenStackExporter, &e.sched, new(placementScrape), ch)
}

func (e *PlacementExporter) fetchProviders(ctx context.Context, s *placementScrape) error {
	return fetchPlacementProviders(ctx, e.ClientV2, s)
}

func (e *PlacementExporter) fetchInventories(ctx context.Context, s *placementScrape) error {
	return fetchPlacementInventories(ctx, e.ClientV2, s)
}

func (e *PlacementExporter) fetchUsages(ctx context.Context, s *placementScrape) error {
	return fetchPlacementUsages(ctx, e.ClientV2, s)
}

func (e *PlacementExporter) fetchAllocations(ctx context.Context, s *placementScrape) error {
	return fetchPlacementAllocations(ctx, e.ClientV2, s)
}

func (e *PlacementExporter) emitInventories(_ context.Context, s *placementScrape, ch chan<- prometheus.Metric) error {
	emitPlacementInventories(&e.descs, s, ch)
	return nil
}

func (e *PlacementExporter) emitUsages(_ context.Context, s *placementScrape, ch chan<- prometheus.Metric) error {
	emitPlacementUsages(&e.descs, s, ch)
	return nil
}

func (e *PlacementExporter) emitAllocations(_ context.Context, s *placementScrape, ch chan<- prometheus.Metric) error {
	emitPlacementAllocations(&e.descs, s, ch)
	return nil
}

func fetchPlacementProviders(ctx context.Context, client *gophercloudv2.ServiceClient, s *placementScrape) error {
	allPages, err := resourceproviders.List(client, resourceproviders.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.providers, err = resourceproviders.ExtractResourceProviders(allPages)
	return err
}

// The per-provider fetches below walk the providers one request at a time,
// as the legacy collector did; a failed provider fails the whole source.

func fetchPlacementInventories(ctx context.Context, client *gophercloudv2.ServiceClient, s *placementScrape) error {
	s.inventories = make([]*resourceproviders.ResourceProviderInventories, len(s.providers))
	for i := range s.providers {
		result, err := resourceproviders.GetInventories(ctx, client, s.providers[i].UUID).Extract()
		if err != nil {
			return err
		}
		s.inventories[i] = result
	}
	return nil
}

func fetchPlacementUsages(ctx context.Context, client *gophercloudv2.ServiceClient, s *placementScrape) error {
	s.usages = make([]*resourceproviders.ResourceProviderUsage, len(s.providers))
	for i := range s.providers {
		result, err := resourceproviders.GetUsages(ctx, client, s.providers[i].UUID).Extract()
		if err != nil {
			return err
		}
		s.usages[i] = result
	}
	return nil
}

func fetchPlacementAllocations(ctx context.Context, client *gophercloudv2.ServiceClient, s *placementScrape) error {
	s.allocations = make([]*resourceproviders.ResourceProviderAllocations, len(s.providers))
	for i := range s.providers {
		result, err := resourceproviders.GetAllocations(ctx, client, s.providers[i].UUID).Extract()
		if err != nil {
			return err
		}
		s.allocations[i] = result
	}
	return nil
}

func emitPlacementInventories(descs *placementDescs, s *placementScrape, ch chan<- prometheus.Metric) {
	for i := range s.inventories {
		name := s.providers[i].Name
		inventories := s.inventories[i]
		generation := float64(inventories.ResourceProviderGeneration)
		for k, v := range inventories.Inventories {
			emitGauge(ch, descs.ResourceTotal, float64(v.Total), name, k)
			emitGauge(ch, descs.ResourceAllocationRatio, float64(v.AllocationRatio), name, k)
			emitGauge(ch, descs.ResourceGeneration, generation, name, k)
			emitGauge(ch, descs.ResourceReserved, float64(v.Reserved), name, k)
		}
	}
}

func emitPlacementUsages(descs *placementDescs, s *placementScrape, ch chan<- prometheus.Metric) {
	for i := range s.usages {
		name := s.providers[i].Name
		for k, v := range s.usages[i].Usages {
			emitGauge(ch, descs.ResourceUsage, float64(v), name, k)
		}
	}
}

func emitPlacementAllocations(descs *placementDescs, s *placementScrape, ch chan<- prometheus.Metric) {
	for i := range s.allocations {
		name := s.providers[i].Name
		for consumerID, allocation := range s.allocations[i].Allocations {
			for resourceClass, amount := range allocation.Resources {
				emitGauge(ch, descs.ResourceProviderAllocations, float64(amount), name, consumerID, resourceClass)
			}
		}
	}
}

// ListPlacementResourceProviders collects every placement metric of exporter
// in one sequential pass outside the schedule: the provider list, then the
// inventories, usages and (when resource_provider_allocations is enabled)
// allocations of each provider. It is kept for placement_benchmark_test.go,
// which times this path on the base of an exporter built by
// NewPlacementExporter; Collect runs the DAG schedule instead.
func ListPlacementResourceProviders(ctx context.Context, exporter *BaseOpenStackExporter, ch chan<- prometheus.Metric) error {
	var descs placementDescs
	exporter.registerDescs(&descs)

	s := new(placementScrape)
	if err := fetchPlacementProviders(ctx, exporter.ClientV2, s); err != nil {
		return err
	}
	if err := fetchPlacementInventories(ctx, exporter.ClientV2, s); err != nil {
		return err
	}
	if err := fetchPlacementUsages(ctx, exporter.ClientV2, s); err != nil {
		return err
	}
	if descs.ResourceProviderAllocations != nil {
		if err := fetchPlacementAllocations(ctx, exporter.ClientV2, s); err != nil {
			return err
		}
	}

	emitPlacementInventories(&descs, s, ch)
	emitPlacementUsages(&descs, s, ch)
	emitPlacementAllocations(&descs, s, ch)
	return nil
}
