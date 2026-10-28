package exporters

import (
	"context"
	"log/slog"

	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/containers"
	"github.com/gophercloud/gophercloud/v2/pagination"
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	RegisterTypedExporter("object-store", NewObjectStoreExporter)
}

type ObjectStoreExporter struct {
	BaseOpenStackExporter
	descs objectStoreDescs
	sched Schedule[objectStoreScrape]
}

type objectStoreDescs struct {
	Objects *prometheus.Desc `metric:"objects" labels:"container_name"`
	Bytes   *prometheus.Desc `metric:"bytes"   labels:"container_name"`
}

type objectStoreScrape struct {
	containers []containers.Container
}

func NewObjectStoreExporter(config *ExporterConfig, logger *slog.Logger) (*ObjectStoreExporter, error) {
	e := &ObjectStoreExporter{
		BaseOpenStackExporter: BaseOpenStackExporter{
			Name:           "object_store",
			ExporterConfig: *config,
			logger:         logger,
		},
	}
	graph := Graph[objectStoreScrape]{
		Sources: []Source[objectStoreScrape]{
			{Name: "containers", Fetch: e.fetchContainers},
		},
		Emitters: []Emitter[objectStoreScrape]{
			{Name: "containers", Metrics: []string{"objects", "bytes"}, Sources: []string{"containers"}, Emit: e.emitContainers},
		},
	}
	sched, err := SetupExporter(&e.BaseOpenStackExporter, &e.descs, &graph)
	if err != nil {
		return nil, err
	}
	e.sched = sched
	return e, nil
}

func (e *ObjectStoreExporter) Collect(ch chan<- prometheus.Metric) {
	CollectSchedule(&e.BaseOpenStackExporter, &e.sched, new(objectStoreScrape), ch)
}

// fetchContainers lists the account's containers page by page; the listing
// already carries each container's object count and byte size.
func (e *ObjectStoreExporter) fetchContainers(ctx context.Context, s *objectStoreScrape) error {
	return containers.List(e.ClientV2, containers.ListOpts{}).EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		containerList, err := containers.ExtractInfo(page)
		if err != nil {
			return false, err
		}
		s.containers = append(s.containers, containerList...)
		return true, nil
	})
}

func (e *ObjectStoreExporter) emitContainers(_ context.Context, s *objectStoreScrape, ch chan<- prometheus.Metric) error {
	for i := range s.containers {
		c := &s.containers[i]
		emitGauge(ch, e.descs.Objects, float64(c.Count), c.Name)
		emitGauge(ch, e.descs.Bytes, float64(c.Bytes), c.Name)
	}
	return nil
}
