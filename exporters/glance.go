package exporters

import (
	"context"
	"log/slog"
	"strconv"

	"github.com/gophercloud/gophercloud/v2/openstack/image/v2/images"
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	RegisterTypedExporter("image", NewGlanceExporter)
}

type GlanceExporter struct {
	BaseOpenStackExporter
	descs glanceDescs
	sched Schedule[glanceScrape]
}

type glanceDescs struct {
	Images         *prometheus.Desc `metric:"images"`
	ImageBytes     *prometheus.Desc `metric:"image_bytes"      labels:"id,name,tenant_id,image_type"                                     slow:"true"`
	ImageCreatedAt *prometheus.Desc `metric:"image_created_at" labels:"id,name,tenant_id,visibility,hidden,status,image_type" slow:"true"`
}

type glanceScrape struct {
	images []images.Image
}

func NewGlanceExporter(config *ExporterConfig, logger *slog.Logger) (*GlanceExporter, error) {
	e := &GlanceExporter{
		BaseOpenStackExporter: BaseOpenStackExporter{
			Name:           "glance",
			ExporterConfig: *config,
			logger:         logger,
		},
	}
	graph := Graph[glanceScrape]{
		Sources: []Source[glanceScrape]{
			{Name: "images", Fetch: e.fetchImages},
		},
		Emitters: []Emitter[glanceScrape]{
			{Name: "count", Metrics: []string{"images"}, Sources: []string{"images"}, Emit: e.emitCount},
			{Name: "properties", Metrics: []string{"image_bytes", "image_created_at"}, Sources: []string{"images"}, Emit: e.emitProperties},
		},
	}
	sched, err := SetupExporter(&e.BaseOpenStackExporter, &e.descs, &graph)
	if err != nil {
		return nil, err
	}
	e.sched = sched
	return e, nil
}

func (e *GlanceExporter) Collect(ch chan<- prometheus.Metric) {
	CollectSchedule(&e.BaseOpenStackExporter, &e.sched, new(glanceScrape), ch)
}

func (e *GlanceExporter) fetchImages(ctx context.Context, s *glanceScrape) error {
	allPages, err := images.List(e.ClientV2, images.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.images, err = images.ExtractImages(allPages)
	return err
}

func (e *GlanceExporter) emitCount(_ context.Context, s *glanceScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.Images, float64(len(s.images)))
	return nil
}

func (e *GlanceExporter) emitProperties(_ context.Context, s *glanceScrape, ch chan<- prometheus.Metric) error {
	for i := range s.images {
		image := &s.images[i]
		imageType := getImageType(image)
		emitGauge(ch, e.descs.ImageBytes, float64(image.SizeBytes), image.ID, image.Name, image.Owner, imageType)
		emitGauge(ch, e.descs.ImageCreatedAt, float64(image.CreatedAt.Unix()), image.ID, image.Name,
			image.Owner, string(image.Visibility), strconv.FormatBool(image.Hidden), string(image.Status), imageType)
	}
	return nil
}

// getImageType returns the "image_type" image property set by nova, e.g.
// "snapshot" for server snapshots or "backup" for server backups. Images
// uploaded directly to glance do not carry this property and are reported
// as "image".
func getImageType(image *images.Image) string {
	if imageType, ok := image.Properties["image_type"].(string); ok && imageType != "" {
		return imageType
	}
	return "image"
}
