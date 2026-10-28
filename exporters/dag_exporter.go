package exporters

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// This file wires BaseOpenStackExporter to the DAG engine in dag.go: the
// exporter factory registry, the construction sequence (SetupExporter) and
// the per-scrape collection sequence (CollectSchedule).

// ---------------------------------------------------------------------------
// Exporter factory registry
// ---------------------------------------------------------------------------

// ExporterFactory constructs an exporter for a resolved service client.
type ExporterFactory func(config *ExporterConfig, logger *slog.Logger) (OpenStackExporter, error)

var exporterRegistry = map[string]ExporterFactory{}

// RegisterExporter registers a factory for an OpenStack service name, such
// as "compute". Call it from the exporter's init(); NewExporter then
// dispatches to the factory and the service appears in SupportedExporters.
func RegisterExporter(serviceName string, factory ExporterFactory) {
	exporterRegistry[serviceName] = factory
	if !slices.Contains(SupportedExporters, serviceName) {
		SupportedExporters = append(SupportedExporters, serviceName)
	}
}

// RegisterTypedExporter adapts a constructor returning a concrete exporter
// type, so init() functions need no wrapper closure.
func RegisterTypedExporter[T OpenStackExporter](
	serviceName string,
	factory func(config *ExporterConfig, logger *slog.Logger) (T, error),
) {
	RegisterExporter(serviceName, func(config *ExporterConfig, logger *slog.Logger) (OpenStackExporter, error) {
		return factory(config, logger)
	})
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

// SetupOption customises SetupExporter.
type SetupOption func(*BaseOpenStackExporter) error

// WithDynamicMetrics declares metrics whose descriptors are built at runtime
// because their label set is not fixed. The callback runs after the policy
// is resolved and before pruning, so a dynamic metric participates in
// pruning like a tagged one; it should check IsMetricEnabled, build the
// descriptor and hand it to RegisterDesc.
func WithDynamicMetrics(names []string, build func(*BaseOpenStackExporter)) SetupOption {
	return func(base *BaseOpenStackExporter) error {
		for _, name := range names {
			base.declareDynamicMetric(name)
		}
		build(base)
		return nil
	}
}

// SetupExporter performs the whole construction sequence of a DAG exporter:
// parse the metric declarations in descs, resolve the enable/disable policy,
// create descriptors for the enabled metrics, prune the graph to the nodes
// those metrics need, and log the resulting schedule.
//
// Keeping these steps in one call is deliberate: descriptor creation and
// pruning must see the same policy, and pruning must run last. The returned
// schedule holds a reference to g, so g must not be modified afterwards.
func SetupExporter[S any](base *BaseOpenStackExporter, descs any, g *Graph[S], opts ...SetupOption) (Schedule[S], error) {
	base.initInstrumentation()
	base.resolveMetrics(parseMetricDefs(descs))

	for _, opt := range opts {
		if err := opt(base); err != nil {
			return Schedule[S]{}, err
		}
	}

	base.registerDescs(descs)

	sched, err := g.PruneSchedule(base.metricPolicy())
	if err != nil {
		return Schedule[S]{}, err
	}
	base.logSchedule(&sched)
	return sched, nil
}

// initInstrumentation creates the up gauge, the scrape counters and, when
// --collect-metric-time is set, the per-source duration histogram. It is
// idempotent.
func (exporter *BaseOpenStackExporter) initInstrumentation() {
	if exporter.upDesc != nil {
		return
	}
	exporter.upDesc = prometheus.NewDesc(prometheus.BuildFQName(exporter.GetName(), "", "up"), "up", nil, nil)

	service := prometheus.Labels{"service": exporter.Name}
	exporter.scrapesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        prometheus.BuildFQName(exporter.Prefix, "exporter", "scrapes_total"),
		Help:        "Total number of scrapes.",
		ConstLabels: service,
	})
	exporter.scrapeErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        prometheus.BuildFQName(exporter.Prefix, "exporter", "scrape_errors_total"),
		Help:        "Total number of scrapes in which at least one source or emitter failed.",
		ConstLabels: service,
	})
	if exporter.CollectTime {
		exporter.sourceFetchDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        prometheus.BuildFQName(exporter.Prefix, "exporter", "source_fetch_duration_seconds"),
			Help:        "Duration of source fetches in seconds.",
			Buckets:     []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60},
			ConstLabels: service,
		}, []string{"source"})
	}
}

func (exporter *BaseOpenStackExporter) logSchedule(sched interface {
	Waves() [][]string
	PrunedSources() []string
	PrunedEmitters() []string
}) {
	logger := exporter.log()
	for i, wave := range sched.Waves() {
		logger.Debug("DAG wave", "exporter", exporter.Name, "wave", i, "nodes", strings.Join(wave, ", "))
	}
	if pruned := sched.PrunedSources(); len(pruned) > 0 {
		logger.Debug("DAG pruned sources (no live emitter needs them)", "exporter", exporter.Name, "sources", strings.Join(pruned, ", "))
	}
	if pruned := sched.PrunedEmitters(); len(pruned) > 0 {
		logger.Debug("DAG pruned emitters (all metrics disabled)", "exporter", exporter.Name, "emitters", strings.Join(pruned, ", "))
	}
}

// ---------------------------------------------------------------------------
// Collection
// ---------------------------------------------------------------------------

// CollectSchedule is the Collect implementation of a DAG exporter: it runs
// the schedule against a fresh state, records the outcome in the scrape
// counters and the optional duration histogram, and emits the up gauge.
// up is 1 when at least one source fetched successfully.
func CollectSchedule[S any](base *BaseOpenStackExporter, sched *Schedule[S], state *S, ch chan<- prometheus.Metric) {
	base.scrapesTotal.Inc()
	result := sched.Run(context.Background(), state, ch, base.runHooks())
	if result.SourcesFailed > 0 || result.EmittersFailed > 0 {
		base.scrapeErrors.Inc()
	}

	ch <- base.scrapesTotal
	ch <- base.scrapeErrors
	if base.sourceFetchDuration != nil {
		base.sourceFetchDuration.Collect(ch)
	}

	up := 0.0
	if result.Healthy() {
		up = 1
	}
	ch <- prometheus.MustNewConstMetric(base.upDesc, prometheus.GaugeValue, up)
}

func (exporter *BaseOpenStackExporter) runHooks() RunHooks {
	logger := exporter.log()
	return RunHooks{
		SourceDone: func(name string, elapsed time.Duration, err error) {
			if exporter.sourceFetchDuration != nil {
				exporter.sourceFetchDuration.WithLabelValues(name).Observe(elapsed.Seconds())
			}
			if err != nil {
				logger.Error("Source fetch failed", "exporter", exporter.Name, "source", name, "err", err)
			}
		},
		EmitterDone: func(name string, _ time.Duration, err error) {
			if err != nil {
				logger.Error("Emitter failed", "exporter", exporter.Name, "emitter", name, "err", err)
			}
		},
	}
}
