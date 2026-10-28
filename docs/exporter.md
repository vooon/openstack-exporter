# Exporter Internals

<!-- markdownlint-disable MD013 -->

This document describes how an exporter is wired, how a scrape runs, and how
to write a new exporter on the DAG collection model.

## Runtime flow

Each exporter registers itself during package initialisation:

```go
func init() {
    RegisterTypedExporter("image", NewGlanceExporter)
}
```

The first argument is the OpenStack service selector (`compute`, `network`,
`volume`, ...). It names the Keystone catalog entry, the
`--disable-service.<service>` flag and the `/probe` include/exclude filters.
Registration appends it to `SupportedExporters`.

On startup `main`:

1. Builds the service flags from `SupportedExporters` and parses the CLI.
2. Resolves the active services from Keystone autodetection or explicit flags.
3. Builds one `exporters.ExporterOptions` value with cloud, prefix,
   enable/disable lists, endpoint type and per-exporter options.
4. Starts the cache worker when `--cache` is set, and the HTTP server.

`exporters.NewExporter(serviceName, opts, logger)` loads the cloud from
`clouds.yaml`, builds the HTTP transport and the Gophercloud service client,
wraps everything in `ExporterConfig` and calls the registered factory.

Two names must be kept apart:

* the service name, used for registration, catalog lookup and service flags
  (`compute`), and
* the metric namespace, `BaseOpenStackExporter.Name`, used in metric names and
  in `--disable-metric` / `--enable-metric` keys (`nova`).

With the default prefix, Nova's `total_vms` is `openstack_nova_total_vms` and
its option key is `nova-total_vms`. Keep both stable: users have them in
Prometheus jobs.

## Exporter shape

```go
type GlanceExporter struct {
    BaseOpenStackExporter
    descs glanceDescs
    sched Schedule[glanceScrape]
}

type glanceDescs struct {
    Images         *prometheus.Desc `metric:"images"`
    ImageBytes     *prometheus.Desc `metric:"image_bytes"      labels:"id,name,tenant_id" slow:"true"`
    ImageCreatedAt *prometheus.Desc `metric:"image_created_at" labels:"id,name,tenant_id,visibility,hidden,status" slow:"true"`
}

type glanceScrape struct {
    images []images.Image
}
```

The descriptor struct is the metric declaration. `SetupExporter` scans its
exported `*prometheus.Desc` fields and reads these tags:

Tag | Meaning
----|--------
`metric` | Local metric name. Defaults to the field name converted to snake_case.
`labels` | Comma-separated variable label names, in emission order.
`slow:"true"` | Suppressed by `--disable-slow-metrics` unless listed in `--enable-metric`.
`deprecated:"<version>"` | Suppressed by `--disable-deprecated-metrics` unless listed in `--enable-metric`.

A field stays `nil` when its metric is disabled. Emit through `emitGauge`,
which is a no-op for a nil descriptor, so emitters need no nil checks.

The scrape state struct holds everything fetched during one scrape. Keep API
response slices and derived data there, never on the exporter, so concurrent
scrapes cannot interfere.

## Metric policy

`SetupExporter` resolves every declared metric once, with this precedence:

1. listed in `--disable-metric`: disabled, absolutely;
2. listed in `--enable-metric`: enabled, overriding 3 and 4;
3. tagged `slow` and `--disable-slow-metrics`: disabled;
4. tagged `deprecated` and `--disable-deprecated-metrics`: disabled;
5. otherwise enabled.

Descriptor creation and graph pruning both read that one result, so a
disabled metric loses its descriptor *and* the API calls that only it needed.
`IsMetricEnabled(names...)` answers from the same policy; use it inside a
source to guard an extra request that feeds only some of the source's metrics.

Metrics whose label set is only known at runtime (Nova's `server_status` with
`--nova.metadata-extra-labels`) are declared with `WithDynamicMetrics`: the
callback runs after policy resolution, builds the descriptor when the metric
is enabled and hands it to `RegisterDesc`.

## DAG model

A graph has two node kinds:

* `Source` fetches OpenStack data into the scrape state. It may depend on
  other sources by name (`DependsOn`).
* `Emitter` reads the scrape state and emits metrics. It declares the metric
  names it may emit and the sources it needs.

```go
func NewGlanceExporter(config *ExporterConfig, logger *slog.Logger) (*GlanceExporter, error) {
    e := &GlanceExporter{BaseOpenStackExporter: BaseOpenStackExporter{
        Name:           "glance",
        ExporterConfig: *config,
        logger:         logger,
    }}
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
```

`SetupExporter` validates the graph (duplicate names, missing references,
cycles, an emitter naming a metric that was never declared), prunes it to the
nodes the enabled metrics need and logs the result at debug level.

`CollectSchedule` runs the pruned schedule on every scrape:

* a node starts as soon as its own dependencies have completed, so
  independent sources run concurrently;
* everything downstream of a failed source is skipped, so an emitter never
  sees partially fetched state and a failed list call produces absent
  metrics, not zeros;
* a panic in a fetch or emit function is turned into that node's error;
* `up` is 1 when at least one source succeeded.

### Rules for sources and emitters

* Concurrent sources write disjoint fields of the scrape state.
* A source that reads a field another source fills declares it in
  `DependsOn`.
* A source returns an error when its request fails. If it fans out over many
  items and wants its dependents to run despite individual failures, it must
  absorb those failures itself (record which items are missing in the state
  and return nil).
* Emitters treat the state as read-only and never call the API.
* Emitters emit through `emitGauge` unless they need another value type.

## Instrumentation

Every DAG exporter emits:

Metric | Labels | Meaning
-------|--------|--------
`<prefix>_<name>_up` | | 1 when at least one source succeeded in the last scrape
`<prefix>_exporter_scrapes_total` | `service` | scrapes of this exporter
`<prefix>_exporter_scrape_errors_total` | `service` | scrapes in which a source or emitter failed
`<prefix>_exporter_source_fetch_duration_seconds` | `service`, `source` | histogram, only with `--collect-metric-time`

## Adding a new exporter

1. Create `exporters/<name>.go` with `init()` calling `RegisterTypedExporter`.
2. Define the exporter struct, the descriptor struct and the scrape state.
3. Write one `fetch*` method per API call family and one `emit*` method per
   group of metrics that share inputs.
4. Build the graph in the constructor, call `SetupExporter`, keep the
   schedule.
5. Implement `Collect` as one call to `CollectSchedule`.
6. Add fixtures under `exporters/fixtures/`, a `<name>_test.go` suite with
   the expected text-format output, and add the suite to
   `TestOpenStackSuites` in `exporters/exporter_test.go`.
7. Run the focused tests:

   ```sh
   go test ./ ./cache ./exporters ./utils
   ```

   If your shell exports `OS_COMPUTE_API_VERSION` or similar, unset them
   first: the test fixtures pin their own microversions.

Naming checklist before opening a PR: `RegisterTypedExporter` takes the
service selector; `Name` is the metric namespace; `metric` tags are local
names; every entry in an emitter's `Metrics` matches a tag; expected test
output uses full `openstack_<name>_<metric>` names.
