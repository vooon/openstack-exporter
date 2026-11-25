package exporters

import (
	"reflect"
	"regexp"
	"slices"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// This file holds the metric declaration and policy half of the DAG exporter
// model: metrics are declared as *prometheus.Desc fields with struct tags,
// the enable/disable flags are resolved once into a policy, and both
// descriptor creation and schedule pruning read that one policy.

var (
	reFirstCap = regexp.MustCompile(`(.)([A-Z][a-z]+)`)
	reAllCap   = regexp.MustCompile(`([a-z0-9])([A-Z])`)
)

// camelToSnake converts a CamelCase identifier to snake_case,
// e.g. VolumeGB -> volume_gb, PoolCapacityFreeGB -> pool_capacity_free_gb.
func camelToSnake(s string) string {
	s = reFirstCap.ReplaceAllString(s, "${1}_${2}")
	s = reAllCap.ReplaceAllString(s, "${1}_${2}")
	return strings.ToLower(s)
}

// metricDef is one metric declaration parsed from the struct tags of a
// descriptor struct.
type metricDef struct {
	field      int    // index of the *prometheus.Desc field in the descs struct
	name       string // local metric name, without prefix or exporter namespace
	labels     []string
	slow       bool
	deprecated string // version the metric was deprecated in, empty if current
}

// parseMetricDefs reads the metric declarations from the exported
// *prometheus.Desc fields of dst, a pointer to a struct. It inspects struct
// tags only and applies no policy, so the result does not depend on
// configuration.
//
// Tags: "metric" names the metric (default: camelToSnake of the field name),
// "labels" is a comma-separated label list, "slow" ("true") and "deprecated"
// (a version string) are optional.
func parseMetricDefs(dst any) []metricDef {
	rt := reflect.ValueOf(dst).Elem().Type()
	descPtrType := reflect.TypeOf((*prometheus.Desc)(nil))

	var defs []metricDef
	for i := 0; i < rt.NumField(); i++ {
		sf := rt.Field(i)
		if !sf.IsExported() || sf.Type != descPtrType {
			continue
		}

		name := sf.Tag.Get("metric")
		if name == "" {
			name = camelToSnake(sf.Name)
		}

		var labels []string
		if raw := sf.Tag.Get("labels"); raw != "" {
			labels = strings.Split(raw, ",")
		}

		defs = append(defs, metricDef{
			field:      i,
			name:       name,
			labels:     labels,
			slow:       sf.Tag.Get("slow") == "true",
			deprecated: sf.Tag.Get("deprecated"),
		})
	}
	return defs
}

// qualifiedMetricName returns the "exporter-metric" key used by
// --disable-metric and --enable-metric, e.g. "nova-limits_vcpus_max".
func (exporter *BaseOpenStackExporter) qualifiedMetricName(name string) string {
	return exporter.Name + "-" + name
}

// isExplicitlyEnabled reports whether name is listed in --enable-metric.
func (exporter *BaseOpenStackExporter) isExplicitlyEnabled(name string) bool {
	return slices.Contains(exporter.EnabledMetrics, exporter.qualifiedMetricName(name))
}

// resolveMetrics decides once which declared metrics this exporter collects.
// Precedence, highest first:
//
//  1. listed in --disable-metric                   -> disabled
//  2. listed in --enable-metric                    -> enabled (overrides 3, 4)
//  3. slow and --disable-slow-metrics              -> disabled
//  4. deprecated and --disable-deprecated-metrics  -> disabled
//  5. otherwise                                    -> enabled
func (exporter *BaseOpenStackExporter) resolveMetrics(defs []metricDef) {
	if exporter.metricEnabled == nil {
		exporter.metricEnabled = make(map[string]bool, len(defs))
	}

	for _, def := range defs {
		enabled := true
		switch {
		case slices.Contains(exporter.DisabledMetrics, exporter.qualifiedMetricName(def.name)):
			exporter.log().Warn("metric disabled for exporter", "metric", def.name, "exporter", exporter.Name)
			enabled = false
		case exporter.isExplicitlyEnabled(def.name):
			// --enable-metric overrides the slow/deprecated policy below.
		case exporter.DisableSlowMetrics && def.slow:
			enabled = false
		case exporter.DisableDeprecatedMetrics && def.deprecated != "":
			enabled = false
		}

		if enabled && def.deprecated != "" {
			exporter.log().Warn("metric deprecated", "metric", def.name, "exporter", exporter.Name, "version", def.deprecated)
		}
		exporter.metricEnabled[def.name] = enabled
	}
}

// declareDynamicMetric records a metric whose descriptor is built at runtime
// because its label set is not fixed. Declaring it keeps the name visible to
// IsMetricEnabled and to schedule pruning, so it prunes like a tagged one.
func (exporter *BaseOpenStackExporter) declareDynamicMetric(name string) {
	if exporter.metricEnabled == nil {
		exporter.metricEnabled = make(map[string]bool)
	}
	if _, ok := exporter.metricEnabled[name]; !ok {
		exporter.metricEnabled[name] = !slices.Contains(exporter.DisabledMetrics, exporter.qualifiedMetricName(name))
	}
}

// metricPolicy returns the resolved policy in the form PruneSchedule takes.
func (exporter *BaseOpenStackExporter) metricPolicy() map[string]bool {
	return exporter.metricEnabled
}

// IsMetricEnabled reports whether any of the given metrics is collected.
// Passing several names guards an API call that feeds multiple metrics.
//
// The answer comes from the policy resolved by SetupExporter, which accounts
// for all four enable/disable flags. An unknown name is reported as enabled
// and logged, since silently dropping a metric over a typo would be worse.
func (exporter *BaseOpenStackExporter) IsMetricEnabled(names ...string) bool {
	for _, name := range names {
		enabled, known := exporter.metricEnabled[name]
		if !known {
			exporter.log().Warn("unknown metric queried; treating as enabled", "metric", name, "exporter", exporter.Name)
			return true
		}
		if enabled {
			return true
		}
	}
	return false
}

// registerDescs creates a prometheus.Desc for every enabled metric declared
// by dst and stores it in the matching field. Fields of disabled metrics stay
// nil, which emitGauge treats as a no-op. resolveMetrics must run first.
func (exporter *BaseOpenStackExporter) registerDescs(dst any) {
	rv := reflect.ValueOf(dst).Elem()
	for _, def := range parseMetricDefs(dst) {
		if !exporter.metricEnabled[def.name] {
			continue
		}
		exporter.log().Info("Adding metric to exporter", "metric", def.name, "exporter", exporter.Name, "disable_key", exporter.qualifiedMetricName(def.name))
		desc := prometheus.NewDesc(prometheus.BuildFQName(exporter.GetName(), "", def.name), def.name, def.labels, nil)
		rv.Field(def.field).Set(reflect.ValueOf(desc))
		exporter.allDescs = append(exporter.allDescs, desc)
	}
}

// RegisterDesc adds a descriptor built outside the struct-tag mechanism, for
// a metric whose label set is only known at runtime, so Describe reports it.
func (exporter *BaseOpenStackExporter) RegisterDesc(d *prometheus.Desc) {
	if d != nil {
		exporter.allDescs = append(exporter.allDescs, d)
	}
}

// emitGauge emits a gauge sample when desc is non-nil and does nothing when
// the metric was pruned, so emitters need no per-metric nil checks.
func emitGauge(ch chan<- prometheus.Metric, desc *prometheus.Desc, value float64, labelValues ...string) {
	if desc != nil {
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, labelValues...)
	}
}
