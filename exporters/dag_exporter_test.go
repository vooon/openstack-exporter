package exporters

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// ---------------------------------------------------------------------------
// Metric declaration and policy
// ---------------------------------------------------------------------------

type policyTestDescs struct {
	Plain      *prometheus.Desc `metric:"plain"`
	Slow       *prometheus.Desc `metric:"slow_metric" slow:"true"`
	Deprecated *prometheus.Desc `metric:"deprecated_metric" deprecated:"1.4"`
	Untagged   *prometheus.Desc `labels:"id"`
}

func newPolicyTestExporter(t *testing.T, opts ExporterOptions) (*BaseOpenStackExporter, *policyTestDescs) {
	t.Helper()
	opts.Prefix = "openstack"
	base := &BaseOpenStackExporter{
		Name:           "test",
		ExporterConfig: ExporterConfig{ExporterOptions: opts},
		logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return base, &policyTestDescs{}
}

func TestCamelToSnake(t *testing.T) {
	for in, want := range map[string]string{
		"VolumeGB":           "volume_gb",
		"PoolCapacityFreeGB": "pool_capacity_free_gb",
		"Untagged":           "untagged",
	} {
		if got := camelToSnake(in); got != want {
			t.Errorf("camelToSnake(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseMetricDefsReadsTags(t *testing.T) {
	defs := parseMetricDefs(&policyTestDescs{})
	want := []metricDef{
		{field: 0, name: "plain"},
		{field: 1, name: "slow_metric", slow: true},
		{field: 2, name: "deprecated_metric", deprecated: "1.4"},
		{field: 3, name: "untagged", labels: []string{"id"}},
	}
	if !reflect.DeepEqual(defs, want) {
		t.Fatalf("parseMetricDefs() = %+v, want %+v", defs, want)
	}
}

func TestMetricOptionsUseExporterNames(t *testing.T) {
	base, _ := newPolicyTestExporter(t, ExporterOptions{
		DisabledMetrics: []string{"test-plain"},
		EnabledMetrics:  []string{"test-slow_metric"},
	})
	base.resolveMetrics([]metricDef{{name: "plain"}, {name: "slow_metric"}})

	if got := base.qualifiedMetricName("plain"); got != "test-plain" {
		t.Fatalf("qualifiedMetricName() = %q, want %q", got, "test-plain")
	}
	if base.IsMetricEnabled("plain") {
		t.Fatal("--disable-metric key did not disable the metric")
	}
	if !base.isExplicitlyEnabled("slow_metric") {
		t.Fatal("--enable-metric key did not enable the metric")
	}
}

// TestResolveMetricsPrecedence pins the documented precedence so the policy
// cannot drift between descriptor creation and schedule pruning.
func TestResolveMetricsPrecedence(t *testing.T) {
	tests := []struct {
		name string
		opts ExporterOptions
		want map[string]bool
	}{
		{
			name: "defaults enable everything",
			opts: ExporterOptions{},
			want: map[string]bool{"plain": true, "slow_metric": true, "deprecated_metric": true},
		},
		{
			name: "disable-slow-metrics drops only slow",
			opts: ExporterOptions{DisableSlowMetrics: true},
			want: map[string]bool{"plain": true, "slow_metric": false, "deprecated_metric": true},
		},
		{
			name: "disable-deprecated-metrics drops only deprecated",
			opts: ExporterOptions{DisableDeprecatedMetrics: true},
			want: map[string]bool{"plain": true, "slow_metric": true, "deprecated_metric": false},
		},
		{
			name: "enable-metric overrides slow and deprecated policy",
			opts: ExporterOptions{
				DisableSlowMetrics:       true,
				DisableDeprecatedMetrics: true,
				EnabledMetrics:           []string{"test-slow_metric", "test-deprecated_metric"},
			},
			want: map[string]bool{"plain": true, "slow_metric": true, "deprecated_metric": true},
		},
		{
			name: "disable-metric wins over enable-metric",
			opts: ExporterOptions{
				DisabledMetrics: []string{"test-slow_metric"},
				EnabledMetrics:  []string{"test-slow_metric"},
			},
			want: map[string]bool{"plain": true, "slow_metric": false, "deprecated_metric": true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base, descs := newPolicyTestExporter(t, tt.opts)
			base.resolveMetrics(parseMetricDefs(descs))
			for metric, want := range tt.want {
				if got := base.IsMetricEnabled(metric); got != want {
					t.Errorf("IsMetricEnabled(%q) = %v, want %v", metric, got, want)
				}
			}
		})
	}
}

func TestIsMetricEnabledGuardsSeveralNames(t *testing.T) {
	base, descs := newPolicyTestExporter(t, ExporterOptions{DisabledMetrics: []string{"test-plain"}})
	base.resolveMetrics(parseMetricDefs(descs))

	if !base.IsMetricEnabled("plain", "slow_metric") {
		t.Fatal("one enabled metric should keep a shared fetch alive")
	}
	if base.IsMetricEnabled("plain") {
		t.Fatal("disabled metric reported enabled")
	}
	if !base.IsMetricEnabled("typo") {
		t.Fatal("unknown metric must be treated as enabled")
	}
}

func TestIsMetricEnabledFallsBackToDisableListOnLegacyExporters(t *testing.T) {
	base := &BaseOpenStackExporter{
		Name:           "legacy",
		ExporterConfig: ExporterConfig{ExporterOptions: ExporterOptions{DisabledMetrics: []string{"legacy-x"}}},
	}
	if base.IsMetricEnabled("x") {
		t.Fatal("legacy exporter ignored --disable-metric")
	}
	if !base.IsMetricEnabled("y") {
		t.Fatal("legacy exporter disabled an unlisted metric")
	}
}

func TestRegisterDescsFollowsResolvedPolicy(t *testing.T) {
	base, descs := newPolicyTestExporter(t, ExporterOptions{DisableSlowMetrics: true})
	base.resolveMetrics(parseMetricDefs(descs))
	base.registerDescs(descs)

	if descs.Plain == nil {
		t.Error("enabled metric got no descriptor")
	}
	if descs.Slow != nil {
		t.Error("slow metric got a descriptor despite --disable-slow-metrics")
	}
	if descs.Deprecated == nil {
		t.Error("deprecated metric got no descriptor")
	}
	if got := descs.Plain.String(); !strings.Contains(got, `fqName: "openstack_test_plain"`) {
		t.Errorf("descriptor name = %s, want openstack_test_plain", got)
	}
	if got := len(base.allDescs); got != 3 {
		t.Errorf("allDescs = %d, want 3", got)
	}
}

func TestDeclareDynamicMetricParticipatesInPruning(t *testing.T) {
	base, descs := newPolicyTestExporter(t, ExporterOptions{DisabledMetrics: []string{"test-dynamic"}})
	base.resolveMetrics(parseMetricDefs(descs))
	base.declareDynamicMetric("dynamic")
	if base.IsMetricEnabled("dynamic") {
		t.Fatal("dynamic metric ignored --disable-metric")
	}

	base2, descs2 := newPolicyTestExporter(t, ExporterOptions{})
	base2.resolveMetrics(parseMetricDefs(descs2))
	base2.declareDynamicMetric("dynamic")
	if !base2.IsMetricEnabled("dynamic") {
		t.Fatal("dynamic metric was not enabled by default")
	}
}

func TestPruneScheduleDropsSlowAndDeprecatedSources(t *testing.T) {
	type scrape struct{}
	graph := Graph[scrape]{
		Sources: []Source[scrape]{
			{Name: "projects"},
			{Name: "limits", DependsOn: []string{"projects"}},
			{Name: "plain"},
		},
		Emitters: []Emitter[scrape]{
			{Name: "emitPlain", Metrics: []string{"plain"}, Sources: []string{"plain"}},
			{Name: "emitSlow", Metrics: []string{"slow_metric"}, Sources: []string{"limits"}},
			{Name: "emitDeprecated", Metrics: []string{"deprecated_metric"}, Sources: []string{"limits"}},
			{Name: "emitUntagged", Metrics: []string{"untagged"}, Sources: []string{"plain"}},
		},
	}

	tests := []struct {
		name string
		opts ExporterOptions
		want [][]string
	}{
		{
			name: "slow pruning drops the limits source and its projects dependency",
			opts: ExporterOptions{DisableSlowMetrics: true, DisableDeprecatedMetrics: true},
			want: [][]string{{"source:plain"}, {"emitter:emitPlain", "emitter:emitUntagged"}},
		},
		{
			name: "explicitly enabling a slow metric keeps its sources alive",
			opts: ExporterOptions{
				DisableSlowMetrics:       true,
				DisableDeprecatedMetrics: true,
				EnabledMetrics:           []string{"test-slow_metric"},
			},
			want: [][]string{
				{"source:projects", "source:plain"},
				{"source:limits", "emitter:emitPlain", "emitter:emitUntagged"},
				{"emitter:emitSlow"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base, descs := newPolicyTestExporter(t, tt.opts)
			sched, err := SetupExporter(base, descs, &graph)
			if err != nil {
				t.Fatalf("SetupExporter() error = %v", err)
			}
			if got := sched.Waves(); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("pruned waves = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestSetupExporterRejectsUnknownEmitterMetric(t *testing.T) {
	type scrape struct{}
	graph := Graph[scrape]{
		Sources:  []Source[scrape]{{Name: "a"}},
		Emitters: []Emitter[scrape]{{Name: "emit", Metrics: []string{"typo"}, Sources: []string{"a"}}},
	}
	base, descs := newPolicyTestExporter(t, ExporterOptions{})
	_, err := SetupExporter(base, descs, &graph)
	if err == nil || !strings.Contains(err.Error(), `emitter "emit" declares unknown metric "typo"`) {
		t.Fatalf("SetupExporter() error = %v, want unknown metric error", err)
	}
}

// ---------------------------------------------------------------------------
// End-to-end: a small DAG exporter gathered through a registry
// ---------------------------------------------------------------------------

type fakeDescs struct {
	Items      *prometheus.Desc `metric:"items"`
	ItemDetail *prometheus.Desc `metric:"item_detail" labels:"id" slow:"true"`
}

type fakeScrape struct {
	items   []string
	details map[string]float64
}

type fakeExporter struct {
	BaseOpenStackExporter
	descs    fakeDescs
	sched    Schedule[fakeScrape]
	listErr  error
	listCall int
}

func newFakeExporter(t *testing.T, name string, opts ExporterOptions) *fakeExporter {
	t.Helper()
	opts.Prefix = "openstack"
	e := &fakeExporter{BaseOpenStackExporter: BaseOpenStackExporter{
		Name:           name,
		ExporterConfig: ExporterConfig{ExporterOptions: opts},
		logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}}
	graph := Graph[fakeScrape]{
		Sources: []Source[fakeScrape]{
			{Name: "items", Fetch: e.fetchItems},
			{Name: "details", DependsOn: []string{"items"}, Fetch: e.fetchDetails},
		},
		Emitters: []Emitter[fakeScrape]{
			{Name: "items", Metrics: []string{"items"}, Sources: []string{"items"}, Emit: e.emitItems},
			{Name: "details", Metrics: []string{"item_detail"}, Sources: []string{"details"}, Emit: e.emitDetails},
		},
	}
	sched, err := SetupExporter(&e.BaseOpenStackExporter, &e.descs, &graph)
	if err != nil {
		t.Fatalf("SetupExporter() error = %v", err)
	}
	e.sched = sched
	return e
}

func (e *fakeExporter) fetchItems(_ context.Context, s *fakeScrape) error {
	e.listCall++
	if e.listErr != nil {
		return e.listErr
	}
	s.items = []string{"a", "b"}
	return nil
}

func (e *fakeExporter) fetchDetails(_ context.Context, s *fakeScrape) error {
	s.details = make(map[string]float64, len(s.items))
	for i, id := range s.items {
		s.details[id] = float64(i + 1)
	}
	return nil
}

func (e *fakeExporter) emitItems(_ context.Context, s *fakeScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.Items, float64(len(s.items)))
	return nil
}

func (e *fakeExporter) emitDetails(_ context.Context, s *fakeScrape, ch chan<- prometheus.Metric) error {
	for _, id := range s.items {
		emitGauge(ch, e.descs.ItemDetail, s.details[id], id)
	}
	return nil
}

func (e *fakeExporter) Collect(ch chan<- prometheus.Metric) {
	CollectSchedule(&e.BaseOpenStackExporter, &e.sched, new(fakeScrape), ch)
}

func TestDAGExporterCollectsThroughRegistry(t *testing.T) {
	e := newFakeExporter(t, "fake", ExporterOptions{})
	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(e); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	expected := `
# HELP openstack_exporter_scrape_errors_total Total number of scrapes in which at least one source or emitter failed.
# TYPE openstack_exporter_scrape_errors_total counter
openstack_exporter_scrape_errors_total{service="fake"} 0
# HELP openstack_exporter_scrapes_total Total number of scrapes.
# TYPE openstack_exporter_scrapes_total counter
openstack_exporter_scrapes_total{service="fake"} 1
# HELP openstack_fake_item_detail item_detail
# TYPE openstack_fake_item_detail gauge
openstack_fake_item_detail{id="a"} 1
openstack_fake_item_detail{id="b"} 2
# HELP openstack_fake_items items
# TYPE openstack_fake_items gauge
openstack_fake_items 2
# HELP openstack_fake_up up
# TYPE openstack_fake_up gauge
openstack_fake_up 1
`
	if err := testutil.GatherAndCompare(registry, strings.NewReader(expected)); err != nil {
		t.Fatal(err)
	}
}

func TestDAGExporterPrunesSlowMetricAndItsSource(t *testing.T) {
	e := newFakeExporter(t, "fake", ExporterOptions{DisableSlowMetrics: true})
	if e.descs.ItemDetail != nil {
		t.Fatal("slow metric got a descriptor")
	}
	if got, want := e.sched.PrunedSources(), []string{"details"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("PrunedSources() = %v, want %v", got, want)
	}

	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(e)
	expected := `
# HELP openstack_fake_items items
# TYPE openstack_fake_items gauge
openstack_fake_items 2
# HELP openstack_fake_up up
# TYPE openstack_fake_up gauge
openstack_fake_up 1
`
	if err := testutil.GatherAndCompare(registry, strings.NewReader(expected), "openstack_fake_items", "openstack_fake_item_detail", "openstack_fake_up"); err != nil {
		t.Fatal(err)
	}
}

func TestDAGExporterDisablingAllMetricsSkipsEveryFetch(t *testing.T) {
	e := newFakeExporter(t, "fake", ExporterOptions{DisabledMetrics: []string{"fake-items", "fake-item_detail"}})
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(e)
	if _, err := registry.Gather(); err != nil {
		t.Fatal(err)
	}
	if e.listCall != 0 {
		t.Fatalf("list source ran %d times with every metric disabled", e.listCall)
	}
	expected := `
# HELP openstack_fake_up up
# TYPE openstack_fake_up gauge
openstack_fake_up 0
`
	if err := testutil.GatherAndCompare(registry, strings.NewReader(expected), "openstack_fake_up"); err != nil {
		t.Fatal(err)
	}
}

func TestDAGExporterFailedSourceOmitsMetricsAndReportsDown(t *testing.T) {
	e := newFakeExporter(t, "fake", ExporterOptions{})
	e.listErr = errors.New("api down")
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(e)

	expected := `
# HELP openstack_exporter_scrape_errors_total Total number of scrapes in which at least one source or emitter failed.
# TYPE openstack_exporter_scrape_errors_total counter
openstack_exporter_scrape_errors_total{service="fake"} 1
# HELP openstack_exporter_scrapes_total Total number of scrapes.
# TYPE openstack_exporter_scrapes_total counter
openstack_exporter_scrapes_total{service="fake"} 1
# HELP openstack_fake_up up
# TYPE openstack_fake_up gauge
openstack_fake_up 0
`
	// No items/item_detail families: a failed source must not turn into zeros.
	if err := testutil.GatherAndCompare(registry, strings.NewReader(expected)); err != nil {
		t.Fatal(err)
	}
}

func TestDAGExportersShareInstrumentationFamilies(t *testing.T) {
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(newFakeExporter(t, "left", ExporterOptions{CollectTime: true}))
	registry.MustRegister(newFakeExporter(t, "right", ExporterOptions{CollectTime: true}))

	mfs, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	families := map[string]int{}
	for _, mf := range mfs {
		families[mf.GetName()] = len(mf.GetMetric())
	}
	if got := families["openstack_exporter_scrapes_total"]; got != 2 {
		t.Errorf("scrapes_total series = %d, want 2 (one per service)", got)
	}
	if got := families["openstack_exporter_source_fetch_duration_seconds"]; got != 4 {
		t.Errorf("source_fetch_duration series = %d, want 4 (two services x two sources)", got)
	}
}

func TestSourceFetchDurationIsOptIn(t *testing.T) {
	e := newFakeExporter(t, "fake", ExporterOptions{})
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(e)
	mfs, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if strings.Contains(mf.GetName(), "source_fetch_duration") {
			t.Fatal("duration histogram emitted without --collect-metric-time")
		}
	}
}
