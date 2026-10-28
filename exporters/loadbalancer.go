package exporters

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/amphorae"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/loadbalancers"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	RegisterTypedExporter("load-balancer", NewLoadbalancerExporter)
}

// Octavia API v2 entities have two status codes present in the response body.
// The provisioning_status describes the lifecycle status of the entity while the operating_status provides the observed status of the entity.
// Here we put operating_status in metrics value and provisioning_status in metrics label
var knownLoadbalancerStatuses = map[string]int{
	"ONLINE":     0, // Entity is operating normally. All pool members are healthy
	"DRAINING":   1, // The member is not accepting new connections
	"OFFLINE":    2, // Entity is administratively disabled
	"ERROR":      3, // The entity has failed. The member is failing it's health monitoring checks. All of the pool members are in ERROR
	"NO_MONITOR": 4, // No health monitor is configured for this entity and it's status is unknown
}

// The status of the amphora. One of: BOOTING, ALLOCATED, READY, PENDING_CREATE, PENDING_DELETE, DELETED, ERROR.
var knownAmphoraStatuses = map[string]int{
	"BOOTING":        0,
	"ALLOCATED":      1,
	"READY":          2,
	"PENDING_CREATE": 3,
	"PENDING_DELETE": 4,
	"DELETED":        5,
	"ERROR":          6,
}

func mapLoadbalancerStatus(current string) int {
	return mapStatus(knownLoadbalancerStatuses, current)
}

func mapAmphoraStatus(current string) int {
	return mapStatus(knownAmphoraStatuses, current)
}

func mapPoolStatus(current string) int {
	return mapStatus(knownLoadbalancerStatuses, current)
}

// Metric names of the per-loadbalancer and per-listener stats families.
var (
	loadbalancerStatsMetrics = []string{"stats_bytes_in", "stats_bytes_out", "stats_active_connections", "stats_total_connections", "stats_request_errors"}
	listenerStatsMetrics     = []string{"listener_stats_bytes_in", "listener_stats_bytes_out", "listener_stats_active_connections", "listener_stats_total_connections", "listener_stats_request_errors"}
)

type LoadbalancerExporter struct {
	BaseOpenStackExporter
	descs loadbalancerDescs
	sched Schedule[loadbalancerScrape]
}

type loadbalancerDescs struct {
	TotalLoadbalancers *prometheus.Desc `metric:"total_loadbalancers"`
	LoadbalancerStatus *prometheus.Desc `metric:"loadbalancer_status" labels:"id,name,project_id,operating_status,provisioning_status,provider,vip_address"`

	StatsBytesIn           *prometheus.Desc `metric:"stats_bytes_in"           labels:"id,name,project_id,operating_status,provisioning_status,provider,vip_address" slow:"true"`
	StatsBytesOut          *prometheus.Desc `metric:"stats_bytes_out"          labels:"id,name,project_id,operating_status,provisioning_status,provider,vip_address" slow:"true"`
	StatsActiveConnections *prometheus.Desc `metric:"stats_active_connections" labels:"id,name,project_id,operating_status,provisioning_status,provider,vip_address" slow:"true"`
	StatsTotalConnections  *prometheus.Desc `metric:"stats_total_connections"  labels:"id,name,project_id,operating_status,provisioning_status,provider,vip_address" slow:"true"`
	StatsRequestErrors     *prometheus.Desc `metric:"stats_request_errors"     labels:"id,name,project_id,operating_status,provisioning_status,provider,vip_address" slow:"true"`

	TotalListeners                 *prometheus.Desc `metric:"total_listeners"`
	ListenerStatsBytesIn           *prometheus.Desc `metric:"listener_stats_bytes_in"           labels:"id,name,project_id,operating_status,provisioning_status,protocol,protocol_port,loadbalancer_id" slow:"true"`
	ListenerStatsBytesOut          *prometheus.Desc `metric:"listener_stats_bytes_out"          labels:"id,name,project_id,operating_status,provisioning_status,protocol,protocol_port,loadbalancer_id" slow:"true"`
	ListenerStatsActiveConnections *prometheus.Desc `metric:"listener_stats_active_connections" labels:"id,name,project_id,operating_status,provisioning_status,protocol,protocol_port,loadbalancer_id" slow:"true"`
	ListenerStatsTotalConnections  *prometheus.Desc `metric:"listener_stats_total_connections"  labels:"id,name,project_id,operating_status,provisioning_status,protocol,protocol_port,loadbalancer_id" slow:"true"`
	ListenerStatsRequestErrors     *prometheus.Desc `metric:"listener_stats_request_errors"     labels:"id,name,project_id,operating_status,provisioning_status,protocol,protocol_port,loadbalancer_id" slow:"true"`

	TotalAmphorae *prometheus.Desc `metric:"total_amphorae"`
	AmphoraStatus *prometheus.Desc `metric:"amphora_status" labels:"id,loadbalancer_id,compute_id,status,role,lb_network_ip,ha_ip,cert_expiration"`
	TotalPools    *prometheus.Desc `metric:"total_pools"`
	PoolStatus    *prometheus.Desc `metric:"pool_status"    labels:"id,provisioning_status,name,loadbalancers,protocol,lb_algorithm,operating_status,project_id"`
}

type loadbalancerScrape struct {
	loadbalancers     []loadbalancers.LoadBalancer
	loadbalancerStats map[string]*loadbalancers.Stats // by loadbalancer ID; absent when the stats call failed
	listeners         []listeners.Listener
	listenerStats     map[string]*listeners.Stats // by listener ID; absent when the stats call failed
	amphorae          []amphorae.Amphora
	pools             []pools.Pool
}

func NewLoadbalancerExporter(config *ExporterConfig, logger *slog.Logger) (*LoadbalancerExporter, error) {
	e := &LoadbalancerExporter{
		BaseOpenStackExporter: BaseOpenStackExporter{
			Name:           "loadbalancer",
			ExporterConfig: *config,
			logger:         logger,
		},
	}
	graph := Graph[loadbalancerScrape]{
		Sources: []Source[loadbalancerScrape]{
			{Name: "loadbalancers", Fetch: e.fetchLoadbalancers},
			{Name: "loadbalancer_stats", DependsOn: []string{"loadbalancers"}, Fetch: e.fetchLoadbalancerStats},
			{Name: "listeners", Fetch: e.fetchListeners},
			{Name: "listener_stats", DependsOn: []string{"listeners"}, Fetch: e.fetchListenerStats},
			{Name: "amphorae", Fetch: e.fetchAmphorae},
			{Name: "pools", Fetch: e.fetchPools},
		},
		Emitters: []Emitter[loadbalancerScrape]{
			{Name: "loadbalancers", Metrics: []string{"total_loadbalancers", "loadbalancer_status"}, Sources: []string{"loadbalancers"}, Emit: e.emitLoadbalancers},
			{Name: "loadbalancer_stats", Metrics: loadbalancerStatsMetrics, Sources: []string{"loadbalancers", "loadbalancer_stats"}, Emit: e.emitLoadbalancerStats},
			{Name: "listeners", Metrics: []string{"total_listeners"}, Sources: []string{"listeners"}, Emit: e.emitListeners},
			{Name: "listener_stats", Metrics: listenerStatsMetrics, Sources: []string{"listeners", "listener_stats"}, Emit: e.emitListenerStats},
			{Name: "amphorae", Metrics: []string{"total_amphorae", "amphora_status"}, Sources: []string{"amphorae"}, Emit: e.emitAmphorae},
			{Name: "pools", Metrics: []string{"total_pools", "pool_status"}, Sources: []string{"pools"}, Emit: e.emitPools},
		},
	}
	sched, err := SetupExporter(&e.BaseOpenStackExporter, &e.descs, &graph)
	if err != nil {
		return nil, err
	}
	e.sched = sched
	return e, nil
}

func (e *LoadbalancerExporter) Collect(ch chan<- prometheus.Metric) {
	CollectSchedule(&e.BaseOpenStackExporter, &e.sched, new(loadbalancerScrape), ch)
}

func (e *LoadbalancerExporter) fetchLoadbalancers(ctx context.Context, s *loadbalancerScrape) error {
	allPages, err := loadbalancers.List(e.ClientV2, loadbalancers.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.loadbalancers, err = loadbalancers.ExtractLoadBalancers(allPages)
	return err
}

// fetchLoadbalancerStats queries the stats of every loadbalancer. A failed
// stats call is logged and leaves that loadbalancer without stats metrics,
// so one broken loadbalancer does not hide the others.
func (e *LoadbalancerExporter) fetchLoadbalancerStats(ctx context.Context, s *loadbalancerScrape) error {
	s.loadbalancerStats = make(map[string]*loadbalancers.Stats, len(s.loadbalancers))
	for i := range s.loadbalancers {
		lb := &s.loadbalancers[i]
		stats, err := loadbalancers.GetStats(ctx, e.ClientV2, lb.ID).Extract()
		if err != nil {
			e.logger.Warn("failed to get loadbalancer stats", "id", lb.ID, "error", err)
			continue
		}
		s.loadbalancerStats[lb.ID] = stats
	}
	return nil
}

func (e *LoadbalancerExporter) fetchListeners(ctx context.Context, s *loadbalancerScrape) error {
	allPages, err := listeners.List(e.ClientV2, listeners.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.listeners, err = listeners.ExtractListeners(allPages)
	return err
}

// fetchListenerStats queries the stats of every listener, skipping listeners
// whose stats call fails in the same way as fetchLoadbalancerStats.
func (e *LoadbalancerExporter) fetchListenerStats(ctx context.Context, s *loadbalancerScrape) error {
	s.listenerStats = make(map[string]*listeners.Stats, len(s.listeners))
	for i := range s.listeners {
		listener := &s.listeners[i]
		stats, err := listeners.GetStats(ctx, e.ClientV2, listener.ID).Extract()
		if err != nil {
			e.logger.Warn("failed to get listener stats", "id", listener.ID, "error", err)
			continue
		}
		s.listenerStats[listener.ID] = stats
	}
	return nil
}

func (e *LoadbalancerExporter) fetchAmphorae(ctx context.Context, s *loadbalancerScrape) error {
	allPages, err := amphorae.List(e.ClientV2, amphorae.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.amphorae, err = amphorae.ExtractAmphorae(allPages)
	return err
}

func (e *LoadbalancerExporter) fetchPools(ctx context.Context, s *loadbalancerScrape) error {
	allPages, err := pools.List(e.ClientV2, pools.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.pools, err = pools.ExtractPools(allPages)
	return err
}

func loadbalancerLabelValues(lb *loadbalancers.LoadBalancer) []string {
	return []string{lb.ID, lb.Name, lb.ProjectID, lb.OperatingStatus, lb.ProvisioningStatus, lb.Provider, lb.VipAddress}
}

func (e *LoadbalancerExporter) emitLoadbalancers(_ context.Context, s *loadbalancerScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.TotalLoadbalancers, float64(len(s.loadbalancers)))
	for i := range s.loadbalancers {
		lb := &s.loadbalancers[i]
		emitGauge(ch, e.descs.LoadbalancerStatus, float64(mapLoadbalancerStatus(lb.OperatingStatus)), loadbalancerLabelValues(lb)...)
	}
	return nil
}

func (e *LoadbalancerExporter) emitLoadbalancerStats(_ context.Context, s *loadbalancerScrape, ch chan<- prometheus.Metric) error {
	for i := range s.loadbalancers {
		lb := &s.loadbalancers[i]
		stats, ok := s.loadbalancerStats[lb.ID]
		if !ok {
			continue
		}
		labelValues := loadbalancerLabelValues(lb)
		emitGauge(ch, e.descs.StatsBytesIn, float64(stats.BytesIn), labelValues...)
		emitGauge(ch, e.descs.StatsBytesOut, float64(stats.BytesOut), labelValues...)
		emitGauge(ch, e.descs.StatsActiveConnections, float64(stats.ActiveConnections), labelValues...)
		emitGauge(ch, e.descs.StatsTotalConnections, float64(stats.TotalConnections), labelValues...)
		emitGauge(ch, e.descs.StatsRequestErrors, float64(stats.RequestErrors), labelValues...)
	}
	return nil
}

func (e *LoadbalancerExporter) emitListeners(_ context.Context, s *loadbalancerScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.TotalListeners, float64(len(s.listeners)))
	return nil
}

func (e *LoadbalancerExporter) emitListenerStats(_ context.Context, s *loadbalancerScrape, ch chan<- prometheus.Metric) error {
	for i := range s.listeners {
		listener := &s.listeners[i]
		stats, ok := s.listenerStats[listener.ID]
		if !ok {
			continue
		}
		labelValues := []string{listener.ID, listener.Name, listener.ProjectID,
			listener.OperatingStatus, listener.ProvisioningStatus, listener.Protocol,
			strconv.Itoa(listener.ProtocolPort), listenerLbsLabels(listener.Loadbalancers)}
		emitGauge(ch, e.descs.ListenerStatsBytesIn, float64(stats.BytesIn), labelValues...)
		emitGauge(ch, e.descs.ListenerStatsBytesOut, float64(stats.BytesOut), labelValues...)
		emitGauge(ch, e.descs.ListenerStatsActiveConnections, float64(stats.ActiveConnections), labelValues...)
		emitGauge(ch, e.descs.ListenerStatsTotalConnections, float64(stats.TotalConnections), labelValues...)
		emitGauge(ch, e.descs.ListenerStatsRequestErrors, float64(stats.RequestErrors), labelValues...)
	}
	return nil
}

func (e *LoadbalancerExporter) emitAmphorae(_ context.Context, s *loadbalancerScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.TotalAmphorae, float64(len(s.amphorae)))
	for i := range s.amphorae {
		a := &s.amphorae[i]
		emitGauge(ch, e.descs.AmphoraStatus, float64(mapAmphoraStatus(a.Status)),
			a.ID, a.LoadbalancerID, a.ComputeID, a.Status, a.Role, a.LBNetworkIP, a.HAIP, a.CertExpiration.Format(time.RFC3339))
	}
	return nil
}

func (e *LoadbalancerExporter) emitPools(_ context.Context, s *loadbalancerScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.TotalPools, float64(len(s.pools)))
	for i := range s.pools {
		pool := &s.pools[i]
		emitGauge(ch, e.descs.PoolStatus, float64(mapPoolStatus(pool.OperatingStatus)),
			pool.ID, pool.ProvisioningStatus, pool.Name, lbsLabels(pool.Loadbalancers), pool.Protocol, pool.LBMethod, pool.OperatingStatus, pool.ProjectID)
	}
	return nil
}

func lbsLabels(lbs []pools.LoadBalancerID) string {
	label := ""
	for i, l := range lbs {
		if i != 0 {
			label += ","
		}
		label += l.ID
	}

	return label
}

func listenerLbsLabels(lbs []listeners.LoadBalancerID) string {
	label := ""
	for i, l := range lbs {
		if i != 0 {
			label += ","
		}
		label += l.ID
	}

	return label
}
