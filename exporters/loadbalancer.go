package exporters

import (
	"context"
	"log/slog"
	"time"

	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/amphorae"
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

type LoadbalancerExporter struct {
	BaseOpenStackExporter
	descs loadbalancerDescs
	sched Schedule[loadbalancerScrape]
}

type loadbalancerDescs struct {
	TotalLoadbalancers *prometheus.Desc `metric:"total_loadbalancers"`
	LoadbalancerStatus *prometheus.Desc `metric:"loadbalancer_status" labels:"id,name,project_id,operating_status,provisioning_status,provider,vip_address"`
	TotalAmphorae      *prometheus.Desc `metric:"total_amphorae"`
	AmphoraStatus      *prometheus.Desc `metric:"amphora_status"      labels:"id,loadbalancer_id,compute_id,status,role,lb_network_ip,ha_ip,cert_expiration"`
	TotalPools         *prometheus.Desc `metric:"total_pools"`
	PoolStatus         *prometheus.Desc `metric:"pool_status"         labels:"id,provisioning_status,name,loadbalancers,protocol,lb_algorithm,operating_status,project_id"`
}

type loadbalancerScrape struct {
	loadbalancers []loadbalancers.LoadBalancer
	amphorae      []amphorae.Amphora
	pools         []pools.Pool
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
			{Name: "amphorae", Fetch: e.fetchAmphorae},
			{Name: "pools", Fetch: e.fetchPools},
		},
		Emitters: []Emitter[loadbalancerScrape]{
			{Name: "loadbalancers", Metrics: []string{"total_loadbalancers", "loadbalancer_status"}, Sources: []string{"loadbalancers"}, Emit: e.emitLoadbalancers},
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

func (e *LoadbalancerExporter) emitLoadbalancers(_ context.Context, s *loadbalancerScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.TotalLoadbalancers, float64(len(s.loadbalancers)))
	for i := range s.loadbalancers {
		lb := &s.loadbalancers[i]
		emitGauge(ch, e.descs.LoadbalancerStatus, float64(mapLoadbalancerStatus(lb.OperatingStatus)),
			lb.ID, lb.Name, lb.ProjectID, lb.OperatingStatus, lb.ProvisioningStatus, lb.Provider, lb.VipAddress)
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
