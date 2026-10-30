package exporters

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"net/netip"
	"strconv"
	"strings"

	"go4.org/netipx"

	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/projects"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/agents"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/external"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/layer3/floatingips"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/layer3/routers"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/mtu"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/networkipavailabilities"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/portsbinding"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/provider"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/quotas"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/security/groups"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/subnetpools"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/vpnaas/endpointgroups"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/vpnaas/ikepolicies"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/vpnaas/ipsecpolicies"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/vpnaas/services"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/vpnaas/siteconnections"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/networks"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/ports"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/subnets"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
)

func init() {
	RegisterTypedExporter("network", NewNeutronExporter)
}

var knownNetworkStatuses = map[string]int{
	"ACTIVE": 0,
	"BUILD":  1,
	"DOWN":   2,
	"ERROR":  3,
}

func mapNetworkStatus(current string) int {
	return mapStatus(knownNetworkStatuses, current)
}

var knownVpnStatuses = map[string]int{
	"ACTIVE":         0,
	"DOWN":           1,
	"BUILD":          2,
	"ERROR":          3,
	"PENDING_CREATE": 4,
	"PENDING_UPDATE": 5,
	"PENDING_DELETE": 6,
}

func mapVpnServiceStatus(current string) int {
	return mapStatus(knownVpnStatuses, current)
}

func mapVpnConnectionStatus(current string) int {
	return mapStatus(knownVpnStatuses, current)
}

// NeutronExporter : extends BaseOpenStackExporter
type NeutronExporter struct {
	BaseOpenStackExporter
	descs neutronDescs
	sched Schedule[neutronScrape]
}

type neutronDescs struct {
	FloatingIPs                    *prometheus.Desc `metric:"floating_ips"`
	FloatingIPsAssociatedNotActive *prometheus.Desc `metric:"floating_ips_associated_not_active"`
	FloatingIP                     *prometheus.Desc `metric:"floating_ip"                        labels:"id,floating_network_id,router_id,status,project_id,floating_ip_address"`
	Networks                       *prometheus.Desc `metric:"networks"`
	Network                        *prometheus.Desc `metric:"network"                            labels:"id,tenant_id,status,name,is_shared,is_external,provider_network_type,provider_physical_network,provider_segmentation_id,subnets,tags,mtu"`
	SecurityGroups                 *prometheus.Desc `metric:"security_groups"`
	Subnets                        *prometheus.Desc `metric:"subnets"`
	Subnet                         *prometheus.Desc `metric:"subnet"                             labels:"id,tenant_id,name,network_id,cidr,gateway_ip,enable_dhcp,dns_nameservers,tags"`
	Port                           *prometheus.Desc `metric:"port"                               labels:"uuid,network_id,mac_address,device_owner,device_id,status,binding_vif_type,admin_state_up,fixed_ips"`
	Ports                          *prometheus.Desc `metric:"ports"`
	PortsNoIPs                     *prometheus.Desc `metric:"ports_no_ips"`
	PortsLBNotActive               *prometheus.Desc `metric:"ports_lb_not_active"`
	Router                         *prometheus.Desc `metric:"router"                             labels:"id,name,project_id,admin_state_up,status,external_network_id"`
	Routers                        *prometheus.Desc `metric:"routers"`
	VpnEndpointGroups              *prometheus.Desc `metric:"vpn_endpoint_groups"`
	VpnIKEPolicies                 *prometheus.Desc `metric:"vpn_ike_policies"`
	VpnIPsecPolicies               *prometheus.Desc `metric:"vpn_ipsec_policies"`
	VpnServices                    *prometheus.Desc `metric:"vpn_services"`
	VpnService                     *prometheus.Desc `metric:"vpn_service"                        labels:"id,project_id,subnet_id,router_id,admin_state_up,name,external_ipv4,external_ipv6,flavor_id"`
	VpnSiteConnections             *prometheus.Desc `metric:"vpn_siteconnections"`
	VpnSiteConnection              *prometheus.Desc `metric:"vpn_siteconnection"                 labels:"id,project_id,admin_state_up,name,vpn_service_id,ike_policy_id,ipsec_policy_id,peer_id,peer_ep_group_id,local_id,local_ep_group_id"`
	RoutersNotActive               *prometheus.Desc `metric:"routers_not_active"`
	L3AgentOfRouter                *prometheus.Desc `metric:"l3_agent_of_router"                 labels:"router_id,l3_agent_id,ha_state,agent_alive,agent_admin_up,agent_host"`
	AgentState                     *prometheus.Desc `metric:"agent_state"                        labels:"id,hostname,service,adminState,availability_zone"`
	NetworkIPAvailabilitiesTotal   *prometheus.Desc `metric:"network_ip_availabilities_total"    labels:"network_id,network_name,ip_version,cidr,subnet_name,project_id"`
	NetworkIPAvailabilitiesUsed    *prometheus.Desc `metric:"network_ip_availabilities_used"     labels:"network_id,network_name,ip_version,cidr,subnet_name,project_id"`
	SubnetsTotal                   *prometheus.Desc `metric:"subnets_total"                      labels:"ip_version,prefix,prefix_length,project_id,subnet_pool_id,subnet_pool_name"`
	SubnetsUsed                    *prometheus.Desc `metric:"subnets_used"                       labels:"ip_version,prefix,prefix_length,project_id,subnet_pool_id,subnet_pool_name"`
	SubnetsFree                    *prometheus.Desc `metric:"subnets_free"                       labels:"ip_version,prefix,prefix_length,project_id,subnet_pool_id,subnet_pool_name"`
	QuotaNetwork                   *prometheus.Desc `metric:"quota_network"                      labels:"type,tenant,tenant_id" slow:"true"`
	QuotaSubnet                    *prometheus.Desc `metric:"quota_subnet"                       labels:"type,tenant,tenant_id" slow:"true"`
	QuotaSubnetPool                *prometheus.Desc `metric:"quota_subnetpool"                   labels:"type,tenant,tenant_id" slow:"true"`
	QuotaPort                      *prometheus.Desc `metric:"quota_port"                         labels:"type,tenant,tenant_id" slow:"true"`
	QuotaRouter                    *prometheus.Desc `metric:"quota_router"                       labels:"type,tenant,tenant_id" slow:"true"`
	QuotaFloatingIP                *prometheus.Desc `metric:"quota_floatingip"                   labels:"type,tenant,tenant_id" slow:"true"`
	QuotaSecurityGroup             *prometheus.Desc `metric:"quota_security_group"               labels:"type,tenant,tenant_id" slow:"true"`
	QuotaSecurityGroupRule         *prometheus.Desc `metric:"quota_security_group_rule"          labels:"type,tenant,tenant_id" slow:"true"`
	QuotaRBACPolicy                *prometheus.Desc `metric:"quota_rbac_policy"                  labels:"type,tenant,tenant_id" slow:"true"`
}

// neutronNetworkExt : networks.Network with the external, provider and MTU extensions
type neutronNetworkExt struct {
	networks.Network
	external.NetworkExternalExt
	provider.NetworkProviderExt
	mtu.NetworkMTUExt
}

// neutronPortExt : ports.Port with the port binding extension
type neutronPortExt struct {
	ports.Port
	portsbinding.PortsBindingExt
}

// neutronNetIPAvail : one network from network-ip-availabilities, with the
// per-subnet counters already parsed from their JSON numbers
type neutronNetIPAvail struct {
	NetworkID   string
	NetworkName string
	ProjectID   string
	Subnets     []neutronSubnetIPAvail
}

type neutronSubnetIPAvail struct {
	SubnetName string
	CIDR       string
	IPVersion  int
	TotalIPs   float64
	UsedIPs    float64
}

// neutronProjectQuota : quota detail of one project, with the project name
// the metric labels need
type neutronProjectQuota struct {
	projectName string
	projectID   string
	quota       quotas.QuotaDetailSet
}

type neutronScrape struct {
	floatingIPs         []floatingips.FloatingIP
	networks            []neutronNetworkExt
	securityGroups      []groups.SecGroup
	subnets             []subnets.Subnet
	ports               []neutronPortExt
	routers             []routers.Router
	ovnBackend          bool
	routerL3Agents      map[string][]routers.L3Agent
	vpnEndpointGroups   []endpointgroups.EndpointGroup
	vpnIKEPolicies      []ikepolicies.Policy
	vpnIPsecPolicies    []ipsecpolicies.Policy
	vpnServices         []services.Service
	vpnSiteConnections  []siteconnections.Connection
	agents              []agents.Agent
	netIPAvailabilities []neutronNetIPAvail
	subnetPools         []subnetpools.SubnetPool
	projects            []projects.Project
	quotas              []neutronProjectQuota
}

// NewNeutronExporter : returns a pointer to NeutronExporter
func NewNeutronExporter(config *ExporterConfig, logger *slog.Logger) (*NeutronExporter, error) {
	e := &NeutronExporter{
		BaseOpenStackExporter: BaseOpenStackExporter{
			Name:           "neutron",
			ExporterConfig: *config,
			logger:         logger,
		},
	}
	graph := Graph[neutronScrape]{
		Sources: []Source[neutronScrape]{
			{Name: "floatingips", Fetch: e.fetchFloatingIPs},
			{Name: "networks", Fetch: e.fetchNetworks},
			{Name: "secgroups", Fetch: e.fetchSecGroups},
			{Name: "subnets", Fetch: e.fetchSubnets},
			{Name: "ports", Fetch: e.fetchPorts},
			{Name: "routers", Fetch: e.fetchRouters},
			{Name: "router_l3_agents", DependsOn: []string{"routers"}, Fetch: e.fetchRouterL3Agents},
			{Name: "vpn_endpoint_groups", Fetch: e.fetchVpnEndpointGroups},
			{Name: "vpn_ike_policies", Fetch: e.fetchVpnIKEPolicies},
			{Name: "vpn_ipsec_policies", Fetch: e.fetchVpnIPsecPolicies},
			{Name: "vpn_services", Fetch: e.fetchVpnServices},
			{Name: "vpn_siteconnections", Fetch: e.fetchVpnSiteConnections},
			{Name: "agents", Fetch: e.fetchAgents},
			{Name: "net_ip_avail", Fetch: e.fetchNetIPAvail},
			{Name: "subnet_pools", Fetch: e.fetchSubnetPools},
			{Name: "projects", Fetch: e.fetchProjects},
			{Name: "quotas", DependsOn: []string{"projects"}, Fetch: e.fetchQuotas},
		},
		Emitters: []Emitter[neutronScrape]{
			{Name: "floatingips", Metrics: []string{"floating_ips", "floating_ips_associated_not_active", "floating_ip"}, Sources: []string{"floatingips"}, Emit: e.emitFloatingIPs},
			{Name: "networks", Metrics: []string{"networks", "network"}, Sources: []string{"networks"}, Emit: e.emitNetworks},
			{Name: "secgroups", Metrics: []string{"security_groups"}, Sources: []string{"secgroups"}, Emit: e.emitSecGroups},
			{Name: "subnets", Metrics: []string{"subnets", "subnet"}, Sources: []string{"subnets"}, Emit: e.emitSubnets},
			{Name: "ports", Metrics: []string{"port", "ports", "ports_no_ips", "ports_lb_not_active"}, Sources: []string{"ports"}, Emit: e.emitPorts},
			{Name: "routers", Metrics: []string{"router", "routers", "routers_not_active"}, Sources: []string{"routers"}, Emit: e.emitRouters},
			{Name: "router_l3_agents", Metrics: []string{"l3_agent_of_router"}, Sources: []string{"router_l3_agents"}, Emit: e.emitRouterL3Agents},
			{Name: "vpn_endpoint_groups", Metrics: []string{"vpn_endpoint_groups"}, Sources: []string{"vpn_endpoint_groups"}, Emit: e.emitVpnEndpointGroups},
			{Name: "vpn_ike_policies", Metrics: []string{"vpn_ike_policies"}, Sources: []string{"vpn_ike_policies"}, Emit: e.emitVpnIKEPolicies},
			{Name: "vpn_ipsec_policies", Metrics: []string{"vpn_ipsec_policies"}, Sources: []string{"vpn_ipsec_policies"}, Emit: e.emitVpnIPsecPolicies},
			{Name: "vpn_services", Metrics: []string{"vpn_services", "vpn_service"}, Sources: []string{"vpn_services"}, Emit: e.emitVpnServices},
			{Name: "vpn_siteconnections", Metrics: []string{"vpn_siteconnections", "vpn_siteconnection"}, Sources: []string{"vpn_siteconnections"}, Emit: e.emitVpnSiteConnections},
			{Name: "agents", Metrics: []string{"agent_state"}, Sources: []string{"agents"}, Emit: e.emitAgents},
			{Name: "net_ip_avail", Metrics: []string{"network_ip_availabilities_total", "network_ip_availabilities_used"}, Sources: []string{"net_ip_avail"}, Emit: e.emitNetIPAvail},
			{Name: "subnet_pools", Metrics: []string{"subnets_total", "subnets_used", "subnets_free"}, Sources: []string{"subnet_pools", "subnets"}, Emit: e.emitSubnetPools},
			{Name: "quotas", Metrics: []string{"quota_network", "quota_subnet", "quota_subnetpool", "quota_port", "quota_router", "quota_floatingip", "quota_security_group", "quota_security_group_rule", "quota_rbac_policy"}, Sources: []string{"quotas"}, Emit: e.emitQuotas},
		},
	}
	sched, err := SetupExporter(&e.BaseOpenStackExporter, &e.descs, &graph)
	if err != nil {
		return nil, err
	}
	e.sched = sched
	return e, nil
}

func (e *NeutronExporter) Collect(ch chan<- prometheus.Metric) {
	CollectSchedule(&e.BaseOpenStackExporter, &e.sched, new(neutronScrape), ch)
}

// --- Sources ---

func (e *NeutronExporter) fetchFloatingIPs(ctx context.Context, s *neutronScrape) error {
	allPages, err := floatingips.List(e.ClientV2, floatingips.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.floatingIPs, err = floatingips.ExtractFloatingIPs(allPages)
	return err
}

func (e *NeutronExporter) fetchNetworks(ctx context.Context, s *neutronScrape) error {
	allPages, err := networks.List(e.ClientV2, networks.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}
	return networks.ExtractNetworksInto(allPages, &s.networks)
}

func (e *NeutronExporter) fetchSecGroups(ctx context.Context, s *neutronScrape) error {
	allPages, err := groups.List(e.ClientV2, groups.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.securityGroups, err = groups.ExtractGroups(allPages)
	return err
}

func (e *NeutronExporter) fetchSubnets(ctx context.Context, s *neutronScrape) error {
	allPages, err := subnets.List(e.ClientV2, subnets.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.subnets, err = subnets.ExtractSubnets(allPages)
	return err
}

func (e *NeutronExporter) fetchPorts(ctx context.Context, s *neutronScrape) error {
	allPages, err := ports.List(e.ClientV2, ports.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}
	return ports.ExtractPortsInto(allPages, &s.ports)
}

func (e *NeutronExporter) fetchRouters(ctx context.Context, s *neutronScrape) error {
	allPages, err := routers.List(e.ClientV2, routers.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.routers, err = routers.ExtractRouters(allPages)
	return err
}

// fetchRouterL3Agents lists the L3 agents hosting each router, one request
// per router. Neutron with the OVN backend has no router L3 agents, which is
// detected by the presence of ovn-controller agents; then nothing is fetched.
func (e *NeutronExporter) fetchRouterL3Agents(ctx context.Context, s *neutronScrape) error {
	ovnPages, err := agents.List(e.ClientV2, agents.ListOpts{Binary: "ovn-controller"}).AllPages(ctx)
	if err != nil {
		return err
	}
	ovnAgents, err := agents.ExtractAgents(ovnPages)
	if err != nil {
		return err
	}
	s.ovnBackend = len(ovnAgents) > 0
	if s.ovnBackend {
		return nil
	}

	perRouter := make([][]routers.L3Agent, len(s.routers))
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(e.GetAPIDetailConcurrencyCount())
	for i, router := range s.routers {
		g.Go(func() error {
			allPages, err := routers.ListL3Agents(e.ClientV2, router.ID).AllPages(gCtx)
			if err != nil {
				return err
			}
			perRouter[i], err = routers.ExtractL3Agents(allPages)
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	s.routerL3Agents = make(map[string][]routers.L3Agent, len(s.routers))
	for i, router := range s.routers {
		s.routerL3Agents[router.ID] = perRouter[i]
	}
	return nil
}

func (e *NeutronExporter) fetchVpnEndpointGroups(ctx context.Context, s *neutronScrape) error {
	allPages, err := endpointgroups.List(e.ClientV2, endpointgroups.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.vpnEndpointGroups, err = endpointgroups.ExtractEndpointGroups(allPages)
	return err
}

func (e *NeutronExporter) fetchVpnIKEPolicies(ctx context.Context, s *neutronScrape) error {
	allPages, err := ikepolicies.List(e.ClientV2, ikepolicies.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.vpnIKEPolicies, err = ikepolicies.ExtractPolicies(allPages)
	return err
}

func (e *NeutronExporter) fetchVpnIPsecPolicies(ctx context.Context, s *neutronScrape) error {
	allPages, err := ipsecpolicies.List(e.ClientV2, ipsecpolicies.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.vpnIPsecPolicies, err = ipsecpolicies.ExtractPolicies(allPages)
	return err
}

func (e *NeutronExporter) fetchVpnServices(ctx context.Context, s *neutronScrape) error {
	allPages, err := services.List(e.ClientV2, services.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.vpnServices, err = services.ExtractServices(allPages)
	return err
}

func (e *NeutronExporter) fetchVpnSiteConnections(ctx context.Context, s *neutronScrape) error {
	allPages, err := siteconnections.List(e.ClientV2, siteconnections.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.vpnSiteConnections, err = siteconnections.ExtractConnections(allPages)
	return err
}

func (e *NeutronExporter) fetchAgents(ctx context.Context, s *neutronScrape) error {
	allPages, err := agents.List(e.ClientV2, agents.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.agents, err = agents.ExtractAgents(allPages)
	return err
}

func (e *NeutronExporter) fetchNetIPAvail(ctx context.Context, s *neutronScrape) error {
	type customSubnetIPAvailability struct {
		SubnetName string      `json:"subnet_name"`
		CIDR       string      `json:"cidr"`
		IPVersion  int         `json:"ip_version"`
		TotalIPs   json.Number `json:"total_ips"`
		UsedIPs    json.Number `json:"used_ips"`
	}

	type customNetworkIPAvailability struct {
		NetworkID              string                       `json:"network_id"`
		NetworkName            string                       `json:"network_name"`
		ProjectID              string                       `json:"project_id"`
		TenantID               string                       `json:"tenant_id"`
		SubnetIPAvailabilities []customSubnetIPAvailability `json:"subnet_ip_availability"`
	}

	type availabilityWrapper struct {
		NetworkIPAvailabilities []customNetworkIPAvailability `json:"network_ip_availabilities"`
	}

	allPages, err := networkipavailabilities.List(e.ClientV2, networkipavailabilities.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}

	// Decode raw JSON manually to avoid gophercloud unmarshaling big.Int error
	body := allPages.GetBody()
	bodyMap, ok := body.(map[string]interface{})
	if !ok {
		return fmt.Errorf("unexpected type for body: %T", body)
	}

	bodyBytes, err := json.Marshal(bodyMap)
	if err != nil {
		return fmt.Errorf("failed to marshal body back to JSON: %w", err)
	}

	var wrapper availabilityWrapper
	if err := json.Unmarshal(bodyBytes, &wrapper); err != nil {
		return fmt.Errorf("failed to unmarshal network_ip_availabilities JSON: %w", err)
	}

	for _, network := range wrapper.NetworkIPAvailabilities {
		projectID := network.ProjectID
		if projectID == "" && network.TenantID != "" {
			projectID = network.TenantID
		}

		entry := neutronNetIPAvail{NetworkID: network.NetworkID, NetworkName: network.NetworkName, ProjectID: projectID}
		for _, subnet := range network.SubnetIPAvailabilities {
			// Use big.Float to parse TotalIPs
			totalBig := new(big.Float)
			if _, ok := totalBig.SetString(subnet.TotalIPs.String()); !ok {
				return fmt.Errorf("failed to parse total IPs: %s", subnet.TotalIPs.String())
			}
			totalFloat64, _ := totalBig.Float64()

			usedBig := new(big.Float)
			if _, ok := usedBig.SetString(subnet.UsedIPs.String()); !ok {
				return fmt.Errorf("failed to parse used IPs: %s", subnet.UsedIPs.String())
			}
			usedFloat64, _ := usedBig.Float64()

			entry.Subnets = append(entry.Subnets, neutronSubnetIPAvail{
				SubnetName: subnet.SubnetName,
				CIDR:       subnet.CIDR,
				IPVersion:  subnet.IPVersion,
				TotalIPs:   totalFloat64,
				UsedIPs:    usedFloat64,
			})
		}
		s.netIPAvailabilities = append(s.netIPAvailabilities, entry)
	}
	return nil
}

func (e *NeutronExporter) fetchSubnetPools(ctx context.Context, s *neutronScrape) error {
	allPages, err := subnetpools.List(e.ClientV2, subnetpools.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.subnetPools, err = subnetpools.ExtractSubnetPools(allPages)
	return err
}

func (e *NeutronExporter) fetchProjects(ctx context.Context, s *neutronScrape) error {
	var err error
	s.projects, err = GetProjects(ctx, &e.BaseOpenStackExporter)
	return err
}

// fetchQuotas reads the quota detail of every project, one request per
// project, from the neutron API.
func (e *NeutronExporter) fetchQuotas(ctx context.Context, s *neutronScrape) error {
	s.quotas = make([]neutronProjectQuota, len(s.projects))
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(e.GetAPIDetailConcurrencyCount())
	for i, p := range s.projects {
		g.Go(func() error {
			quota, err := quotas.GetDetail(gCtx, e.ClientV2, p.ID).Extract()
			if err != nil {
				return err
			}
			s.quotas[i] = neutronProjectQuota{projectName: p.Name, projectID: p.ID, quota: *quota}
			return nil
		})
	}
	return g.Wait()
}

// --- Emitters ---

func (e *NeutronExporter) emitFloatingIPs(_ context.Context, s *neutronScrape, ch chan<- prometheus.Metric) error {
	failedFIPs := 0
	for i := range s.floatingIPs {
		fip := &s.floatingIPs[i]
		emitGauge(ch, e.descs.FloatingIP, 1, fip.ID, fip.FloatingNetworkID, fip.RouterID, fip.Status, fip.ProjectID, fip.FloatingIP)

		if fip.FixedIP != "" && fip.Status != "ACTIVE" {
			failedFIPs++
		}
	}

	emitGauge(ch, e.descs.FloatingIPs, float64(len(s.floatingIPs)))
	emitGauge(ch, e.descs.FloatingIPsAssociatedNotActive, float64(failedFIPs))
	return nil
}

func (e *NeutronExporter) emitNetworks(_ context.Context, s *neutronScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.Networks, float64(len(s.networks)))
	for i := range s.networks {
		net := &s.networks[i]
		emitGauge(ch, e.descs.Network, float64(mapNetworkStatus(net.Status)), net.ID, net.TenantID, net.Status, net.Name,
			strconv.FormatBool(net.Shared), strconv.FormatBool(net.External), net.NetworkType,
			net.PhysicalNetwork, net.SegmentationID, strings.Join(net.Subnets, ","), strings.Join(net.Tags, ","), strconv.Itoa(net.MTU))
	}
	return nil
}

func (e *NeutronExporter) emitSecGroups(_ context.Context, s *neutronScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.SecurityGroups, float64(len(s.securityGroups)))
	return nil
}

func (e *NeutronExporter) emitSubnets(_ context.Context, s *neutronScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.Subnets, float64(len(s.subnets)))
	for i := range s.subnets {
		subnet := &s.subnets[i]
		emitGauge(ch, e.descs.Subnet, 1.0, subnet.ID, subnet.TenantID, subnet.Name, subnet.NetworkID, subnet.CIDR,
			subnet.GatewayIP, strconv.FormatBool(subnet.EnableDHCP), strings.Join(subnet.DNSNameservers, ","), strings.Join(subnet.Tags, ","))
	}
	return nil
}

func (e *NeutronExporter) emitPorts(_ context.Context, s *neutronScrape, ch chan<- prometheus.Metric) error {
	portsWithNoIP := float64(0)
	lbaasPortsInactive := float64(0)

	for i := range s.ports {
		port := &s.ports[i]
		if port.Status == "ACTIVE" && len(port.FixedIPs) == 0 {
			portsWithNoIP++
		}

		if port.DeviceOwner == "neutron:LOADBALANCERV2" && port.Status != "ACTIVE" {
			lbaasPortsInactive++
		}

		if e.descs.Port != nil {
			// Joining IPs into a string with ',' separator
			fixedIPs := make([]string, len(port.FixedIPs))
			for idx, fip := range port.FixedIPs {
				fixedIPs[idx] = fip.IPAddress
			}

			emitGauge(ch, e.descs.Port, 1, port.ID, port.NetworkID, port.MACAddress, port.DeviceOwner, port.DeviceID,
				port.Status, port.VIFType, strconv.FormatBool(port.AdminStateUp), strings.Join(fixedIPs, ","))
		}
	}

	// NOTE(mnaser): We should deprecate this and users can replace it by
	//               count(openstack_neutron_port)
	emitGauge(ch, e.descs.Ports, float64(len(s.ports)))

	// NOTE(mnaser): We should deprecate this and users can replace it by:
	//               count(openstack_neutron_port{device_owner="neutron:LOADBALANCERV2",status!="ACTIVE"})
	emitGauge(ch, e.descs.PortsLBNotActive, lbaasPortsInactive)

	emitGauge(ch, e.descs.PortsNoIPs, portsWithNoIP)
	return nil
}

func (e *NeutronExporter) emitRouters(_ context.Context, s *neutronScrape, ch chan<- prometheus.Metric) error {
	failedRouters := 0
	for i := range s.routers {
		router := &s.routers[i]
		if router.Status != "ACTIVE" {
			failedRouters++
		}

		emitGauge(ch, e.descs.Router, 1, router.ID, router.Name, router.ProjectID,
			strconv.FormatBool(router.AdminStateUp), router.Status, router.GatewayInfo.NetworkID)
	}

	emitGauge(ch, e.descs.Routers, float64(len(s.routers)))
	emitGauge(ch, e.descs.RoutersNotActive, float64(failedRouters))
	return nil
}

func (e *NeutronExporter) emitRouterL3Agents(_ context.Context, s *neutronScrape, ch chan<- prometheus.Metric) error {
	if s.ovnBackend {
		// ovn-backend doesn't have router l3-agent entity
		return nil
	}

	for i := range s.routers {
		router := &s.routers[i]
		for _, agent := range s.routerL3Agents[router.ID] {
			state := 0
			if agent.Alive {
				state = 1
			}

			emitGauge(ch, e.descs.L3AgentOfRouter, float64(state), router.ID, agent.ID,
				agent.HAState, strconv.FormatBool(agent.Alive), strconv.FormatBool(agent.AdminStateUp), agent.Host)
		}
	}
	return nil
}

func (e *NeutronExporter) emitVpnEndpointGroups(_ context.Context, s *neutronScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.VpnEndpointGroups, float64(len(s.vpnEndpointGroups)))
	return nil
}

func (e *NeutronExporter) emitVpnIKEPolicies(_ context.Context, s *neutronScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.VpnIKEPolicies, float64(len(s.vpnIKEPolicies)))
	return nil
}

func (e *NeutronExporter) emitVpnIPsecPolicies(_ context.Context, s *neutronScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.VpnIPsecPolicies, float64(len(s.vpnIPsecPolicies)))
	return nil
}

func (e *NeutronExporter) emitVpnServices(_ context.Context, s *neutronScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.VpnServices, float64(len(s.vpnServices)))
	for i := range s.vpnServices {
		service := &s.vpnServices[i]
		emitGauge(ch, e.descs.VpnService, float64(mapVpnServiceStatus(service.Status)),
			service.ID, service.ProjectID, service.SubnetID, service.RouterID, strconv.FormatBool(service.AdminStateUp),
			service.Name, service.ExternalV4IP, service.ExternalV6IP, service.FlavorID)
	}
	return nil
}

func (e *NeutronExporter) emitVpnSiteConnections(_ context.Context, s *neutronScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.VpnSiteConnections, float64(len(s.vpnSiteConnections)))
	for i := range s.vpnSiteConnections {
		connection := &s.vpnSiteConnections[i]
		emitGauge(ch, e.descs.VpnSiteConnection, float64(mapVpnConnectionStatus(connection.Status)),
			connection.ID, connection.ProjectID, strconv.FormatBool(connection.AdminStateUp), connection.Name,
			connection.VPNServiceID, connection.IKEPolicyID, connection.IPSecPolicyID, connection.PeerID,
			connection.PeerEPGroupID, connection.LocalID, connection.LocalEPGroupID)
	}
	return nil
}

func (e *NeutronExporter) emitAgents(_ context.Context, s *neutronScrape, ch chan<- prometheus.Metric) error {
	for i := range s.agents {
		agent := &s.agents[i]
		state := 0
		if agent.Alive {
			state = 1
		}

		adminState := "down"
		if agent.AdminStateUp {
			adminState = "up"
		}

		id := agent.ID
		if id == "" {
			var err error
			if id, err = e.UUIDGenFunc(); err != nil {
				return err
			}
		}

		emitGauge(ch, e.descs.AgentState, float64(state), id, agent.Host, agent.Binary, adminState, agent.AvailabilityZone)
	}
	return nil
}

func (e *NeutronExporter) emitNetIPAvail(_ context.Context, s *neutronScrape, ch chan<- prometheus.Metric) error {
	for i := range s.netIPAvailabilities {
		network := &s.netIPAvailabilities[i]
		for j := range network.Subnets {
			subnet := &network.Subnets[j]
			emitGauge(ch, e.descs.NetworkIPAvailabilitiesTotal, subnet.TotalIPs, network.NetworkID,
				network.NetworkName, strconv.Itoa(subnet.IPVersion), subnet.CIDR, subnet.SubnetName, network.ProjectID)
			emitGauge(ch, e.descs.NetworkIPAvailabilitiesUsed, subnet.UsedIPs, network.NetworkID,
				network.NetworkName, strconv.Itoa(subnet.IPVersion), subnet.CIDR, subnet.SubnetName, network.ProjectID)
		}
	}
	return nil
}

// emitSubnetPools : count used/free/total number of subnets per subnet pool
func (e *NeutronExporter) emitSubnetPools(_ context.Context, s *neutronScrape, ch chan<- prometheus.Metric) error {
	subnetPools, err := subnetpoolsWithSubnets(s.subnetPools, s.subnets)
	if err != nil {
		return err
	}

	for i := range subnetPools {
		subnetPool := &subnetPools[i]
		ipPrefixes, err := subnetPool.IPPrefixes()
		if err != nil {
			return err
		}

		for _, ipPrefix := range ipPrefixes {
			for prefixLength := subnetPool.MinPrefixLen; prefixLength <= subnetPool.MaxPrefixLen; prefixLength++ {
				if prefixLength < int(ipPrefix.Bits()) {
					continue
				}

				totalSubnets := math.Pow(2, float64(prefixLength-int(ipPrefix.Bits())))
				emitGauge(ch, e.descs.SubnetsTotal, totalSubnets, strconv.Itoa(subnetPool.IPversion), ipPrefix.String(), strconv.Itoa(prefixLength),
					subnetPool.ProjectID, subnetPool.ID, subnetPool.Name)

				usedSubnets := calculateUsedSubnets(subnetPool.subnets, ipPrefix, prefixLength)
				emitGauge(ch, e.descs.SubnetsUsed, usedSubnets, strconv.Itoa(subnetPool.IPversion), ipPrefix.String(), strconv.Itoa(prefixLength),
					subnetPool.ProjectID, subnetPool.ID, subnetPool.Name)

				freeSubnets, err := calculateFreeSubnets(&ipPrefix, subnetPool.subnets, prefixLength)
				if err != nil {
					return err
				}
				emitGauge(ch, e.descs.SubnetsFree, freeSubnets, strconv.Itoa(subnetPool.IPversion), ipPrefix.String(), strconv.Itoa(prefixLength),
					subnetPool.ProjectID, subnetPool.ID, subnetPool.Name)
			}
		}
	}
	return nil
}

func (e *NeutronExporter) emitQuotas(_ context.Context, s *neutronScrape, ch chan<- prometheus.Metric) error {
	for i := range s.quotas {
		entry := &s.quotas[i]
		emitNeutronQuotaDetail(ch, e.descs.QuotaNetwork, entry.quota.Network, entry.projectName, entry.projectID)
		emitNeutronQuotaDetail(ch, e.descs.QuotaSubnet, entry.quota.Subnet, entry.projectName, entry.projectID)
		emitNeutronQuotaDetail(ch, e.descs.QuotaSubnetPool, entry.quota.SubnetPool, entry.projectName, entry.projectID)
		emitNeutronQuotaDetail(ch, e.descs.QuotaPort, entry.quota.Port, entry.projectName, entry.projectID)
		emitNeutronQuotaDetail(ch, e.descs.QuotaRouter, entry.quota.Router, entry.projectName, entry.projectID)
		emitNeutronQuotaDetail(ch, e.descs.QuotaFloatingIP, entry.quota.FloatingIP, entry.projectName, entry.projectID)
		emitNeutronQuotaDetail(ch, e.descs.QuotaSecurityGroup, entry.quota.SecurityGroup, entry.projectName, entry.projectID)
		emitNeutronQuotaDetail(ch, e.descs.QuotaSecurityGroupRule, entry.quota.SecurityGroupRule, entry.projectName, entry.projectID)
		emitNeutronQuotaDetail(ch, e.descs.QuotaRBACPolicy, entry.quota.RBACPolicy, entry.projectName, entry.projectID)
	}
	return nil
}

func emitNeutronQuotaDetail(ch chan<- prometheus.Metric, desc *prometheus.Desc, q quotas.QuotaDetail, projectName, projectID string) {
	emitGauge(ch, desc, float64(q.Used), "used", projectName, projectID)
	emitGauge(ch, desc, float64(q.Reserved), "reserved", projectName, projectID)
	emitGauge(ch, desc, float64(q.Limit), "limit", projectName, projectID)
}

// --- Subnet pool helpers ---

// subnetpoolWithSubnets : subnetpools.SubnetPool augmented with its subnets
type subnetpoolWithSubnets struct {
	subnetpools.SubnetPool
	subnets []netip.Prefix
}

// IPPrefixes : returns a subnetpoolWithSubnets's prefixes converted to netip.Prefix structs.
func (s *subnetpoolWithSubnets) IPPrefixes() ([]netip.Prefix, error) {
	result := make([]netip.Prefix, len(s.Prefixes))

	for i, prefix := range s.Prefixes {
		ipPrefix, err := netip.ParsePrefix(prefix)
		if err != nil {
			return nil, err
		}

		result[i] = ipPrefix
	}

	return result, nil
}

// subnetpoolsWithSubnets : builds a slice of subnetpoolWithSubnets from subnetpools.SubnetPool and subnets.Subnet structs
func subnetpoolsWithSubnets(pools []subnetpools.SubnetPool, subnets []subnets.Subnet) ([]subnetpoolWithSubnets, error) {
	subnetPrefixes := make(map[string][]netip.Prefix)

	for _, subnet := range subnets {
		if subnet.SubnetPoolID != "" {
			subnetPrefix, err := netip.ParsePrefix(subnet.CIDR)
			if err != nil {
				return nil, err
			}

			subnetPrefixes[subnet.SubnetPoolID] = append(subnetPrefixes[subnet.SubnetPoolID], subnetPrefix)
		}
	}

	result := make([]subnetpoolWithSubnets, len(pools))
	for i, pool := range pools {
		result[i] = subnetpoolWithSubnets{pool, subnetPrefixes[pool.ID]}
	}

	return result, nil
}

// calculateFreeSubnets : Count how many CIDRs of length prefixLength there are in poolPrefix after removing subnetsInPool
func calculateFreeSubnets(poolPrefix *netip.Prefix, subnetsInPool []netip.Prefix, prefixLength int) (float64, error) {
	builder := netipx.IPSetBuilder{}
	builder.AddPrefix(*poolPrefix)

	for _, subnet := range subnetsInPool {
		builder.RemovePrefix(subnet)
	}

	ipset, err := builder.IPSet()
	if err != nil {
		return 0, err
	}

	count := 0.0
	for _, prefix := range ipset.Prefixes() {
		if int(prefix.Bits()) > prefixLength {
			continue
		}

		count += math.Pow(2, float64(prefixLength-int(prefix.Bits())))
	}

	return count, nil
}

// calculateUsedSubnets : find all subnets that overlap with ipPrefix and count the different subnet sizes.
// Finally, return the count that matches prefixLength.
func calculateUsedSubnets(subnets []netip.Prefix, ipPrefix netip.Prefix, prefixLength int) float64 {
	result := make(map[int]int)

	for _, subnet := range subnets {
		if !ipPrefix.Overlaps(subnet) {
			continue
		}

		result[int(subnet.Bits())]++
	}

	return float64(result[prefixLength])
}
