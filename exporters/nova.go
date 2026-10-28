package exporters

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"

	"github.com/gophercloud/gophercloud/v2/openstack/compute/v2/aggregates"
	"github.com/gophercloud/gophercloud/v2/openstack/compute/v2/availabilityzones"
	"github.com/gophercloud/gophercloud/v2/openstack/compute/v2/flavors"
	"github.com/gophercloud/gophercloud/v2/openstack/compute/v2/hypervisors"
	"github.com/gophercloud/gophercloud/v2/openstack/compute/v2/limits"
	"github.com/gophercloud/gophercloud/v2/openstack/compute/v2/quotasets"
	"github.com/gophercloud/gophercloud/v2/openstack/compute/v2/secgroups"
	"github.com/gophercloud/gophercloud/v2/openstack/compute/v2/servers"
	"github.com/gophercloud/gophercloud/v2/openstack/compute/v2/services"
	"github.com/gophercloud/gophercloud/v2/openstack/compute/v2/usage"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/projects"
	"github.com/openstack-exporter/openstack-exporter/utils"
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	RegisterTypedExporter("compute", NewNovaExporter)
}

// Latest supported microversion for Nova which provides all metrics
// See also: https://github.com/openstack-exporter/openstack-exporter/issues/249
const novaLatestSupportedMicroversion = "2.87"

var knownServerStatuses = map[string]int{
	"ACTIVE":            0,
	"BUILD":             1,  // The server has not finished the original build process.
	"BUILD(spawning)":   2,  // The server has not finished the original build process but networking works (HP Cloud specific)
	"DELETED":           3,  // The server is deleted.
	"ERROR":             4,  // The server is in error.
	"HARD_REBOOT":       5,  // The server is hard rebooting.
	"PASSWORD":          6,  // The password is being reset on the server.
	"REBOOT":            7,  // The server is in a soft reboot state.
	"REBUILD":           8,  // The server is currently being rebuilt from an image.
	"RESCUE":            9,  // The server is in rescue mode.
	"RESIZE":            10, // Server is performing the differential copy of data that changed during its initial copy.
	"SHUTOFF":           11, // The virtual machine (VM) was powered down by the user, but not through the OpenStack Compute API.
	"SUSPENDED":         12, // The server is suspended, either by request or necessity.
	"UNKNOWN":           13, // The state of the server is unknown. Contact your cloud provider.
	"VERIFY_RESIZE":     14, // System is awaiting confirmation that the server is operational after a move or resize.
	"MIGRATING":         15, // The server is migrating. This is caused by a live migration (moving a server that is active) action.
	"PAUSED":            16, // The server is paused.
	"REVERT_RESIZE":     17, // The resize or migration of a server failed for some reason. The destination server is being cleaned up and the original source server is restarting.
	"SHELVED":           18, // The server is in shelved state. Depends on the shelve offload time, the server will be automatically shelved off loaded.
	"SHELVED_OFFLOADED": 19, // The shelved server is offloaded (removed from the compute host) and it needs unshelved action to be used again.
	"SOFT_DELETED":      20, // The server is marked as deleted but will remain in the cloud for some configurable amount of time.
}

func mapServerStatus(current string) int {
	return mapStatus(knownServerStatuses, current)
}

// novaServerStatusBaseLabels are the fixed labels of server_status; the
// NovaMetadataMapping labels are appended at construction time.
var novaServerStatusBaseLabels = []string{"id", "status", "name", "tenant_id", "user_id", "address_ipv4",
	"address_ipv6", "host_id", "hypervisor_hostname", "uuid", "availability_zone", "flavor_id", "instance_libvirt"}

type NovaExporter struct {
	BaseOpenStackExporter
	descs novaDescs
	sched Schedule[novaScrape]
	// serverStatusDesc is built in NewNovaExporter: its label set depends on
	// --nova.metadata-extra-labels, so it cannot come from a struct tag.
	serverStatusDesc *prometheus.Desc
}

type novaDescs struct {
	Flavors                       *prometheus.Desc `metric:"flavors"`
	Flavor                        *prometheus.Desc `metric:"flavor"                            labels:"id,name,vcpus,ram,disk,is_public"`
	AvailabilityZones             *prometheus.Desc `metric:"availability_zones"`
	SecurityGroups                *prometheus.Desc `metric:"security_groups"`
	TotalVMs                      *prometheus.Desc `metric:"total_vms"`
	AgentState                    *prometheus.Desc `metric:"agent_state"                       labels:"id,hostname,service,adminState,zone,disabledReason"`
	RunningVMs                    *prometheus.Desc `metric:"running_vms"                       labels:"hostname,availability_zone,aggregates,tenant_id"`
	CurrentWorkload               *prometheus.Desc `metric:"current_workload"                  labels:"hostname,availability_zone,aggregates"`
	VcpusAvailable                *prometheus.Desc `metric:"vcpus_available"                   labels:"hostname,availability_zone,aggregates"`
	VcpusUsed                     *prometheus.Desc `metric:"vcpus_used"                        labels:"hostname,availability_zone,aggregates"`
	MemoryAvailableBytes          *prometheus.Desc `metric:"memory_available_bytes"            labels:"hostname,availability_zone,aggregates"`
	MemoryUsedBytes               *prometheus.Desc `metric:"memory_used_bytes"                 labels:"hostname,availability_zone,aggregates"`
	LocalStorageAvailableBytes    *prometheus.Desc `metric:"local_storage_available_bytes"     labels:"hostname,availability_zone,aggregates"`
	LocalStorageUsedBytes         *prometheus.Desc `metric:"local_storage_used_bytes"          labels:"hostname,availability_zone,aggregates"`
	FreeDiskBytes                 *prometheus.Desc `metric:"free_disk_bytes"                   labels:"hostname,availability_zone,aggregates"`
	LimitsVcpusMax                *prometheus.Desc `metric:"limits_vcpus_max"                  labels:"tenant,tenant_id" slow:"true"`
	LimitsVcpusUsed               *prometheus.Desc `metric:"limits_vcpus_used"                 labels:"tenant,tenant_id" slow:"true"`
	LimitsMemoryMax               *prometheus.Desc `metric:"limits_memory_max"                 labels:"tenant,tenant_id" slow:"true"`
	LimitsMemoryUsed              *prometheus.Desc `metric:"limits_memory_used"                labels:"tenant,tenant_id" slow:"true"`
	LimitsInstancesUsed           *prometheus.Desc `metric:"limits_instances_used"             labels:"tenant,tenant_id" slow:"true"`
	LimitsInstancesMax            *prometheus.Desc `metric:"limits_instances_max"              labels:"tenant,tenant_id" slow:"true"`
	ServerLocalGB                 *prometheus.Desc `metric:"server_local_gb"                   labels:"name,id,tenant_id" slow:"true"`
	QuotaCores                    *prometheus.Desc `metric:"quota_cores"                       labels:"type,tenant,tenant_id"`
	QuotaInstances                *prometheus.Desc `metric:"quota_instances"                   labels:"type,tenant,tenant_id"`
	QuotaKeyPairs                 *prometheus.Desc `metric:"quota_key_pairs"                   labels:"type,tenant,tenant_id"`
	QuotaMetadataItems            *prometheus.Desc `metric:"quota_metadata_items"              labels:"type,tenant,tenant_id"`
	QuotaRAM                      *prometheus.Desc `metric:"quota_ram"                         labels:"type,tenant,tenant_id"`
	QuotaServerGroups             *prometheus.Desc `metric:"quota_server_groups"               labels:"type,tenant,tenant_id"`
	QuotaServerGroupMembers       *prometheus.Desc `metric:"quota_server_group_members"        labels:"type,tenant,tenant_id"`
	QuotaFixedIPs                 *prometheus.Desc `metric:"quota_fixed_ips"                   labels:"type,tenant,tenant_id"`
	QuotaFloatingIPs              *prometheus.Desc `metric:"quota_floating_ips"                labels:"type,tenant,tenant_id"`
	QuotaSecurityGroupRules       *prometheus.Desc `metric:"quota_security_group_rules"        labels:"type,tenant,tenant_id"`
	QuotaSecurityGroups           *prometheus.Desc `metric:"quota_security_groups"             labels:"type,tenant,tenant_id"`
	QuotaInjectedFileContentBytes *prometheus.Desc `metric:"quota_injected_file_content_bytes" labels:"type,tenant,tenant_id"`
	QuotaInjectedFilePathBytes    *prometheus.Desc `metric:"quota_injected_file_path_bytes"    labels:"type,tenant,tenant_id"`
	QuotaInjectedFiles            *prometheus.Desc `metric:"quota_injected_files"              labels:"type,tenant,tenant_id"`
}

type novaScrape struct {
	flavors        []flavors.Flavor
	azs            []availabilityzones.AvailabilityZone
	securityGroups []secgroups.SecurityGroup
	servers        []servers.Server
	services       []services.Service
	hypervisors    []hypervisors.Hypervisor
	aggregates     []aggregates.Aggregate
	projects       []projects.Project
	limits         []novaProjectLimits
	quotas         []novaProjectQuotas
	tenantUsages   []usage.TenantUsage
}

type novaProjectLimits struct {
	projectName string
	projectID   string
	limits      limits.Limits
}

type novaProjectQuotas struct {
	projectName string
	projectID   string
	quotas      quotasets.QuotaDetailSet
}

func NewNovaExporter(config *ExporterConfig, logger *slog.Logger) (*NovaExporter, error) {
	ctx := context.TODO()

	err := utils.SetupClientMicroversionV2(ctx, config.ClientV2, "OS_COMPUTE_API_VERSION", novaLatestSupportedMicroversion, logger)
	if err != nil {
		return nil, err
	}

	e := &NovaExporter{
		BaseOpenStackExporter: BaseOpenStackExporter{
			Name:           "nova",
			ExporterConfig: *config,
			logger:         logger,
		},
	}
	graph := Graph[novaScrape]{
		Sources: []Source[novaScrape]{
			{Name: "flavors", Fetch: e.fetchFlavors},
			{Name: "azs", Fetch: e.fetchAZs},
			{Name: "secgroups", Fetch: e.fetchSecGroups},
			{Name: "servers", Fetch: e.fetchServers},
			{Name: "services", Fetch: e.fetchServices},
			{Name: "hypervisors", Fetch: e.fetchHypervisors},
			{Name: "aggregates", Fetch: e.fetchAggregates},
			{Name: "projects", Fetch: e.fetchProjects},
			{Name: "limits", DependsOn: []string{"projects"}, Fetch: e.fetchLimits},
			{Name: "quotas", DependsOn: []string{"projects"}, Fetch: e.fetchQuotas},
			{Name: "usage", Fetch: e.fetchUsage},
		},
		Emitters: []Emitter[novaScrape]{
			{Name: "flavors", Metrics: []string{"flavors", "flavor"}, Sources: []string{"flavors"}, Emit: e.emitFlavors},
			{Name: "azs", Metrics: []string{"availability_zones"}, Sources: []string{"azs"}, Emit: e.emitAZs},
			{Name: "secgroups", Metrics: []string{"security_groups"}, Sources: []string{"secgroups"}, Emit: e.emitSecGroups},
			{Name: "total_vms", Metrics: []string{"total_vms"}, Sources: []string{"servers"}, Emit: e.emitTotalVMs},
			{Name: "running_vms", Metrics: []string{"running_vms"}, Sources: []string{"servers"}, Emit: e.emitRunningVMs},
			{Name: "server_status", Metrics: []string{"server_status"}, Sources: []string{"servers", "flavors"}, Emit: e.emitServerStatus},
			{Name: "services", Metrics: []string{"agent_state"}, Sources: []string{"services"}, Emit: e.emitServices},
			{Name: "hypervisors", Metrics: []string{"current_workload", "vcpus_available", "vcpus_used",
				"memory_available_bytes", "memory_used_bytes", "local_storage_available_bytes",
				"local_storage_used_bytes", "free_disk_bytes"},
				Sources: []string{"hypervisors", "aggregates"}, Emit: e.emitHypervisors},
			{Name: "limits", Metrics: []string{"limits_vcpus_max", "limits_vcpus_used", "limits_memory_max",
				"limits_memory_used", "limits_instances_used", "limits_instances_max"},
				Sources: []string{"limits"}, Emit: e.emitLimits},
			{Name: "usage", Metrics: []string{"server_local_gb"}, Sources: []string{"usage"}, Emit: e.emitUsage},
			{Name: "quotas", Metrics: []string{"quota_cores", "quota_instances", "quota_key_pairs",
				"quota_metadata_items", "quota_ram", "quota_server_groups", "quota_server_group_members",
				"quota_fixed_ips", "quota_floating_ips", "quota_security_group_rules", "quota_security_groups",
				"quota_injected_file_content_bytes", "quota_injected_file_path_bytes", "quota_injected_files"},
				Sources: []string{"quotas"}, Emit: e.emitQuotas},
		},
	}

	// server_status carries extra labels from NovaMetadataMapping, so its
	// descriptor is declared dynamically instead of through a struct tag.
	serverStatus := WithDynamicMetrics([]string{"server_status"}, func(base *BaseOpenStackExporter) {
		if !base.IsMetricEnabled("server_status") {
			return
		}
		labels := append(slices.Clone(novaServerStatusBaseLabels), config.NovaMetadataMapping.Labels...)
		e.serverStatusDesc = prometheus.NewDesc(prometheus.BuildFQName(base.GetName(), "", "server_status"),
			"server_status", labels, nil)
		base.RegisterDesc(e.serverStatusDesc)
	})

	sched, err := SetupExporter(&e.BaseOpenStackExporter, &e.descs, &graph, serverStatus)
	if err != nil {
		return nil, err
	}
	e.sched = sched
	return e, nil
}

func (e *NovaExporter) Collect(ch chan<- prometheus.Metric) {
	CollectSchedule(&e.BaseOpenStackExporter, &e.sched, new(novaScrape), ch)
}

// --- Sources ---

func (e *NovaExporter) fetchFlavors(ctx context.Context, s *novaScrape) error {
	allPages, err := flavors.ListDetail(e.ClientV2, flavors.ListOpts{AccessType: "None"}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.flavors, err = flavors.ExtractFlavors(allPages)
	return err
}

func (e *NovaExporter) fetchAZs(ctx context.Context, s *novaScrape) error {
	allPages, err := availabilityzones.List(e.ClientV2).AllPages(ctx)
	if err != nil {
		return err
	}
	s.azs, err = availabilityzones.ExtractAvailabilityZones(allPages)
	return err
}

func (e *NovaExporter) fetchSecGroups(ctx context.Context, s *novaScrape) error {
	allPages, err := secgroups.List(e.ClientV2).AllPages(ctx)
	if err != nil {
		return err
	}
	s.securityGroups, err = secgroups.ExtractSecurityGroups(allPages)
	return err
}

func (e *NovaExporter) fetchServers(ctx context.Context, s *novaScrape) error {
	allPages, err := servers.List(e.ClientV2, getServerListOptions(e.TenantID)).AllPages(ctx)
	if err != nil {
		return err
	}
	return servers.ExtractServersInto(allPages, &s.servers)
}

func (e *NovaExporter) fetchServices(ctx context.Context, s *novaScrape) error {
	allPages, err := services.List(e.ClientV2, services.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.services, err = services.ExtractServices(allPages)
	return err
}

func (e *NovaExporter) fetchHypervisors(ctx context.Context, s *novaScrape) error {
	var listOpts *hypervisors.ListOpts

	// Hypervisors API supports paging with limit from microversion 2.33.
	// Keep explicit limit to avoid missing metrics on large clouds.
	if ok, _ := utils.IsMicroversionAtLeast(e.ClientV2.Microversion, "2.33"); ok {
		listOpts = &hypervisors.ListOpts{Limit: new(1000)}
	} else {
		listOpts = &hypervisors.ListOpts{}
	}

	allPages, err := hypervisors.List(e.ClientV2, listOpts).AllPages(ctx)
	if err != nil {
		return err
	}
	s.hypervisors, err = hypervisors.ExtractHypervisors(allPages)
	return err
}

func (e *NovaExporter) fetchAggregates(ctx context.Context, s *novaScrape) error {
	allPages, err := aggregates.List(e.ClientV2).AllPages(ctx)
	if err != nil {
		return err
	}
	s.aggregates, err = aggregates.ExtractAggregates(allPages)
	return err
}

func (e *NovaExporter) fetchProjects(ctx context.Context, s *novaScrape) error {
	var err error
	s.projects, err = GetProjects(ctx, &e.BaseOpenStackExporter)
	return err
}

// fetchLimits gets the absolute limits of every project, one request at a
// time. Limits are obtained from the nova API, so this exporter's client
// is used; the configured project is queried without a tenant_id filter.
func (e *NovaExporter) fetchLimits(ctx context.Context, s *novaScrape) error {
	s.limits = make([]novaProjectLimits, 0, len(s.projects))
	for _, p := range s.projects {
		limitGetOpts := limits.GetOpts{TenantID: p.ID}
		if p.ID == e.TenantID {
			limitGetOpts = limits.GetOpts{}
		}

		projectLimits, err := limits.Get(ctx, e.ClientV2, limitGetOpts).Extract()
		if err != nil {
			return err
		}
		s.limits = append(s.limits, novaProjectLimits{projectName: p.Name, projectID: p.ID, limits: *projectLimits})
	}
	return nil
}

func (e *NovaExporter) fetchQuotas(ctx context.Context, s *novaScrape) error {
	s.quotas = make([]novaProjectQuotas, 0, len(s.projects))
	for _, p := range s.projects {
		quotaSet, err := quotasets.GetDetail(ctx, e.ClientV2, p.ID).Extract()
		if err != nil {
			return err
		}
		s.quotas = append(s.quotas, novaProjectQuotas{projectName: p.Name, projectID: p.ID, quotas: quotaSet})
	}
	return nil
}

func (e *NovaExporter) fetchUsage(ctx context.Context, s *novaScrape) error {
	allPages, err := usage.AllTenants(e.ClientV2, usage.AllTenantsOpts{Detailed: true}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.tenantUsages, err = usage.ExtractAllTenants(allPages)
	return err
}

// --- Emitters ---

func (e *NovaExporter) emitFlavors(_ context.Context, s *novaScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.Flavors, float64(len(s.flavors)))
	for _, f := range s.flavors {
		emitGauge(ch, e.descs.Flavor, 1, f.ID, f.Name, fmt.Sprintf("%v", f.VCPUs), fmt.Sprintf("%v", f.RAM),
			fmt.Sprintf("%v", f.Disk), fmt.Sprintf("%v", f.IsPublic))
	}
	return nil
}

func (e *NovaExporter) emitAZs(_ context.Context, s *novaScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.AvailabilityZones, float64(len(s.azs)))
	return nil
}

func (e *NovaExporter) emitSecGroups(_ context.Context, s *novaScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.SecurityGroups, float64(len(s.securityGroups)))
	return nil
}

func (e *NovaExporter) emitTotalVMs(_ context.Context, s *novaScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.TotalVMs, float64(len(s.servers)))
	return nil
}

type novaRunningVMLabels struct {
	hostname         string
	availabilityZone string
	tenantID         string
}

func (e *NovaExporter) emitRunningVMs(_ context.Context, s *novaScrape, ch chan<- prometheus.Metric) error {
	runningVMs := map[novaRunningVMLabels]int{}
	for i := range s.servers {
		server := &s.servers[i]
		if !strings.EqualFold(server.Status, "ACTIVE") {
			continue
		}
		labels := novaRunningVMLabels{
			hostname:         server.HypervisorHostname,
			availabilityZone: server.AvailabilityZone,
			tenantID:         server.TenantID,
		}
		runningVMs[labels]++
	}
	for labels, count := range runningVMs {
		emitGauge(ch, e.descs.RunningVMs, float64(count), labels.hostname, labels.availabilityZone, "", labels.tenantID)
	}
	return nil
}

func (e *NovaExporter) emitServerStatus(_ context.Context, s *novaScrape, ch chan<- prometheus.Metric) error {
	// https://docs.openstack.org/api-ref/compute/#list-servers-detailed
	// ***
	// If micro-version is greater than 2.46, the server's flavor carries
	// no id, only its original_name; map the name back to the id through
	// the flavor list. The same mapping is used whenever a server lacks
	// flavor.id, whatever the negotiated microversion.
	var mapper flavorIDMapper
	mvAtLeast247, _ := utils.IsMicroversionAtLeast(e.ClientV2.Microversion, "2.47")
	if mvAtLeast247 || serversNeedFlavorIDMapper(s.servers) {
		mapper = newFlavorIDMapper(s.flavors)
	}

	for i := range s.servers {
		server := &s.servers[i]
		var flavorID string
		if mapper == nil {
			flavorID = fmt.Sprintf("%v", server.Flavor["id"])
		} else {
			flavorID = mapper.Search(server.Flavor["original_name"])
		}
		labelValues := []string{
			server.ID, server.Status, server.Name, server.TenantID,
			server.UserID, server.AccessIPv4, server.AccessIPv6, server.HostID, server.HypervisorHostname, server.ID,
			server.AvailabilityZone, flavorID, server.InstanceName,
		}
		metadataValues := e.NovaMetadataMapping.Extract(server.Metadata)

		emitGauge(ch, e.serverStatusDesc, float64(mapServerStatus(server.Status)), append(labelValues, metadataValues...)...)
	}
	return nil
}

func (e *NovaExporter) emitServices(_ context.Context, s *novaScrape, ch chan<- prometheus.Metric) error {
	for _, service := range s.services {
		var state = 0
		if service.State == "up" {
			state = 1
		}
		emitGauge(ch, e.descs.AgentState, float64(state),
			service.ID, service.Host, service.Binary, service.Status, service.Zone, service.DisabledReason)
	}
	return nil
}

func (e *NovaExporter) emitHypervisors(_ context.Context, s *novaScrape, ch chan<- prometheus.Metric) error {
	hostToAzMap := map[string]string{}     // map of hypervisors and in which AZ they are
	hostToAggrMap := map[string][]string{} // map of hypervisors and of which aggregates they are part of
	for _, a := range s.aggregates {
		isAzAggregate := isAzAggregate(a)
		for _, h := range a.Hosts {
			// Map the AZ of this aggregate to each host part of this aggregate
			if a.AvailabilityZone != "" {
				hostToAzMap[h] = a.AvailabilityZone
			}
			// Map the aggregate name to each host part of this aggregate
			if !isAzAggregate {
				hostToAggrMap[h] = append(hostToAggrMap[h], a.Name)
			}
		}
	}

	for i := range s.hypervisors {
		hypervisor := &s.hypervisors[i]
		availabilityZone := hostToAzMap[hypervisor.Service.Host]
		aggregates := aggregatesLabel(hypervisor.Service.Host, hostToAggrMap)

		var vcpus int
		if !reflect.ValueOf(hypervisor.CPUInfo).IsZero() {
			vcpus = max(hypervisor.CPUInfo.Topology.Cells, 1) * hypervisor.CPUInfo.Topology.Sockets * hypervisor.CPUInfo.Topology.Cores * hypervisor.CPUInfo.Topology.Threads
		} else {
			vcpus = hypervisor.VCPUs
		}

		emit := func(desc *prometheus.Desc, value float64) {
			emitGauge(ch, desc, value, hypervisor.HypervisorHostname, availabilityZone, aggregates)
		}
		emit(e.descs.CurrentWorkload, float64(hypervisor.CurrentWorkload))
		emit(e.descs.VcpusAvailable, float64(vcpus))
		emit(e.descs.VcpusUsed, float64(hypervisor.VCPUsUsed))
		emit(e.descs.MemoryAvailableBytes, float64(hypervisor.MemoryMB*MEGABYTE))
		emit(e.descs.MemoryUsedBytes, float64(hypervisor.MemoryMBUsed*MEGABYTE))
		emit(e.descs.LocalStorageAvailableBytes, float64(hypervisor.LocalGB*GIGABYTE))
		emit(e.descs.LocalStorageUsedBytes, float64(hypervisor.LocalGBUsed*GIGABYTE))
		emit(e.descs.FreeDiskBytes, float64(hypervisor.FreeDiskGB*GIGABYTE))
	}
	return nil
}

func (e *NovaExporter) emitLimits(_ context.Context, s *novaScrape, ch chan<- prometheus.Metric) error {
	for i := range s.limits {
		p := &s.limits[i]
		absolute := p.limits.Absolute
		emit := func(desc *prometheus.Desc, value float64) {
			emitGauge(ch, desc, value, p.projectName, p.projectID)
		}
		emit(e.descs.LimitsVcpusMax, float64(absolute.MaxTotalCores))
		emit(e.descs.LimitsVcpusUsed, float64(absolute.TotalCoresUsed))
		emit(e.descs.LimitsMemoryMax, float64(absolute.MaxTotalRAMSize))
		emit(e.descs.LimitsMemoryUsed, float64(absolute.TotalRAMUsed))
		emit(e.descs.LimitsInstancesUsed, float64(absolute.TotalInstancesUsed))
		emit(e.descs.LimitsInstancesMax, float64(absolute.MaxTotalInstances))
	}
	return nil
}

func (e *NovaExporter) emitUsage(_ context.Context, s *novaScrape, ch chan<- prometheus.Metric) error {
	for _, tenant := range s.tenantUsages {
		for _, server := range tenant.ServerUsages {
			emitGauge(ch, e.descs.ServerLocalGB, float64(server.LocalGB), server.Name, server.InstanceID, tenant.TenantID)
		}
	}
	return nil
}

func (e *NovaExporter) emitQuotas(_ context.Context, s *novaScrape, ch chan<- prometheus.Metric) error {
	for i := range s.quotas {
		p := &s.quotas[i]
		q := &p.quotas
		emit := func(desc *prometheus.Desc, detail quotasets.QuotaDetail) {
			emitGauge(ch, desc, float64(detail.InUse), "in_use", p.projectName, p.projectID)
			emitGauge(ch, desc, float64(detail.Reserved), "reserved", p.projectName, p.projectID)
			emitGauge(ch, desc, float64(detail.Limit), "limit", p.projectName, p.projectID)
		}
		emit(e.descs.QuotaCores, q.Cores)
		emit(e.descs.QuotaInstances, q.Instances)
		emit(e.descs.QuotaKeyPairs, q.KeyPairs)
		emit(e.descs.QuotaMetadataItems, q.MetadataItems)
		emit(e.descs.QuotaRAM, q.RAM)
		emit(e.descs.QuotaServerGroups, q.ServerGroups)
		emit(e.descs.QuotaServerGroupMembers, q.ServerGroupMembers)
		emit(e.descs.QuotaFixedIPs, q.FixedIPs)
		emit(e.descs.QuotaFloatingIPs, q.FloatingIPs)
		emit(e.descs.QuotaSecurityGroupRules, q.SecurityGroupRules)
		emit(e.descs.QuotaSecurityGroups, q.SecurityGroups)
		emit(e.descs.QuotaInjectedFileContentBytes, q.InjectedFileContentBytes)
		emit(e.descs.QuotaInjectedFilePathBytes, q.InjectedFilePathBytes)
		emit(e.descs.QuotaInjectedFiles, q.InjectedFiles)
	}
	return nil
}

// --- Helpers ---

func getServerListOptions(tenantID string) servers.ListOpts {
	if tenantID == "" {
		return servers.ListOpts{AllTenants: true}
	}
	return servers.ListOpts{TenantID: tenantID}
}

// Help function to determine if this aggregate has only the 'availability_zone' metadata
// attribute set. If so, the only purpose of the aggregate is to set the AZ for its member hosts.
func isAzAggregate(a aggregates.Aggregate) bool {
	if len(a.Metadata) == 1 {
		if _, ok := a.Metadata["availability_zone"]; ok {
			return true
		}
	}
	return false
}

func aggregatesLabel(h string, hostToAggrMap map[string][]string) string {
	if aggregates, ok := hostToAggrMap[h]; ok {
		slices.Sort(aggregates)
		return strings.Join(aggregates, ",")
	}
	return ""
}

// flavorIDMapper helper storage to map from Flavor Name to ID
type flavorIDMapper map[string]string

func newFlavorIDMapper(allFlavors []flavors.Flavor) flavorIDMapper {
	m := make(flavorIDMapper, len(allFlavors))
	for _, f := range allFlavors {
		m[f.Name] = f.ID
	}
	return m
}

// serversNeedFlavorIDMapper reports whether any server lacks flavor.id in
// its detail, which is what a 2.47+ response looks like.
func serversNeedFlavorIDMapper(allServers []servers.Server) bool {
	for i := range allServers {
		if _, ok := allServers[i].Flavor["id"]; !ok {
			return true
		}
	}
	return false
}

func (s flavorIDMapper) Search(flavorName any) string {
	// flavor name is unique, making it suitable as the key for searching
	key, ok := flavorName.(string)
	if !ok {
		return ""
	}

	return s[key]
}
