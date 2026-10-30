package exporters

import (
	"context"
	"log/slog"
	"strconv"
	"strings"

	"github.com/gophercloud/gophercloud/v2/openstack/blockstorage/v3/backups"
	"github.com/gophercloud/gophercloud/v2/openstack/blockstorage/v3/quotasets"
	"github.com/gophercloud/gophercloud/v2/openstack/blockstorage/v3/schedulerstats"
	"github.com/gophercloud/gophercloud/v2/openstack/blockstorage/v3/services"
	"github.com/gophercloud/gophercloud/v2/openstack/blockstorage/v3/snapshots"
	"github.com/gophercloud/gophercloud/v2/openstack/blockstorage/v3/volumes"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/projects"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
)

func init() {
	RegisterTypedExporter("volume", NewCinderExporter)
}

type CinderExporter struct {
	BaseOpenStackExporter
	descs cinderDescs
	sched Schedule[cinderScrape]
}

// Microversion required for the os-backup-project-attr:project_id attribute
// in backup listings, available since Pike (2017).
const backupProjectIDMicroversion = "3.18"

var knownVolumeStatuses = map[string]int{
	"creating":          0,
	"available":         1,
	"reserved":          2,
	"attaching":         3,
	"detaching":         4,
	"in-use":            5,
	"maintenance":       6,
	"deleting":          7,
	"awaiting-transfer": 8,
	"error":             9,
	"error_deleting":    10,
	"backing-up":        11,
	"restoring-backup":  12,
	"error_backing-up":  13,
	"error_restoring":   14,
	"error_extending":   15,
	"downloading":       16,
	"uploading":         17,
	"retyping":          18,
	"extending":         19,
}

func mapVolumeStatus(volStatus string) int {
	return mapStatus(knownVolumeStatuses, volStatus)
}

type cinderDescs struct {
	Volumes             *prometheus.Desc `metric:"volumes"`
	Snapshots           *prometheus.Desc `metric:"snapshots"`
	SnapshotGB          *prometheus.Desc `metric:"snapshot_gb"                 labels:"id,name,status,tenant_id,volume_id"`
	Backups             *prometheus.Desc `metric:"backups"`
	BackupGB            *prometheus.Desc `metric:"backup_gb"                   labels:"id,name,status,tenant_id,volume_id,snapshot_id,incremental"`
	AgentState          *prometheus.Desc `metric:"agent_state"                 labels:"uuid,hostname,service,adminState,zone,disabledReason"`
	VolumeGB            *prometheus.Desc `metric:"volume_gb"                   labels:"id,name,status,availability_zone,bootable,tenant_id,user_id,volume_type,server_id"`
	VolumeStatus        *prometheus.Desc `metric:"volume_status"               labels:"id,name,status,bootable,tenant_id,size,volume_type,server_id"                   deprecated:"1.4"`
	VolumeStatusCounter *prometheus.Desc `metric:"volume_status_counter"       labels:"status"`
	PoolCapacityFreeGB  *prometheus.Desc `metric:"pool_capacity_free_gb"       labels:"name,volume_backend_name,vendor_name"`
	PoolCapacityTotalGB *prometheus.Desc `metric:"pool_capacity_total_gb"      labels:"name,volume_backend_name,vendor_name"`
	LimitsVolumeMaxGB   *prometheus.Desc `metric:"limits_volume_max_gb"        labels:"tenant,tenant_id"             slow:"true"`
	LimitsVolumeUsedGB  *prometheus.Desc `metric:"limits_volume_used_gb"       labels:"tenant,tenant_id"             slow:"true"`
	LimitsBackupMaxGB   *prometheus.Desc `metric:"limits_backup_max_gb"        labels:"tenant,tenant_id"             slow:"true"`
	LimitsBackupUsedGB  *prometheus.Desc `metric:"limits_backup_used_gb"       labels:"tenant,tenant_id"             slow:"true"`
	VolumeTypeQuota     *prometheus.Desc `metric:"volume_type_quota_gigabytes" labels:"tenant,tenant_id,volume_type" slow:"true"`
}

type cinderScrape struct {
	volumes   []volumes.Volume
	snapshots []snapshots.Snapshot
	backups   []backups.Backup
	agents    []services.Service
	pools     []schedulerstats.StoragePool
	projects  []projects.Project
	limits    []cinderProjectLimits
}

// cinderProjectLimits bundles the quota usage and quota set of one project.
type cinderProjectLimits struct {
	projectName string
	projectID   string
	usage       quotasets.QuotaUsageSet
	quotas      quotasets.QuotaSet
}

func NewCinderExporter(config *ExporterConfig, logger *slog.Logger) (*CinderExporter, error) {
	e := &CinderExporter{
		BaseOpenStackExporter: BaseOpenStackExporter{
			Name:           "cinder",
			ExporterConfig: *config,
			logger:         logger,
		},
	}
	graph := Graph[cinderScrape]{
		Sources: []Source[cinderScrape]{
			{Name: "volumes", Fetch: e.fetchVolumes},
			{Name: "snapshots", Fetch: e.fetchSnapshots},
			{Name: "backups", Fetch: e.fetchBackups},
			{Name: "agents", Fetch: e.fetchAgents},
			{Name: "pools", Fetch: e.fetchPools},
			{Name: "projects", Fetch: e.fetchProjects},
			{Name: "limits", DependsOn: []string{"projects"}, Fetch: e.fetchLimits},
		},
		Emitters: []Emitter[cinderScrape]{
			{Name: "volumes", Metrics: []string{"volumes", "volume_gb", "volume_status_counter"}, Sources: []string{"volumes"}, Emit: e.emitVolumes},
			{Name: "volume_status", Metrics: []string{"volume_status"}, Sources: []string{"volumes"}, Emit: e.emitVolumeStatus},
			{Name: "snapshots", Metrics: []string{"snapshots", "snapshot_gb"}, Sources: []string{"snapshots"}, Emit: e.emitSnapshots},
			{Name: "backups", Metrics: []string{"backups", "backup_gb"}, Sources: []string{"backups"}, Emit: e.emitBackups},
			{Name: "agents", Metrics: []string{"agent_state"}, Sources: []string{"agents"}, Emit: e.emitAgents},
			{Name: "pools", Metrics: []string{"pool_capacity_free_gb", "pool_capacity_total_gb"}, Sources: []string{"pools"}, Emit: e.emitPools},
			{Name: "limits", Metrics: []string{"limits_volume_max_gb", "limits_volume_used_gb", "limits_backup_max_gb", "limits_backup_used_gb", "volume_type_quota_gigabytes"}, Sources: []string{"limits"}, Emit: e.emitLimits},
		},
	}
	sched, err := SetupExporter(&e.BaseOpenStackExporter, &e.descs, &graph)
	if err != nil {
		return nil, err
	}
	e.sched = sched
	return e, nil
}

func (e *CinderExporter) Collect(ch chan<- prometheus.Metric) {
	CollectSchedule(&e.BaseOpenStackExporter, &e.sched, new(cinderScrape), ch)
}

func (e *CinderExporter) fetchVolumes(ctx context.Context, s *cinderScrape) error {
	allPages, err := volumes.List(e.ClientV2, getVolumeListOptions(e.TenantID)).AllPages(ctx)
	if err != nil {
		return err
	}
	return volumes.ExtractVolumesInto(allPages, &s.volumes)
}

func (e *CinderExporter) fetchSnapshots(ctx context.Context, s *cinderScrape) error {
	// The detail listing is required to get the snapshot project ID.
	allPages, err := snapshots.ListDetail(e.ClientV2, getSnapshotListOptions(e.TenantID)).AllPages(ctx)
	if err != nil {
		return err
	}
	s.snapshots, err = snapshots.ExtractSnapshots(allPages)
	return err
}

func (e *CinderExporter) fetchBackups(ctx context.Context, s *cinderScrape) error {
	// The detail listing is required to get size, status and project ID of
	// each backup. ListDetailOpts cannot filter on a project, so when the
	// exporter is scoped to a single tenant the result is filtered below.
	listOpts := backups.ListDetailOpts{AllTenants: e.TenantID == ""}

	// The project ID attribute is only returned with microversion 3.18 or
	// later; a copy of the client is used to not affect other metrics.
	client := *e.ClientV2
	client.Microversion = backupProjectIDMicroversion

	allPages, err := backups.ListDetail(&client, listOpts).AllPages(ctx)
	if err != nil {
		return err
	}
	allBackups, err := backups.ExtractBackups(allPages)
	if err != nil {
		return err
	}

	s.backups = make([]backups.Backup, 0, len(allBackups))
	for _, backup := range allBackups {
		if e.TenantID != "" && backup.ProjectID != e.TenantID {
			continue
		}
		s.backups = append(s.backups, backup)
	}
	return nil
}

func (e *CinderExporter) fetchAgents(ctx context.Context, s *cinderScrape) error {
	allPages, err := services.List(e.ClientV2, services.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.agents, err = services.ExtractServices(allPages)
	return err
}

func (e *CinderExporter) fetchPools(ctx context.Context, s *cinderScrape) error {
	allPages, err := schedulerstats.List(e.ClientV2, schedulerstats.ListOpts{Detail: true}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.pools, err = schedulerstats.ExtractStoragePools(allPages)
	return err
}

func (e *CinderExporter) fetchProjects(ctx context.Context, s *cinderScrape) error {
	var err error
	s.projects, err = GetProjects(ctx, &e.BaseOpenStackExporter)
	return err
}

func (e *CinderExporter) fetchLimits(ctx context.Context, s *cinderScrape) error {
	// Limits and quotas are obtained from the cinder API, so this exporter's
	// client is used. Each goroutine writes its own slot of s.limits.
	s.limits = make([]cinderProjectLimits, len(s.projects))
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(e.GetAPIDetailConcurrencyCount())
	for i, p := range s.projects {
		g.Go(func() error {
			usage, err := quotasets.GetUsage(gCtx, e.ClientV2, p.ID).Extract()
			if err != nil {
				return err
			}
			quotas, err := quotasets.Get(gCtx, e.ClientV2, p.ID).Extract()
			if err != nil {
				return err
			}
			s.limits[i] = cinderProjectLimits{
				projectName: p.Name,
				projectID:   p.ID,
				usage:       usage,
				quotas:      *quotas,
			}
			return nil
		})
	}
	return g.Wait()
}

func volumeServerID(volume *volumes.Volume) string {
	if len(volume.Attachments) > 0 {
		return volume.Attachments[0].ServerID
	}
	return ""
}

func (e *CinderExporter) emitVolumes(_ context.Context, s *cinderScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.Volumes, float64(len(s.volumes)))

	statusCounter := make(map[string]int, len(knownVolumeStatuses))
	for k := range knownVolumeStatuses {
		statusCounter[k] = 0
	}

	for i := range s.volumes {
		volume := &s.volumes[i]
		emitGauge(ch, e.descs.VolumeGB, float64(volume.Size), volume.ID, volume.Name,
			volume.Status, volume.AvailabilityZone, volume.Bootable, volume.TenantID, volume.UserID, volume.VolumeType, volumeServerID(volume))
		statusCounter[volume.Status]++
	}

	for status, count := range statusCounter {
		emitGauge(ch, e.descs.VolumeStatusCounter, float64(count), status)
	}
	return nil
}

func (e *CinderExporter) emitVolumeStatus(_ context.Context, s *cinderScrape, ch chan<- prometheus.Metric) error {
	for i := range s.volumes {
		volume := &s.volumes[i]
		emitGauge(ch, e.descs.VolumeStatus, float64(mapVolumeStatus(volume.Status)), volume.ID, volume.Name,
			volume.Status, volume.Bootable, volume.TenantID, strconv.Itoa(volume.Size), volume.VolumeType, volumeServerID(volume))
	}
	return nil
}

func (e *CinderExporter) emitSnapshots(_ context.Context, s *cinderScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.Snapshots, float64(len(s.snapshots)))
	for i := range s.snapshots {
		snapshot := &s.snapshots[i]
		emitGauge(ch, e.descs.SnapshotGB, float64(snapshot.Size), snapshot.ID, snapshot.Name,
			snapshot.Status, snapshot.ProjectID, snapshot.VolumeID)
	}
	return nil
}

func (e *CinderExporter) emitBackups(_ context.Context, s *cinderScrape, ch chan<- prometheus.Metric) error {
	for i := range s.backups {
		backup := &s.backups[i]
		emitGauge(ch, e.descs.BackupGB, float64(backup.Size), backup.ID, backup.Name,
			backup.Status, backup.ProjectID, backup.VolumeID, backup.SnapshotID,
			strconv.FormatBool(backup.IsIncremental))
	}
	emitGauge(ch, e.descs.Backups, float64(len(s.backups)))
	return nil
}

func (e *CinderExporter) emitAgents(_ context.Context, s *cinderScrape, ch chan<- prometheus.Metric) error {
	for i := range s.agents {
		agent := &s.agents[i]
		state := 0.0
		if agent.State == "up" {
			state = 1
		}
		var id string
		if !e.DisableCinderAgentUUID {
			var err error
			if id, err = e.UUIDGenFunc(); err != nil {
				return err
			}
		}
		emitGauge(ch, e.descs.AgentState, state, id, agent.Host, agent.Binary, agent.Status, agent.Zone, agent.DisabledReason)
	}
	return nil
}

func (e *CinderExporter) emitPools(_ context.Context, s *cinderScrape, ch chan<- prometheus.Metric) error {
	for i := range s.pools {
		pool := &s.pools[i]
		emitGauge(ch, e.descs.PoolCapacityFreeGB, float64(pool.Capabilities.FreeCapacityGB),
			pool.Name, pool.Capabilities.VolumeBackendName, pool.Capabilities.VendorName)
		emitGauge(ch, e.descs.PoolCapacityTotalGB, float64(pool.Capabilities.TotalCapacityGB),
			pool.Name, pool.Capabilities.VolumeBackendName, pool.Capabilities.VendorName)
	}
	return nil
}

func (e *CinderExporter) emitLimits(_ context.Context, s *cinderScrape, ch chan<- prometheus.Metric) error {
	for i := range s.limits {
		lim := &s.limits[i]

		// Loop through all Extra quotas to automatically detect volume types
		for key, value := range lim.quotas.Extra {
			if strings.HasPrefix(key, "gigabytes_") {
				volumeType := strings.TrimPrefix(key, "gigabytes_")
				if quotaValue, ok := value.(float64); ok {
					emitGauge(ch, e.descs.VolumeTypeQuota, quotaValue, lim.projectName, lim.projectID, volumeType)
				}
			}
		}

		emitGauge(ch, e.descs.LimitsVolumeMaxGB, float64(lim.usage.Gigabytes.Limit), lim.projectName, lim.projectID)
		emitGauge(ch, e.descs.LimitsVolumeUsedGB, float64(lim.usage.Gigabytes.InUse), lim.projectName, lim.projectID)
		emitGauge(ch, e.descs.LimitsBackupMaxGB, float64(lim.usage.BackupGigabytes.Limit), lim.projectName, lim.projectID)
		emitGauge(ch, e.descs.LimitsBackupUsedGB, float64(lim.usage.BackupGigabytes.InUse), lim.projectName, lim.projectID)
	}
	return nil
}

func getVolumeListOptions(tenantID string) volumes.ListOpts {
	if tenantID == "" {
		return volumes.ListOpts{AllTenants: true}
	}
	return volumes.ListOpts{TenantID: tenantID}
}

func getSnapshotListOptions(tenantID string) snapshots.ListOpts {
	if tenantID == "" {
		return snapshots.ListOpts{AllTenants: true}
	}
	return snapshots.ListOpts{TenantID: tenantID}
}
