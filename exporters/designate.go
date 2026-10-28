package exporters

import (
	"context"
	"log/slog"
	"strings"

	"github.com/gophercloud/gophercloud/v2/openstack/dns/v2/recordsets"
	"github.com/gophercloud/gophercloud/v2/openstack/dns/v2/zones"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
)

func init() {
	RegisterTypedExporter("dns", NewDesignateExporter)
}

type DesignateExporter struct {
	BaseOpenStackExporter
	descs designateDescs
	sched Schedule[designateScrape]
}

type designateDescs struct {
	Zones            *prometheus.Desc `metric:"zones"`
	ZoneStatus       *prometheus.Desc `metric:"zone_status"       labels:"id,name,status,tenant_id,type"`
	Recordsets       *prometheus.Desc `metric:"recordsets"        labels:"zone_id,zone_name,tenant_id"`
	RecordsetsStatus *prometheus.Desc `metric:"recordsets_status" labels:"id,name,status,zone_id,zone_name,type"`
}

type designateScrape struct {
	zones []zones.Zone
	// recordsets is indexed like zones; an entry is ok once its zone's
	// recordsets were listed successfully.
	recordsets []designateZoneRecordsets
}

type designateZoneRecordsets struct {
	zone       *zones.Zone
	recordsets []recordsets.RecordSet
	ok         bool
}

var zone_status = []string{
	"pending",
	"active",
	"deleted",
	"error",
}

var recordset_status = []string{
	"pending",
	"active",
	"deleted",
	"error",
}

func mapZoneStatus(zoneStatus string) int {
	for idx, status := range zone_status {
		if status == strings.ToLower(zoneStatus) {
			return idx
		}
	}
	return -1
}

func mapRecordsetStatus(recordsetStatus string) int {
	for idx, status := range recordset_status {
		if status == strings.ToLower(recordsetStatus) {
			return idx
		}
	}
	return -1
}

func NewDesignateExporter(config *ExporterConfig, logger *slog.Logger) (*DesignateExporter, error) {
	e := &DesignateExporter{
		BaseOpenStackExporter: BaseOpenStackExporter{
			ExporterConfig: *config,
			Name:           "designate",
			logger:         logger,
		},
	}

	// This header needed for colletiong zone of all projects
	e.ClientV2.MoreHeaders = map[string]string{"X-Auth-All-Projects": "True"}

	graph := Graph[designateScrape]{
		Sources: []Source[designateScrape]{
			{Name: "zones", Fetch: e.fetchZones},
			{Name: "recordsets", DependsOn: []string{"zones"}, Fetch: e.fetchRecordsets},
		},
		Emitters: []Emitter[designateScrape]{
			{Name: "zones", Metrics: []string{"zones", "zone_status"}, Sources: []string{"zones"}, Emit: e.emitZones},
			{Name: "recordsets", Metrics: []string{"recordsets", "recordsets_status"}, Sources: []string{"recordsets"}, Emit: e.emitRecordsets},
		},
	}
	sched, err := SetupExporter(&e.BaseOpenStackExporter, &e.descs, &graph)
	if err != nil {
		return nil, err
	}
	e.sched = sched
	return e, nil
}

func (e *DesignateExporter) Collect(ch chan<- prometheus.Metric) {
	CollectSchedule(&e.BaseOpenStackExporter, &e.sched, new(designateScrape), ch)
}

func (e *DesignateExporter) fetchZones(ctx context.Context, s *designateScrape) error {
	allPages, err := zones.List(e.ClientV2, zones.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.zones, err = zones.ExtractZones(allPages)
	return err
}

// fetchRecordsets lists the recordsets of every zone, at most
// GetDnsConcurrencyCount() zones at a time. As in the legacy collector a
// single failed zone fails the whole fetch.
func (e *DesignateExporter) fetchRecordsets(ctx context.Context, s *designateScrape) error {
	s.recordsets = make([]designateZoneRecordsets, len(s.zones))
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(e.GetDnsConcurrencyCount())

	for i := range s.zones {
		zone := &s.zones[i]
		entry := &s.recordsets[i]
		g.Go(func() error {
			allPages, err := recordsets.ListByZone(e.ClientV2, zone.ID, recordsets.ListOpts{}).AllPages(gCtx)
			if err != nil {
				return err
			}
			rsets, err := recordsets.ExtractRecordSets(allPages)
			if err != nil {
				return err
			}
			*entry = designateZoneRecordsets{zone: zone, recordsets: rsets, ok: true}
			return nil
		})
	}
	return g.Wait()
}

func (e *DesignateExporter) emitZones(_ context.Context, s *designateScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.Zones, float64(len(s.zones)))
	for i := range s.zones {
		zone := &s.zones[i]
		emitGauge(ch, e.descs.ZoneStatus, float64(mapZoneStatus(zone.Status)),
			zone.ID, zone.Name, zone.Status, zone.ProjectID, zone.Type)
	}
	return nil
}

func (e *DesignateExporter) emitRecordsets(_ context.Context, s *designateScrape, ch chan<- prometheus.Metric) error {
	for i := range s.recordsets {
		entry := &s.recordsets[i]
		if !entry.ok {
			continue
		}
		emitGauge(ch, e.descs.Recordsets, float64(len(entry.recordsets)), entry.zone.ID, entry.zone.Name, entry.zone.ProjectID)
		for j := range entry.recordsets {
			rs := &entry.recordsets[j]
			emitGauge(ch, e.descs.RecordsetsStatus, float64(mapRecordsetStatus(rs.Status)),
				rs.ID, rs.Name, rs.Status, rs.ZoneID, rs.ZoneName, rs.Type)
		}
	}
	return nil
}
