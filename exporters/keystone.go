package exporters

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/domains"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/groups"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/projects"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/regions"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/users"
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	RegisterTypedExporter("identity", NewKeystoneExporter)
}

type KeystoneExporter struct {
	BaseOpenStackExporter
	descs keystoneDescs
	sched Schedule[keystoneScrape]
}

type keystoneDescs struct {
	Domains     *prometheus.Desc `metric:"domains"`
	DomainInfo  *prometheus.Desc `metric:"domain_info"  labels:"description,enabled,id,name"`
	Users       *prometheus.Desc `metric:"users"`
	Groups      *prometheus.Desc `metric:"groups"`
	Projects    *prometheus.Desc `metric:"projects"`
	ProjectInfo *prometheus.Desc `metric:"project_info" labels:"is_domain,description,domain_id,enabled,id,name,parent_id,tags"`
	Regions     *prometheus.Desc `metric:"regions"`
}

type keystoneScrape struct {
	domains  []domains.Domain
	users    []users.User
	groups   []groups.Group
	projects []projects.Project
	regions  []regions.Region
}

func NewKeystoneExporter(config *ExporterConfig, logger *slog.Logger) (*KeystoneExporter, error) {
	e := &KeystoneExporter{
		BaseOpenStackExporter: BaseOpenStackExporter{
			Name:           "identity",
			ExporterConfig: *config,
			logger:         logger,
		},
	}
	graph := Graph[keystoneScrape]{
		Sources: []Source[keystoneScrape]{
			{Name: "domains", Fetch: e.fetchDomains},
			{Name: "users", Fetch: e.fetchUsers},
			{Name: "groups", Fetch: e.fetchGroups},
			{Name: "projects", Fetch: e.fetchProjects},
			{Name: "regions", Fetch: e.fetchRegions},
		},
		Emitters: []Emitter[keystoneScrape]{
			{Name: "domains", Metrics: []string{"domains", "domain_info"}, Sources: []string{"domains"}, Emit: e.emitDomains},
			{Name: "users", Metrics: []string{"users"}, Sources: []string{"users"}, Emit: e.emitUsers},
			{Name: "groups", Metrics: []string{"groups"}, Sources: []string{"groups"}, Emit: e.emitGroups},
			{Name: "projects", Metrics: []string{"projects", "project_info"}, Sources: []string{"projects"}, Emit: e.emitProjects},
			{Name: "regions", Metrics: []string{"regions"}, Sources: []string{"regions"}, Emit: e.emitRegions},
		},
	}
	sched, err := SetupExporter(&e.BaseOpenStackExporter, &e.descs, &graph)
	if err != nil {
		return nil, err
	}
	e.sched = sched
	return e, nil
}

func (e *KeystoneExporter) Collect(ch chan<- prometheus.Metric) {
	CollectSchedule(&e.BaseOpenStackExporter, &e.sched, new(keystoneScrape), ch)
}

func (e *KeystoneExporter) fetchDomains(ctx context.Context, s *keystoneScrape) error {
	allPages, err := domains.List(e.ClientV2, domains.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.domains, err = domains.ExtractDomains(allPages)
	return err
}

func (e *KeystoneExporter) fetchUsers(ctx context.Context, s *keystoneScrape) error {
	allPages, err := users.List(e.ClientV2, users.ListOpts{DomainID: e.DomainID}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.users, err = users.ExtractUsers(allPages)
	return err
}

func (e *KeystoneExporter) fetchGroups(ctx context.Context, s *keystoneScrape) error {
	allPages, err := groups.List(e.ClientV2, groups.ListOpts{DomainID: e.DomainID}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.groups, err = groups.ExtractGroups(allPages)
	return err
}

func (e *KeystoneExporter) fetchProjects(ctx context.Context, s *keystoneScrape) error {
	var err error
	s.projects, err = GetProjects(ctx, &e.BaseOpenStackExporter)
	return err
}

func (e *KeystoneExporter) fetchRegions(ctx context.Context, s *keystoneScrape) error {
	allPages, err := regions.List(e.ClientV2, regions.ListOpts{}).AllPages(ctx)
	if err != nil {
		return err
	}
	s.regions, err = regions.ExtractRegions(allPages)
	return err
}

func (e *KeystoneExporter) emitDomains(_ context.Context, s *keystoneScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.Domains, float64(len(s.domains)))
	for i := range s.domains {
		d := &s.domains[i]
		emitGauge(ch, e.descs.DomainInfo, 1.0, d.Description, strconv.FormatBool(d.Enabled), d.ID, d.Name)
	}
	return nil
}

func (e *KeystoneExporter) emitUsers(_ context.Context, s *keystoneScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.Users, float64(len(s.users)))
	return nil
}

func (e *KeystoneExporter) emitGroups(_ context.Context, s *keystoneScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.Groups, float64(len(s.groups)))
	return nil
}

func (e *KeystoneExporter) emitProjects(_ context.Context, s *keystoneScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.Projects, float64(len(s.projects)))
	for i := range s.projects {
		p := &s.projects[i]
		emitGauge(ch, e.descs.ProjectInfo, 1.0, strconv.FormatBool(p.IsDomain),
			p.Description, p.DomainID, strconv.FormatBool(p.Enabled), p.ID, p.Name,
			p.ParentID, strings.Join(p.Tags, ","))
	}
	return nil
}

func (e *KeystoneExporter) emitRegions(_ context.Context, s *keystoneScrape, ch chan<- prometheus.Metric) error {
	emitGauge(ch, e.descs.Regions, float64(len(s.regions)))
	return nil
}

func newIdentityV3ClientV2FromExporter(exporter *BaseOpenStackExporter, fallbackServiceName string) (*gophercloud.ServiceClient, error) {
	var eo gophercloud.EndpointOpts

	// We need a list of all tenants/projects. Therefore, within this nova exporter we need
	// to create an openstack client for the Identity/Keystone API.
	// If possible, use the EndpointOpts spefic to the identity service.
	if v, ok := endpointOptsV2["identity"]; ok {
		eo = v
	} else if v, ok := endpointOptsV2[fallbackServiceName]; ok {
		eo = v
	} else {
		return nil, errors.New("no EndpointOpts available to create Identity client")
	}

	cli, err := openstack.NewIdentityV3(exporter.ClientV2.ProviderClient, eo)
	if err != nil {
		return nil, err
	}

	return cli, nil
}
