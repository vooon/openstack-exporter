package exporters

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"strings"
	"testing"

	gophercloudv2 "github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/baremetal/v1/nodes"
	"github.com/jarcoal/httpmock"
	"github.com/openstack-exporter/openstack-exporter/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type IronicTestSuite struct {
	BaseOpenStackTestSuite
}

var ironicExpectedUp = `
# HELP openstack_ironic_node node
# TYPE openstack_ironic_node gauge
openstack_ironic_node{conductor_group="",console_enabled="false",deploy_kernel="7ff5ef56-daaa-4256-9dd8-c3f1f9964ebc",deploy_ramdisk="e9c96d45-a4c8-4165-8753-9d8f32779e99",id="f50dcc35-4913-4667-a9fa-d130659c5661",instance_uuid="",last_error="",lessee="",maintenance="false",maintenance_reason="",name="r1-02",power_state="power off",provision_state="available",resource_class="baremetal",retired="true",retired_reason="No longer needed",serial_number="",traits=""} 1
openstack_ironic_node{conductor_group="",console_enabled="true",deploy_kernel="7ff5ef56-daaa-4256-9dd8-c3f1f9964ebc",deploy_ramdisk="e9c96d45-a4c8-4165-8753-9d8f32779e99",id="0129d2fc-0e5c-4b5b-a73b-01844d913957",instance_uuid="c0034f14-7937-41d5-b0f1-28d0d4e96426",last_error="",lessee="",maintenance="false",maintenance_reason="",name="r1-04",power_state="power on",provision_state="active",resource_class="baremetal",retired="true",retired_reason="No longer needed",serial_number="",traits=""} 1
openstack_ironic_node{conductor_group="rack-a",console_enabled="true",deploy_kernel="7ff5ef56-daaa-4256-9dd8-c3f1f9964ebc",deploy_ramdisk="e9c96d45-a4c8-4165-8753-9d8f32779e99",id="c9f98cc9-25e9-424e-8a89-002989054ec2",instance_uuid="",last_error="",lessee="",maintenance="true",maintenance_reason="Firmware upgrade",name="r1-05",power_state="power off",provision_state="available",resource_class="baremetal",retired="true",retired_reason="No longer needed",serial_number="",traits="CUSTOM_GPU HW_CPU_X86_VMX"} 1
openstack_ironic_node{conductor_group="",console_enabled="true",deploy_kernel="7ff5ef56-daaa-4256-9dd8-c3f1f9964ebc",deploy_ramdisk="e9c96d45-a4c8-4165-8753-9d8f32779e99",id="d381bea3-8768-4f12-a9b3-abf750ba918f",instance_uuid="31b5f585-104b-497a-bb72-b5376aaa089f",last_error="Provisioning failed Reached timeout",lessee="project-1234",maintenance="false",maintenance_reason="",name="r1-03",power_state="power on",provision_state="active",resource_class="baremetal",retired="true",retired_reason="No longer needed",serial_number="SN-1234567890",traits=""} 1
openstack_ironic_node{conductor_group="",console_enabled="true",deploy_kernel="7ff5ef56-daaa-4256-9dd8-c3f1f9964ebc",deploy_ramdisk="e9c96d45-a4c8-4165-8753-9d8f32779e99",id="d5641882-f7e5-4b92-9423-7e8157586218",instance_uuid="",last_error="",lessee="",maintenance="true",maintenance_reason="",name="r1-01",power_state="power off",provision_state="error",resource_class="baremetal",retired="true",retired_reason="No longer needed",serial_number="",traits=""} 1
# HELP openstack_ironic_node_provision_updated_at node_provision_updated_at
# TYPE openstack_ironic_node_provision_updated_at gauge
openstack_ironic_node_provision_updated_at{id="0129d2fc-0e5c-4b5b-a73b-01844d913957",name="r1-04",provision_state="active"} 1.593544011e+09
openstack_ironic_node_provision_updated_at{id="c9f98cc9-25e9-424e-8a89-002989054ec2",name="r1-05",provision_state="available"} 1.562908443e+09
openstack_ironic_node_provision_updated_at{id="d381bea3-8768-4f12-a9b3-abf750ba918f",name="r1-03",provision_state="active"} 1.593747281e+09
openstack_ironic_node_provision_updated_at{id="d5641882-f7e5-4b92-9423-7e8157586218",name="r1-01",provision_state="error"} 1.594708597e+09
openstack_ironic_node_provision_updated_at{id="f50dcc35-4913-4667-a9fa-d130659c5661",name="r1-02",provision_state="available"} 1.594740492e+09
# HELP openstack_ironic_node_updated_at node_updated_at
# TYPE openstack_ironic_node_updated_at gauge
openstack_ironic_node_updated_at{id="0129d2fc-0e5c-4b5b-a73b-01844d913957",name="r1-04",provision_state="active"} 1.593544011e+09
openstack_ironic_node_updated_at{id="c9f98cc9-25e9-424e-8a89-002989054ec2",name="r1-05",provision_state="available"} 1.592845911e+09
openstack_ironic_node_updated_at{id="d381bea3-8768-4f12-a9b3-abf750ba918f",name="r1-03",provision_state="active"} 1.594162438e+09
openstack_ironic_node_updated_at{id="d5641882-f7e5-4b92-9423-7e8157586218",name="r1-01",provision_state="error"} 1.594708598e+09
openstack_ironic_node_updated_at{id="f50dcc35-4913-4667-a9fa-d130659c5661",name="r1-02",provision_state="available"} 1.594740494e+09
# HELP openstack_ironic_up up
# TYPE openstack_ironic_up gauge
openstack_ironic_up 1
`

func (suite *IronicTestSuite) TestIronicExporter() {
	err := testutil.CollectAndCompare(*suite.Exporter, strings.NewReader(ironicExpectedUp), metricNamesFrom(ironicExpectedUp)...)
	assert.NoError(suite.T(), err)
}

// TestListAllNodesFollowsMarker covers an API whose configured max_limit is
// lower than the requested page size and which omits nodes_links. Collection
// must continue through short pages until the API returns an empty page.
func TestListAllNodesFollowsMarker(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	const (
		nodesURL      = "http://ironic.test/v1/nodes/detail"
		serverMaxSize = 3
	)

	nodePage := func(first, count int) httpmock.Responder {
		entries := make([]string, count)
		for i := range entries {
			entries[i] = fmt.Sprintf(`{"uuid": "node-%04d"}`, first+i)
		}
		resp := httpmock.NewStringResponse(http.StatusOK, `{"nodes": [`+strings.Join(entries, ",")+`]}`)
		resp.Header.Set("Content-Type", "application/json")
		return httpmock.ResponderFromResponse(resp)
	}

	httpmock.RegisterResponder(http.MethodGet,
		nodesURL+"?limit=1000&sort_dir=asc&sort_key=id",
		nodePage(0, serverMaxSize))
	httpmock.RegisterResponder(http.MethodGet,
		nodesURL+"?limit=1000&marker=node-0002&sort_dir=asc&sort_key=id",
		nodePage(serverMaxSize, serverMaxSize))
	httpmock.RegisterResponder(http.MethodGet,
		nodesURL+"?limit=1000&marker=node-0005&sort_dir=asc&sort_key=id",
		nodePage(0, 0))
	httpmock.RegisterNoResponder(httpmock.NewNotFoundResponder(t.Fatal))

	client := &gophercloudv2.ServiceClient{
		ProviderClient: &gophercloudv2.ProviderClient{},
		Endpoint:       "http://ironic.test/v1/",
	}

	allNodes, err := listAllNodes(context.Background(), client)
	require.NoError(t, err)
	require.Len(t, allNodes, 2*serverMaxSize)
	assert.Equal(t, "node-0000", allNodes[0].UUID)
	assert.Equal(t, "node-0005", allNodes[len(allNodes)-1].UUID)
}

func ironicTestMapping(t *testing.T, mapping string) *utils.LabelMappingFlag {
	t.Helper()
	m := &utils.LabelMappingFlag{DeriveLabelFromLeaf: true}
	if err := m.Set(mapping); err != nil {
		t.Fatalf("Set(%q) error = %v", mapping, err)
	}
	return m
}

// The default mapping must reproduce exactly the labels the node metric
// carried when deploy_kernel and deploy_ramdisk were hardcoded.
func TestIronicDefaultNodeExtraLabels(t *testing.T) {
	m := NewIronicNodeExtraLabels()
	if want := []string{"deploy_kernel", "deploy_ramdisk"}; !reflect.DeepEqual(m.Labels, want) {
		t.Fatalf("default labels = %#v, want %#v", m.Labels, want)
	}
}

func TestIronicNodeExtraLabelValues(t *testing.T) {
	node := &nodes.Node{
		DriverInfo:   map[string]any{"deploy_kernel": "kernel-uuid", "ipmi_address": "10.10.0.101", "ipmi_password": "hunter2"},
		InstanceInfo: map[string]any{"image_source": "image-uuid", "memory_mb": float64(32768), "image_properties": map[string]any{"os_distro": "ubuntu"}},
		Extra:        map[string]any{"root_password": "hash"},
		Properties:   map[string]any{"cpu_arch": "x86_64"},
	}

	tests := []struct {
		name    string
		mapping string
		want    []string
	}{
		{"driver_info", "driver_info.deploy_kernel", []string{"kernel-uuid"}},
		{"across dictionaries", "driver_info.ipmi_address,instance_info.image_source,properties.cpu_arch",
			[]string{"10.10.0.101", "image-uuid", "x86_64"}},
		{"numeric value", "instance_info.memory_mb", []string{"32768"}},
		{"nested object is not rendered", "instance_info.image_properties", []string{""}},
		{"missing key", "driver_info.does_not_exist", []string{""}},
		{"renamed label", "bmc=driver_info.ipmi_address", []string{"10.10.0.101"}},
		{"credentials are redacted", "driver_info.ipmi_password,extra.root_password",
			[]string{utils.RedactedLabelValue, utils.RedactedLabelValue}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapping := ironicTestMapping(t, tt.mapping)
			if got := mapping.ExtractNestedAny(ironicNodeNestedMaps(node)...); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ExtractNestedAny() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

// An empty flag value must drop the labels entirely, which is how an operator
// opts out of the defaults.
func TestIronicNodeExtraLabelsCanBeCleared(t *testing.T) {
	mapping := ironicTestMapping(t, "")
	if got := mapping.ExtractNestedAny(ironicNodeNestedMaps(&nodes.Node{DriverInfo: map[string]any{"deploy_kernel": "k"}})...); len(got) != 0 {
		t.Fatalf("ExtractNestedAny() = %#v, want empty", got)
	}
}

func TestValidateNodeExtraLabels(t *testing.T) {
	for _, tt := range []struct {
		mapping string
		wantErr bool
	}{
		{"driver_info.deploy_kernel", false},
		{"instance_info.image_source", false},
		{"extra.foo", false},
		{"properties.cpu_arch", false},
		{"driver_internal_info.agent_version", false},
		{"lbl=bogus_dict.key", true},
		{"unqualified", true},
	} {
		m := ironicTestMapping(t, tt.mapping)
		err := m.ValidateNestedKeys(ironicNodeInfoMaps...)
		if (err != nil) != tt.wantErr {
			t.Errorf("ValidateNestedKeys(%q) error = %v, wantErr %v", tt.mapping, err, tt.wantErr)
		}
	}
}

// ironicExporterWithMapping builds an Ironic exporter against the suite's
// fixtures with the given node label mapping. The fixtures are re-registered
// first because each responder answers a bounded number of times and the
// suite's own exporter already consumed the discovery and token requests.
func (suite *IronicTestSuite) ironicExporterWithMapping(mapping string) (OpenStackExporter, error) {
	suite.teardownFixtures()
	suite.installFixtures()
	opts := ExporterOptions{
		Cloud:                 cloudName,
		Prefix:                suite.Prefix,
		EndpointType:          "public",
		NovaMetadataMapping:   utils.NewLabelMappingFlag(),
		IronicNodeExtraLabels: ironicTestMapping(suite.T(), mapping),
	}
	return NewExporter(suite.ServiceName, opts, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func (suite *IronicTestSuite) TestIronicExtraLabelsEndToEnd() {
	exporter, err := suite.ironicExporterWithMapping("bmc=driver_info.ipmi_address,driver_info.ipmi_password")
	require.NoError(suite.T(), err)

	registry := prometheus.NewPedanticRegistry()
	require.NoError(suite.T(), registry.Register(exporter))
	families, err := registry.Gather()
	require.NoError(suite.T(), err)

	for _, mf := range families {
		if mf.GetName() != "openstack_ironic_node" {
			continue
		}
		require.NotEmpty(suite.T(), mf.GetMetric())
		labels := map[string]string{}
		for _, lp := range mf.GetMetric()[0].GetLabel() {
			labels[lp.GetName()] = lp.GetValue()
		}
		assert.Equal(suite.T(), "10.10.0.101", labels["bmc"])
		assert.Equal(suite.T(), utils.RedactedLabelValue, labels["ipmi_password"], "credential must be redacted")
		assert.NotContains(suite.T(), labels, "deploy_kernel", "defaults are replaced, not appended")
		return
	}
	suite.T().Fatal("openstack_ironic_node family not gathered")
}

func (suite *IronicTestSuite) TestIronicExtraLabelsRejectCollisions() {
	_, err := suite.ironicExporterWithMapping("name=driver_info.ipmi_address")
	require.Error(suite.T(), err)
	assert.Contains(suite.T(), err.Error(), "name is already a label")

	_, err = suite.ironicExporterWithMapping("bogus.key")
	require.Error(suite.T(), err)
	assert.Contains(suite.T(), err.Error(), "unknown map")
}
