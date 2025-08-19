/*
Copyright 2018 The Kubernetes Authors.
Copyright 2022 Google LLC
Copyright 2023 Preferred Networks, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"fmt"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
)

const DefaultName = "meta-fuse-csi-plugin.csi.storage.pfn.io"

type DriverConfig struct {
	Name    string // Driver name
	Version string // Driver version
	NodeID  string // Node name
	Mounter mount.Interface
}

type Driver struct {
	config *DriverConfig

	// CSI RPC servers
	ids csi.IdentityServer
	ns  csi.NodeServer

	// Plugin capabilities
	vcap  map[csi.VolumeCapability_AccessMode_Mode]*csi.VolumeCapability_AccessMode
	nscap []*csi.NodeServiceCapability
}

func NewDriver(config *DriverConfig) (*Driver, error) {
	if config.Name == "" {
		return nil, fmt.Errorf("driver name missing")
	}
	if config.Version == "" {
		return nil, fmt.Errorf("driver version missing")
	}

	driver := &Driver{
		config: config,
		vcap:   map[csi.VolumeCapability_AccessMode_Mode]*csi.VolumeCapability_AccessMode{},
	}

	// TODO: is this ok?
	vcam := []csi.VolumeCapability_AccessMode_Mode{
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
		csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
		csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER,
		csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
	}
	driver.addVolumeCapabilityAccessModes(vcam)

	driver.ids = newIdentityServer(driver)
	nscap := []csi.NodeServiceCapability_RPC_Type{}
	driver.ns = newNodeServer(driver, config.Mounter)
	driver.addNodeServiceCapabilities(nscap)

	return driver, nil
}

func (driver *Driver) addVolumeCapabilityAccessModes(vc []csi.VolumeCapability_AccessMode_Mode) {
	for _, c := range vc {
		klog.Infof("Enabling volume access mode: %v", c.String())
		mode := NewVolumeCapabilityAccessMode(c)
		driver.vcap[mode.Mode] = mode
	}
}

func (driver *Driver) validateVolumeCapabilities(caps []*csi.VolumeCapability) error {
	if len(caps) == 0 {
		return fmt.Errorf("volume capabilities must be provided")
	}

	for _, c := range caps {
		if err := driver.validateVolumeCapability(c); err != nil {
			return err
		}
	}

	return nil
}

func (driver *Driver) validateVolumeCapability(c *csi.VolumeCapability) error {
	if c == nil {
		return fmt.Errorf("volume capability must be provided")
	}

	// Validate access mode
	accessMode := c.GetAccessMode()
	if accessMode == nil {
		return fmt.Errorf("volume capability access mode not set")
	}
	if driver.vcap[accessMode.Mode] == nil {
		return fmt.Errorf("driver does not support access mode: %v", accessMode.Mode.String())
	}

	// Validate access type
	accessType := c.GetAccessType()
	if accessType == nil {
		return fmt.Errorf("volume capability access type not set")
	}
	mountType := c.GetMount()
	if mountType == nil {
		return fmt.Errorf("driver only supports mount access type volume capability")
	}

	return nil
}

func (driver *Driver) addNodeServiceCapabilities(nl []csi.NodeServiceCapability_RPC_Type) {
	nsc := []*csi.NodeServiceCapability{}
	for _, n := range nl {
		klog.Infof("Enabling node service capability: %v", n.String())
		nsc = append(nsc, NewNodeServiceCapability(n))
	}
	driver.nscap = nsc
}

func (driver *Driver) Run(endpoint string) {
	klog.Infof("Running driver: %v", driver.config.Name)

	s := NewNonBlockingGRPCServer()
	s.Start(endpoint, driver.ids, nil, driver.ns)
	s.Wait()
}
