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

package fusestarter

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"syscall"

	"github.com/StatCan/goofys/meta-fuse-csi-plugin/pkg/util"
	"k8s.io/klog/v2"
)

// FuseStarter will be used in the sidecar container to invoke fuse impl.
type FuseStarter struct {
	mounterPath string
	mounterArgs []string
	Cmd         *exec.Cmd
}

// New returns a FuseStarter for the current system.
// It provides an option to specify the path to fuse binary.
func New(mounterPath string, mounterArgs []string) *FuseStarter {
	return &FuseStarter{
		mounterPath: mounterPath,
		mounterArgs: mounterArgs,
		Cmd:         nil,
	}
}

type MountConfig struct {
	FileDescriptor int    `json:"-"`
	VolumeName     string `json:"volumeName,omitempty"`
}

func (m *FuseStarter) Mount(mc *MountConfig) (*exec.Cmd, error) {
	klog.Infof("start to invoke fuse impl for volume %q", mc.VolumeName)

	klog.Infof("%s mounting with args %v...", m.mounterPath, m.mounterArgs)
	cmd := exec.Cmd{
		Path:       m.mounterPath,
		Args:       append([]string{m.mounterPath}, m.mounterArgs...),
		ExtraFiles: []*os.File{os.NewFile(uintptr(mc.FileDescriptor), "/dev/fuse")},
		Stdout:     os.Stdout,
		Stderr:     os.Stderr,
	}

	m.Cmd = &cmd

	return &cmd, nil
}

// Fetch the following information from a given socket path:
// 1. Pod volume name
// 2. The file descriptor
// 3. Mount options passing to mounter (passed by the csi mounter).
func PrepareMountConfig(sp string) (*MountConfig, error) {
	mc := MountConfig{}

	klog.Infof("connecting to socket %q", sp)
	c, err := net.Dial("unix", sp)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to the socket %q: %w", sp, err)
	}
	defer func() {
		// as we got all the information from the socket, closing the connection and deleting the socket
		c.Close()
		if err = syscall.Unlink(sp); err != nil {
			// csi driver may already removed the socket.
			klog.Warningf("failed to close socket %q: %v", sp, err)
		}
	}()

	fd, msg, err := util.RecvMsg(c)
	if err != nil {
		return nil, fmt.Errorf("failed to receive mount options from the socket %q: %w", sp, err)
	}

	mc.FileDescriptor = fd

	if err := json.Unmarshal(msg, &mc); err != nil {
		return nil, fmt.Errorf("failed to unmarshal the mount config: %w", err)
	}

	return &mc, nil
}
