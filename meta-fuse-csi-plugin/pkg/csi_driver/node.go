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
	"os"
	"path/filepath"
	"strings"
	"time"

	csimounter "github.com/StatCan/goofys/meta-fuse-csi-plugin/pkg/csi_mounter"
	"github.com/StatCan/goofys/meta-fuse-csi-plugin/pkg/util"
	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	mount "k8s.io/mount-utils"
)

// NodePublishVolume VolumeContext parameters.
const (
	VolumeContextKeyServiceAccountName = "csi.storage.k8s.io/serviceAccount.name"
	//nolint:gosec
	VolumeContextKeyServiceAccountToken   = "csi.storage.k8s.io/serviceAccount.tokens"
	VolumeContextKeyPodName               = "csi.storage.k8s.io/pod.name"
	VolumeContextKeyPodNamespace          = "csi.storage.k8s.io/pod.namespace"
	VolumeContextKeyEphemeral             = "csi.storage.k8s.io/ephemeral"
	VolumeContextKeyMountOptions          = "mountOptions"
	VolumeContextKeyFdPassingEmptyDirName = "fdPassingEmptyDirName"
	VolumeContextKeyFdPassingSocketName   = "fdPassingSocketName"

	UmountTimeout = time.Second * 5
)

// nodeServer handles mounting and unmounting of GCS FUSE volumes on a node.
type nodeServer struct {
	driver      *Driver
	mounter     mount.Interface
	volumeLocks *util.VolumeLocks
}

func newNodeServer(driver *Driver, mounter mount.Interface) csi.NodeServer {
	return &nodeServer{
		driver:      driver,
		mounter:     mounter,
		volumeLocks: util.NewVolumeLocks(),
	}
}

func (s *nodeServer) NodeGetInfo(_ context.Context, _ *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: s.driver.config.NodeID,
	}, nil
}

func (s *nodeServer) NodeGetCapabilities(_ context.Context, _ *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: s.driver.nscap,
	}, nil
}

func (s *nodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	vc := req.GetVolumeContext()

	fuseMountOptions := []string{}
	if req.GetReadonly() {
		fuseMountOptions = joinMountOptions(fuseMountOptions, []string{"ro"})
	} else {
		fuseMountOptions = joinMountOptions(fuseMountOptions, []string{"rw"})
	}
	if capMount := req.GetVolumeCapability().GetMount(); capMount != nil {
		fuseMountOptions = joinMountOptions(fuseMountOptions, capMount.GetMountFlags())
	}
	if mountOptions, ok := vc[VolumeContextKeyMountOptions]; ok {
		fuseMountOptions = joinMountOptions(fuseMountOptions, strings.Split(mountOptions, ","))
	}

	if vc[VolumeContextKeyEphemeral] != "true" {
		return nil, status.Errorf(codes.InvalidArgument, "NodePublishVolume VolumeContext %q must be provided for ephemeral storage", VolumeContextKeyEphemeral)
	}

	targetPath := req.GetTargetPath()
	if len(targetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "NodePublishVolume target path must be provided")
	}

	if err := s.driver.validateVolumeCapabilities([]*csi.VolumeCapability{req.GetVolumeCapability()}); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Acquire a lock on the target path instead of volumeID, since we do not want to serialize multiple node publish calls on the same volume.
	if acquired := s.volumeLocks.TryAcquire(targetPath); !acquired {
		return nil, status.Errorf(codes.Aborted, util.VolumeOperationAlreadyExistsFmt, targetPath)
	}
	defer s.volumeLocks.Release(targetPath)

	// Parse targetPath to get volumeName
	podId, volumeName, err := util.ParsePodIDVolumeFromTargetpath(targetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to parse targetPath %q", targetPath)
	}

	fdPassingEmptyDirName, ok := vc[VolumeContextKeyFdPassingEmptyDirName]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "NodePublishVolume VolumeContext %q must be provided", VolumeContextKeyFdPassingEmptyDirName)
	}

	fdPassingSocketName, ok := vc[VolumeContextKeyFdPassingSocketName]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "NodePublishVolume VolumeContext %q must be provided", VolumeContextKeyFdPassingSocketName)
	}

	// Check if the target path is already mounted
	mounted, err := s.isDirMounted(targetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check if path %q is already mounted: %v", targetPath, err)
	}

	if mounted {
		// Already mounted
		klog.V(4).Infof("NodePublishVolume succeeded on volume %q to target path %q, mount already exists.", volumeName, targetPath)

		return &csi.NodePublishVolumeResponse{}, nil
	}

	emptyDir := util.GetEmptyDirPath(podId, fdPassingEmptyDirName)
	if _, err := os.Stat(emptyDir); err != nil {
		return nil, status.Errorf(codes.Internal, "directory %q for emptyDir %q does not exist", emptyDir, fdPassingEmptyDirName)
	}

	sockPath := filepath.Join(emptyDir, fdPassingSocketName)
	if _, err := os.Stat(sockPath); err == nil {
		// Unix domain socket already waits for connection
		klog.V(4).Infof("NodePublishVolume succeeded on volume %q to target path %q, unix domain socket already exists.", volumeName, targetPath)

		return &csi.NodePublishVolumeResponse{}, nil
	}

	klog.V(4).Infof("NodePublishVolume attempting mkdir for path %q", targetPath)
	if err := os.MkdirAll(targetPath, 0o750); err != nil {
		return nil, status.Errorf(codes.Internal, "mkdir failed for path %q: %v", targetPath, err)
	}

	// fuseMountOptions[0] is fdPassingSockPath
	fuseMountOptions = append([]string{sockPath}, fuseMountOptions...)

	// Start to mount
	if err = s.mounter.Mount(volumeName, targetPath, "fuse", fuseMountOptions); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to mount volume %q to target path %q: %v", volumeName, targetPath, err)
	}

	klog.V(4).Infof("NodePublishVolume succeeded on volume %q to target path %q", volumeName, targetPath)

	return &csi.NodePublishVolumeResponse{}, nil
}

func (s *nodeServer) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	// Validate arguments
	targetPath := req.GetTargetPath()
	if len(targetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "NodeUnpublishVolume target path must be provided")
	}

	// Acquire a lock on the target path instead of volumeID, since we do not want to serialize multiple node unpublish calls on the same volume.
	if acquired := s.volumeLocks.TryAcquire(targetPath); !acquired {
		return nil, status.Errorf(codes.Aborted, util.VolumeOperationAlreadyExistsFmt, targetPath)
	}
	defer s.volumeLocks.Release(targetPath)

	// Check if the target path is already mounted
	if mounted, err := s.isDirMounted(targetPath); mounted || err != nil {
		if err != nil {
			klog.Errorf("failed to check if path %q is already mounted: %v", targetPath, err)
		}
		// Force unmount the target path
		// Try to do force unmount firstly because if the file descriptor was not closed,
		// mount.CleanupMountPoint() call will hang.
		forceUnmounter, ok := s.mounter.(mount.MounterForceUnmounter)
		if ok {
			if err = forceUnmounter.UnmountWithForce(targetPath, UmountTimeout); err != nil {
				return nil, status.Errorf(codes.Internal, "failed to force unmount target path %q: %v", targetPath, err)
			}
		} else {
			klog.Warningf("failed to cast the mounter to a forceUnmounter, proceed with the default mounter Unmount")
			if err = s.mounter.Unmount(targetPath); err != nil {
				return nil, status.Errorf(codes.Internal, "failed to unmount target path %q: %v", targetPath, err)
			}
		}
	}

	// Remove all files in the path
	isMounted, err := s.mounter.IsMountPoint(targetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check %q is mounted: %v", targetPath, err)
	}

	// If nothing is mounted and files are written, following mount.CleanupMountPoint will fail.
	// To avoid the failure, cleanup childs in mount path's directory.
	// CAUTION: This can cause data loss.
	// TODO: Not to remove childs.
	if !isMounted {
		if err = removeChilds(targetPath); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to remove childs in %q: %v", targetPath, err)
		}
	}

	// Cleanup the mount point
	if err := mount.CleanupMountPoint(targetPath, s.mounter, false /* bind mount */); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to cleanup the mount point %q: %v", targetPath, err)
	}

	// Checking the fd-passing socket is closed.
	// If not closed, close it and wait for the acception goroutine exits.
	// NOTE: The acception goroutine owns FUSE fd, and floated FUSE fd causes hang.
	//       When the acception goroutine exis, FUSE fd is also closed.
	csiMounter, ok := s.mounter.(*csimounter.Mounter)
	if !ok {
		klog.Error("failed to cast the mounter to a csimounter.Mounter.")
	} else if !csiMounter.FdPassingSockets.Exist(targetPath) {
		klog.V(4).Infof("fd-passing socket for %q is already unregistered.", targetPath)
	} else {
		klog.V(4).Infof("closing fd-passing socket for %q.", targetPath)
		if err := csiMounter.FdPassingSockets.CloseAndUnregister(targetPath, true); err != nil {
			klog.Warningf("fd-passing socket for %q is already unregistered.", targetPath)
		} else {
			csiMounter.FdPassingSockets.WaitForExit(targetPath)
			klog.V(4).Infof("fd-passing socket for %q is closed.", targetPath)
		}
	}

	klog.V(4).Infof("NodeUnpublishVolume succeeded on target path %q", targetPath)

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// isDirMounted checks if the path is already a mount point.
func (s *nodeServer) isDirMounted(targetPath string) (bool, error) {
	mps, err := s.mounter.List()
	if err != nil {
		return false, err
	}
	for _, m := range mps {
		if m.Path == targetPath {
			return true, nil
		}
	}

	return false, nil
}

// joinMountOptions joins mount options eliminating duplicates.
func joinMountOptions(userOptions []string, systemOptions []string) []string {
	allMountOptions := sets.NewString()

	for _, mountOption := range userOptions {
		if len(mountOption) > 0 {
			allMountOptions.Insert(mountOption)
		}
	}

	for _, mountOption := range systemOptions {
		allMountOptions.Insert(mountOption)
	}

	return allMountOptions.List()
}

// removeChilds remove all childs in the directory
func removeChilds(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}
