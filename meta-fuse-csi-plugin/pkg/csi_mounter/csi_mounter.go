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

package csimounter

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	starter "github.com/StatCan/goofys/meta-fuse-csi-plugin/pkg/fuse_starter"
	"github.com/StatCan/goofys/meta-fuse-csi-plugin/pkg/util"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
)

const (
	// See the nonroot user discussion: https://github.com/GoogleContainerTools/distroless/issues/443
	NobodyUID = 65534
	NobodyGID = 65534
)

// Mounter provides the meta-fuse-csi-plugin implementation of mount.Interface
// for the linux platform.
type Mounter struct {
	mount.MounterForceUnmounter
	chdirMu          sync.Mutex
	FdPassingSockets *FdPassingSockets
}

// New returns a mount.MounterForceUnmounter for the current system.
// It provides options to override the default mounter behavior.
// mounterPath allows using an alternative to `/bin/mount` for mounting.
func New(mounterPath string) (mount.Interface, error) {
	m, ok := mount.New(mounterPath).(mount.MounterForceUnmounter)
	if !ok {
		return nil, fmt.Errorf("failed to cast mounter to MounterForceUnmounter")
	}

	return &Mounter{
		m,
		sync.Mutex{},
		newFdPassingSockets(),
	}, nil
}

func (m *Mounter) Mount(source string, target string, fstype string, options []string) error {
	if len(options) == 1 {
		options = append(options, "")
	}

	fdPassingSocketPath := options[0]
	fdPassingSocketDir, fdPassingSocketName := filepath.Split(fdPassingSocketPath)
	klog.V(4).Infof("start to mount (fdPassingSocketDir=%s fdPassingSocketName=%s)", fdPassingSocketDir, fdPassingSocketName)

	options = options[1:]

	csiMountOptions, _ := prepareMountOptions(options[1:])

	klog.V(4).Info("passing the descriptor")

	err := m.createAndRegisterFdPassingSocket(target, fdPassingSocketDir, fdPassingSocketName)
	if err != nil {
		return fmt.Errorf("failed to create fd-passing socket: %w", err)
	}

	// Prepare sidecar mounter MountConfig
	mc := starter.MountConfig{
		VolumeName: source,
	}
	mcb, err := json.Marshal(mc)
	if err != nil {
		return fmt.Errorf("failed to marshal sidecar mounter MountConfig %v: %w", mc, err)
	}

	// Asynchronously waiting for the sidecar container to connect to the listener
	go func(mounter *Mounter, target, fstype string, csiMountOptions []string, msg []byte) {
		defer func() {
			err = mounter.FdPassingSockets.CloseAndUnregister(target, false)
			if err != nil {
				klog.Errorf("failed to close and unregister fd-passing socket for %q: %w", target, err)
			}
		}()

		podID, volumeName, _ := util.ParsePodIDVolumeFromTargetpath(target)
		logPrefix := fmt.Sprintf("[Pod %v, VolumeName %v]", podID, volumeName)

		klog.V(4).Infof("%v start to accept connections to the listener.", logPrefix)
		a, err := mounter.FdPassingSockets.accept(target)
		if err != nil {
			klog.Errorf("%v failed to accept connections to the listener: %v", logPrefix, err)
			return
		}
		defer a.Close()

		klog.V(4).Info("opening the device /dev/fuse")
		fuseFd, err := syscall.Open("/dev/fuse", syscall.O_RDWR, 0o644)
		if err != nil {
			klog.Errorf("failed to open the device /dev/fuse: %w", err)
			return
		}
		defer syscall.Close(fuseFd)
		csiMountOptions = append(csiMountOptions, fmt.Sprintf("fd=%v", fuseFd))

		// fuse-impl expects fuse is mounted.
		klog.V(4).Info("mounting the fuse filesystem")
		err = mounter.MountSensitiveWithoutSystemdWithMountFlags(volumeName, target, fstype, csiMountOptions, nil, []string{"--internal-only"})
		if err != nil {
			klog.Errorf("failed to mount the fuse filesystem: %w", err)
			return
		}

		klog.V(4).Infof("%v start to send file descriptor and mount options", logPrefix)
		if err = util.SendMsg(a, fuseFd, msg); err != nil {
			klog.Errorf("%v failed to send file descriptor and mount options: %v", logPrefix, err)
			return
		}

		klog.V(4).Infof("%v exiting the goroutine.", logPrefix)
	}(m, target, fstype, csiMountOptions, mcb)

	return nil
}

func (m *Mounter) createAndRegisterFdPassingSocket(target, sockDir, sockName string) error {
	m.chdirMu.Lock()
	defer m.chdirMu.Unlock()

	// Need to change the current working directory to the temp volume base path,
	// because the socket absolute path is longer than 104 characters,
	// which will cause "bind: invalid argument" errors.
	exPwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("failed to get the current directory to %w", err)
	}
	if err = os.Chdir(sockDir); err != nil {
		return fmt.Errorf("failed to change directory to %q: %w", sockDir, err)
	}

	klog.V(4).Infof("creating a listener for the socket at %q", sockDir)
	l, err := net.Listen("unix", sockName)
	if err != nil {
		return fmt.Errorf("failed to create the listener for the socket: %w", err)
	}

	unixListner := l.(*net.UnixListener)

	// Change the socket ownership
	err = os.Chown(sockDir, NobodyUID, NobodyGID)
	if err != nil {
		return fmt.Errorf("failed to change ownership on emptyDirBasePath: %w", err)
	}
	err = os.Chown(sockName, NobodyUID, NobodyGID)
	if err != nil {
		return fmt.Errorf("failed to change ownership on socket: %w", err)
	}

	if err = os.Chdir(exPwd); err != nil {
		return fmt.Errorf("failed to change directory to %q: %w", exPwd, err)
	}

	sockPath := filepath.Join(sockDir, sockName)
	if err = m.FdPassingSockets.register(target, sockPath, unixListner); err != nil {
		return fmt.Errorf("failed to register socket at %q: %w", sockPath, err)
	}

	return nil
}

func prepareMountOptions(options []string) ([]string, []string) {
	allowedOptions := map[string]bool{
		"exec":    true,
		"noexec":  true,
		"atime":   true,
		"noatime": true,
		"sync":    true,
		"async":   true,
		"dirsync": true,
	}

	csiMountOptions := []string{
		"nodev",
		"nosuid",
		"allow_other",
		"default_permissions",
		"rootmode=40000",
		fmt.Sprintf("user_id=%d", os.Getuid()),
		fmt.Sprintf("group_id=%d", os.Getgid()),
	}

	// users may pass options that should be used by Linux mount(8),
	// filter out these options and not pass to the sidecar mounter.
	validMountOptions := []string{"rw", "ro"}
	optionSet := sets.NewString(options...)
	for _, o := range validMountOptions {
		if optionSet.Has(o) {
			csiMountOptions = append(csiMountOptions, o)
			optionSet.Delete(o)
		}
	}

	for _, o := range optionSet.List() {
		if strings.HasPrefix(o, "o=") {
			v := o[2:]
			if allowedOptions[v] {
				csiMountOptions = append(csiMountOptions, v)
			} else {
				klog.Warningf("got invalid mount option %q. Will discard invalid options and continue to mount.", v)
			}
			optionSet.Delete(o)
		}
	}

	return csiMountOptions, optionSet.List()
}

type FdPassingSockets struct {
	// key is target path
	sockets      map[string]*FdPassingSocket
	socketsMutex sync.Mutex
}

type FdPassingSocket struct {
	socketPath string
	listener   *net.UnixListener
	exitChan   chan bool
	closed     bool
}

func newFdPassingSockets() *FdPassingSockets {
	return &FdPassingSockets{
		map[string]*FdPassingSocket{},
		sync.Mutex{},
	}
}

func (fds *FdPassingSockets) register(targetPath string, sockPath string, listener *net.UnixListener) error {
	fds.socketsMutex.Lock()
	defer fds.socketsMutex.Unlock()

	// if the socket is already registered, return error
	if _, ok := fds.sockets[targetPath]; ok {
		return fmt.Errorf("fd-passing socket for %q is already registered", sockPath)
	}

	fdSock := &FdPassingSocket{
		socketPath: sockPath,
		listener:   listener,
		exitChan:   make(chan bool, 5),
		closed:     false,
	}

	fds.sockets[targetPath] = fdSock

	return nil
}

func (fds *FdPassingSockets) get(targetPath string) *FdPassingSocket {
	fds.socketsMutex.Lock()
	defer fds.socketsMutex.Unlock()

	return fds.sockets[targetPath]
}

func (fds *FdPassingSockets) accept(targetPath string) (net.Conn, error) {
	sock := fds.get(targetPath)
	if sock == nil {
		return nil, fmt.Errorf("")
	}

	return sock.listener.Accept()
}

func (fds *FdPassingSockets) Exist(targetPath string) bool {
	sock := fds.get(targetPath)
	return sock != nil
}

func (fds *FdPassingSockets) WaitForExit(targetPath string) {
	sock := fds.get(targetPath)
	if sock == nil {
		return
	}

	// wait for exit signal
	<-sock.exitChan
}

func (fds *FdPassingSockets) CloseAndUnregister(targetPath string, onlyClose bool) error {
	fds.socketsMutex.Lock()
	defer fds.socketsMutex.Unlock()

	sock, ok := fds.sockets[targetPath]
	if !ok {
		// fd-passing socket is already unregistered
		return nil
	}

	if !sock.closed {
		sock.listener.Close()
		sock.closed = true
	}

	if onlyClose {
		return nil
	}

	// if unix domain socket exists, remove it.
	if _, err := os.Stat(sock.socketPath); err == nil {
		syscall.Unlink(sock.socketPath)
	}

	// notify that listener has been closed via channel
	sock.exitChan <- true

	// remove registered socket
	delete(fds.sockets, targetPath)

	return nil
}
