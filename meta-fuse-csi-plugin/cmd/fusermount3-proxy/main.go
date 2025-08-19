/*
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

package main

import (
	"fmt"
	"os"
	"strconv"
	"syscall"

	starter "github.com/StatCan/goofys/meta-fuse-csi-plugin/pkg/fuse_starter"
	"github.com/StatCan/goofys/meta-fuse-csi-plugin/pkg/util"
	flag "github.com/spf13/pflag"

	"k8s.io/klog/v2"
)

var (
	optUnmount     = flag.BoolP("unmount", "u", false, "unmount (NOT SUPPORTED)")
	optAutoUnmount = flag.BoolP("auto-unmount", "U", false, "auto-unmount (NOT SUPPORTED)")
	optLazy        = flag.BoolP("lazy", "z", false, "lazy umount (NOT SUPPORTED)")
	optQuiet       = flag.BoolP("quiet", "q", false, "quiet (NOT SUPPORTED)")
	optHelp        = flag.BoolP("help", "h", false, "print help")
	optVersion     = flag.BoolP("version", "V", false, "print version")
	optOptions     = flag.StringP("options", "o", "", "mount options")
	// This is set at compile time.
	version   = "unknown"
	builddate = "unknown"
)

var ignoredOptions = map[string]*bool{
	"unmount":      optUnmount,
	"auto-unmount": optAutoUnmount,
	"lazy":         optLazy,
	"optQuiet":     optQuiet,
}

const (
	ENV_FUSE_COMMFD                         = "_FUSE_COMMFD"
	ENV_FUSERMOUNT3PROXY_FDPASSING_SOCKPATH = "FUSERMOUNT3PROXY_FDPASSING_SOCKPATH"
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	if *optHelp {
		flag.PrintDefaults()
		os.Exit(0)
	}

	if *optVersion {
		fmt.Printf("fusermount3-dummy version %v (BuildDate %v)\n", version, builddate)
		os.Exit(0)
	}

	klog.Infof("Running meta-fuse-csi-plugin fusermount3-dummy version %v (BuildDate %v)", version, builddate)

	if *optUnmount {
		klog.Warning("'unmount' is not supported.")
		os.Exit(0)
	}

	if len(flag.Args()) == 0 {
		klog.Error("mountpoint is not specified.")
		os.Exit(1)
	}

	if *optOptions == "" {
		klog.Error("options is not specified.")
		os.Exit(1)
	}

	// fd-passing socket between fusermount3-dummy and csi-driver is passed as env var
	fdPassingSocketPath := os.Getenv(ENV_FUSERMOUNT3PROXY_FDPASSING_SOCKPATH)
	if fdPassingSocketPath == "" {
		klog.Errorf("environment variable %q is not specified.", ENV_FUSERMOUNT3PROXY_FDPASSING_SOCKPATH)
		os.Exit(1)
	}
	klog.Infof("fd-passing socket path is %q", fdPassingSocketPath)

	mntPoint := flag.Args()[0]
	klog.Infof("mountpoint is %q, but ignored.", mntPoint)

	for k, v := range ignoredOptions {
		if *v {
			klog.Warningf("opiton %q is true, but ignored.", k)
		}
	}

	// TODO: send options to csi-driver and use them?
	klog.Infof("options=%q", *optOptions)

	// get unix domain socket from caller
	commFdStr := os.Getenv(ENV_FUSE_COMMFD)
	commFd, err := strconv.Atoi(commFdStr)
	if err != nil {
		klog.Errorf("failed to get commFd _FUSE_COMMFD=%q", commFdStr)
		os.Exit(1)
	}
	klog.Infof("commFd from %q is %d", ENV_FUSE_COMMFD, commFd)

	commConn, err := util.GetNetConnFromRawUnixSocketFd(commFd)
	if err != nil {
		klog.Errorf("failed to convert commFd to net.Conn: %w", err)
		os.Exit(1)
	}
	klog.Infof("net.Conn is acquired from fd %d", commFd)

	// get fd for /dev/fuse from csi-driver
	mc, err := starter.PrepareMountConfig(fdPassingSocketPath)
	if err != nil {
		klog.Errorf("failed to prepare mount config: socket path %q: %w", fdPassingSocketPath, err)
		os.Exit(1)
	}
	defer syscall.Close(mc.FileDescriptor)
	klog.Infof("received fd for /dev/fuse from csi-driver via socket %q", fdPassingSocketPath)

	// now already FUSE-fs mounted and fd is ready.
	err = util.SendMsg(commConn, mc.FileDescriptor, []byte{0})
	if err != nil {
		klog.Errorf("failed to send fd via commFd: %w", err)
		os.Exit(1)
	}
	klog.Infof("sent fd for /dev/fuse via commFd %d", commFd)
	klog.Info("exiting fusermount3-dummy...")
}
