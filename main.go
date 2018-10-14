// Copyright 2015 - 2017 Ka-Hing Cheung
// Copyright 2015 - 2017 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"strconv"

	goofys "github.com/kahing/goofys/api"
	. "github.com/kahing/goofys/internal"

	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/context"

	"github.com/jacobsa/fuse"
	"github.com/jinzhu/copier"
	"github.com/kardianos/osext"
	"github.com/urfave/cli"

	daemon "github.com/sevlyar/go-daemon"
)

var log = GetLogger("main")

func registerSIGINTHandler(fs *Goofys, flags *FlagStorage) {
	// Register for SIGINT.
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM, syscall.SIGUSR1)

	// Start a goroutine that will unmount when the signal is received.
	go func() {
		for {
			s := <-signalChan
			if s == syscall.SIGUSR1 {
				log.Infof("Received %v", s)
				fs.SigUsr1()
				continue
			}

			if len(flags.Cache) == 0 {
				log.Infof("Received %v, attempting to unmount...", s)

				err := TryUnmount(flags.MountPoint)
				if err != nil {
					log.Errorf("Failed to unmount in response to %v: %v", s, err)
				} else {
					log.Printf("Successfully unmounted %v in response to %v",
						flags.MountPoint, s)
					return
				}
			} else {
				log.Infof("Received %v", s)
				// wait for catfs to die and cleanup
			}
		}
	}()
}

var waitedForSignal os.Signal

func waitForSignal(wg *sync.WaitGroup) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGUSR1, syscall.SIGUSR2)

	wg.Add(1)
	go func() {
		waitedForSignal = <-signalChan
		wg.Done()
	}()
}

func kill(pid int, s os.Signal) (err error) {
	p, err := os.FindProcess(pid)
	if err != nil {
		return err
	}

	defer p.Release()

	err = p.Signal(s)
	if err != nil {
		return err
	}
	return
}

func fuseMount(
	dir string,
	bucketName string,
	flags *FlagStorage,
	ready chan error) (dev uintptr, err error) {

	// XXX really silly copy here! in goofys.Mount we will copy it
	// back to FlagStorage. But I don't see a easier way to expose
	// Config in the api package
	var config goofys.Config
	copier.Copy(&config, *flags)

	return goofys.FuseMount(dir, bucketName, &config, ready)
}

// Start serving requests, returning a fuse.MountedFileSystem that can be joined
// to wait for unmounting.
func fuseServe(
	ctx context.Context,
	bucketName string,
	dev uintptr,
	flags *FlagStorage,
	ready chan error) (fs *Goofys, mfs *fuse.MountedFileSystem, err error) {

	// XXX really silly copy here! in goofys.Mount we will copy it
	// back to FlagStorage. But I don't see a easier way to expose
	// Config in the api package
	var config goofys.Config
	copier.Copy(&config, *flags)

	return goofys.FuseServe(ctx, bucketName, dev, &config, ready)
}

func massagePath() {
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, "PATH=") {
			return
		}
	}

	// mount -a seems to run goofys without PATH
	// usually fusermount is in /bin
	os.Setenv("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
}

func massageArg0() {
	var err error
	os.Args[0], err = osext.Executable()
	if err != nil {
		panic(fmt.Sprintf("Unable to discover current executable: %v", err))
	}
}

var Version = "use `make build' to fill version hash correctly"

func main() {
	VersionHash = Version

	massagePath()

	app := NewApp()

	var flags *FlagStorage
	var child *os.Process

	app.Action = func(c *cli.Context) (err error) {
		// We should get two arguments exactly. Otherwise error out.
		if len(c.Args()) != 2 {
			fmt.Fprintf(
				os.Stderr,
				"Error: %s takes exactly two arguments.\n\n",
				app.Name)
			cli.ShowAppHelp(c)
			os.Exit(1)
		}

		// Populate and parse flags.
		bucketName := c.Args()[0]
		flags = PopulateFlags(c)
		if flags == nil {
			cli.ShowAppHelp(c)
			err = fmt.Errorf("invalid arguments")
			return
		}
		defer func() {
			time.Sleep(time.Second)
			flags.Cleanup()
		}()

		// Mount the file system.
		var dev uintptr
		ready := make(chan error, 1)
		fuseMounted, err := fuse.IsMounted(flags.MountPoint)
		if !fuseMounted {
			dev, err = fuseMount(flags.MountPoint, bucketName, flags, ready)
			if err != nil {
				return err
			}
			err = os.Setenv("GOOFYS_FUSE_FD", strconv.Itoa(int(dev)))
			if err != nil {
				return err
			}
		} else {
			// XXX: We do Reborn() after starting FuseMount, so for OSX we need other way
			// to safely wait for mount completion.
			ready <- nil
		}

		if !flags.Foreground {
			var wg sync.WaitGroup
			waitForSignal(&wg)

			massageArg0()

			ctx := new(daemon.Context)
			child, err = ctx.Reborn()

			if err != nil {
				panic(fmt.Sprintf("unable to daemonize: %v", err))
			}

			InitLoggers(!flags.Foreground && child == nil)

			if child != nil {
				// attempt to wait for child to notify parent
				wg.Wait()
				if waitedForSignal == syscall.SIGUSR1 {
					return
				} else {
					return fuse.EINVAL
				}
			} else {
				// kill our own waiting goroutine
				kill(os.Getpid(), syscall.SIGUSR1)
				wg.Wait()
				defer ctx.Release()

				devFd, err := strconv.Atoi(os.Getenv("GOOFYS_FUSE_FD"))
				if err != nil {
					return err
				}
				dev = uintptr(devFd)
			}

		} else {
			InitLoggers(!flags.Foreground)
		}

		// Start the fuse server.
		var mfs *fuse.MountedFileSystem
		var fs *Goofys
		fs, mfs, err = fuseServe(
			context.Background(),
			bucketName,
			dev,
			flags,
			ready)

		if err != nil {
			if !flags.Foreground {
				kill(os.Getppid(), syscall.SIGUSR2)
			}
			log.Fatalf("Mounting file system: %v", err)
			// fatal also terminates itself
		} else {
			if !flags.Foreground {
				kill(os.Getppid(), syscall.SIGUSR1)
			}
			log.Println("File system has been successfully mounted.")
			// Let the user unmount with Ctrl-C
			// (SIGINT). But if cache is on, catfs will
			// receive the signal and we would detect that exiting
			registerSIGINTHandler(fs, flags)

			// Wait for the file system to be unmounted.
			err = mfs.Join(context.Background())
			if err != nil {
				err = fmt.Errorf("MountedFileSystem.Join: %v", err)
				return
			}

			log.Println("Successfully exiting.")
		}
		return
	}

	err := app.Run(MassageMountFlags(os.Args))
	if err != nil {
		if flags != nil && !flags.Foreground && child != nil {
			log.Fatalln("Unable to mount file system, see syslog for details")
		}
		os.Exit(1)
	}
}
