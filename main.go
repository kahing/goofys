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
	. "github.com/kahing/goofys/internal"

	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/codegangsta/cli"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"

	"github.com/kardianos/osext"

	"github.com/sirupsen/logrus"

	daemon "github.com/sevlyar/go-daemon"
)

var log = GetLogger("main")

func registerSIGINTHandler(flags *FlagStorage) {
	// Register for SIGINT.
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	// Start a goroutine that will unmount when the signal is received.
	go func() {
		for {
			s := <-signalChan
			if len(flags.Cache) == 0 {
				log.Infof("Received %v, attempting to unmount...", s)

				err := tryUnmount(flags.MountPoint)
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

func tryUnmount(mountPoint string) (err error) {
	for i := 0; i < 20; i++ {
		err = fuse.Unmount(mountPoint)
		if err != nil {
			time.Sleep(time.Second)
		} else {
			break
		}
	}
	return
}

// Mount the file system based on the supplied arguments, returning a
// fuse.MountedFileSystem that can be joined to wait for unmounting.
func mount(
	ctx context.Context,
	bucketName string,
	flags *FlagStorage) (mfs *fuse.MountedFileSystem, err error) {

	awsConfig := &aws.Config{
		Region: &flags.Region,
		Logger: GetLogger("s3"),
		//LogLevel: aws.LogLevel(aws.LogDebug),
	}

	if len(flags.Profile) > 0 {
		awsConfig.Credentials = credentials.NewSharedCredentials("", flags.Profile)
	}

	if len(flags.Endpoint) > 0 {
		awsConfig.Endpoint = &flags.Endpoint
	}

	awsConfig.S3ForcePathStyle = aws.Bool(true)

	goofys := NewGoofys(bucketName, awsConfig, flags)
	if goofys == nil {
		err = fmt.Errorf("Mount: initialization failed")
		return
	}
	server := fuseutil.NewFileSystemServer(goofys)

	fuseLog := GetLogger("fuse")

	// Mount the file system.
	mountCfg := &fuse.MountConfig{
		FSName:                  bucketName,
		Options:                 flags.MountOptions,
		ErrorLogger:             GetStdLogger(NewLogger("fuse"), logrus.ErrorLevel),
		DisableWritebackCaching: true,
	}

	if flags.DebugFuse {
		fuseLog.Level = logrus.DebugLevel
		log.Level = logrus.DebugLevel
		mountCfg.DebugLogger = GetStdLogger(fuseLog, logrus.DebugLevel)
	}

	mfs, err = fuse.Mount(flags.MountPoint, server, mountCfg)
	if err != nil {
		err = fmt.Errorf("Mount: %v", err)
		return
	}

	if len(flags.Cache) != 0 {
		log.Infof("Starting catfs %v", flags.Cache)
		catfs := exec.Command("catfs", flags.Cache...)
		catfs.Env = append(catfs.Env, "RUST_LOG=info")
		lvl := logrus.InfoLevel
		if flags.DebugFuse {
			lvl = logrus.DebugLevel
		}
		catfsLog := GetLogger("catfs")
		catfsLog.Formatter.(*LogHandle).Lvl = &lvl
		catfs.Stderr = catfsLog.Writer()
		err = catfs.Start()
		if err != nil {
			err = fmt.Errorf("Failed to start catfs: %v", err)

			// sleep a bit otherwise can't unmount right away
			time.Sleep(time.Second)
			err2 := tryUnmount(flags.MountPoint)
			if err2 != nil {
				err = fmt.Errorf("%v. Failed to unmount: %v", err, err2)
			}
		}

		go func() {
			err := catfs.Wait()
			log.Errorf("catfs exited: %v", err)

			if err != nil {
				// if catfs terminated cleanly, it
				// should have unmounted this,
				// otherwise we will do it ourselves
				err2 := tryUnmount(flags.MountPointArg)
				if err2 != nil {
					log.Errorf("Failed to unmount: %v", err2)
				}
			}

			if flags.MountPointArg != flags.MountPoint {
				err2 := tryUnmount(flags.MountPoint)
				if err2 != nil {
					log.Errorf("Failed to unmount: %v", err2)
				}
			}

			if err != nil {
				os.Exit(1)
			}
		}()
	}

	return
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
			}

		} else {
			InitLoggers(!flags.Foreground)
		}

		// Mount the file system.
		var mfs *fuse.MountedFileSystem
		mfs, err = mount(
			context.Background(),
			bucketName,
			flags)

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
			registerSIGINTHandler(flags)

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
