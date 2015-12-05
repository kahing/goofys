// Copyright 2015 Ka-Hing Cheung
// Copyright 2015 Google Inc. All Rights Reserved.
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
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/net/context"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/codegangsta/cli"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	"github.com/Sirupsen/logrus"
	"github.com/kardianos/osext"

	daemon "github.com/sevlyar/go-daemon"
)

var log = GetLogger("main")

func registerSIGINTHandler(mountPoint string) {
	// Register for SIGINT.
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	// Start a goroutine that will unmount when the signal is received.
	go func() {
		for {
			s := <-signalChan
			log.Infof("Received %v, attempting to unmount...", s)

			err := fuse.Unmount(mountPoint)
			if err != nil {
				log.Errorf("Failed to unmount in response to %v: %v", s, err)
			} else {
				log.Printf("Successfully unmounted %v in response to %v", mountPoint, s)
				return
			}
		}
	}()
}

// Mount the file system based on the supplied arguments, returning a
// fuse.MountedFileSystem that can be joined to wait for unmounting.
func mount(
	ctx context.Context,
	bucketName string,
	mountPoint string,
	flags *FlagStorage) (wg *sync.WaitGroup, err error) {

	awsConfig := &aws.Config{
		Region: aws.String("us-west-2"),
		Logger: GetLogger("s3"),
		//LogLevel: aws.LogLevel(aws.LogDebug),
	}
	if len(flags.Endpoint) > 0 {
		awsConfig.Endpoint = &flags.Endpoint
	}
	if flags.UsePathRequest {
		awsConfig.S3ForcePathStyle = aws.Bool(true)
	}

	goofys := NewGoofys(bucketName, awsConfig, flags)
	if goofys == nil {
		err = fmt.Errorf("Mount: initialization failed")
		return
	}

	if flags.DebugFuse {
		fuselog := GetLogger("fuse")
		fuselog.Level = logrus.DebugLevel
		fuse.Debug = func(msg interface{}) {
			fuselog.Debug(msg)
		}
	}

	c, err := fuse.Mount(
		mountPoint,
		fuse.FSName("goofys#"+bucketName),
		fuse.Subtype("goofys"),
		fuse.VolumeName(bucketName),
		fuse.MaxReadahead(1*1024*1024),
	)
	if err != nil {
		return
	}

	var waitgroup sync.WaitGroup
	wg = &waitgroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err = fs.Serve(c, goofys)
		if err != nil {
			log.Fatal(err)
		}

		// check if the mount process has an error to report
		<-c.Ready
		if err := c.MountError; err != nil {
			log.Fatal(err)
		}

		log.Info("Server shutdown")
	}()

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
	os.Setenv("PATH", "/bin")
}

func massageArg0() {
	var err error
	os.Args[0], err = osext.Executable()
	if err != nil {
		panic(fmt.Sprintf("Unable to discover current executable: %v", err))
	}
}

func main() {
	app := NewApp()
	app.Action = func(c *cli.Context) {
		var err error

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
		mountPoint := c.Args()[1]
		flags := PopulateFlags(c)

		InitLoggers(!flags.Foreground)

		massagePath()

		if !flags.Foreground {
			massageArg0()

			ctx := new(daemon.Context)
			child, err := ctx.Reborn()

			if err != nil {
				panic(fmt.Sprintf("unable to daemonize: %v", err))
			}

			if child != nil {
				return
			} else {
				defer ctx.Release()
			}

		}

		// Mount the file system.
		wg, err := mount(
			context.Background(),
			bucketName,
			mountPoint,
			flags)

		if err != nil {
			log.Fatalf("Mounting file system: %v", err)
		}

		log.Println("File system has been successfully mounted.")

		// Let the user unmount with Ctrl-C (SIGINT).
		registerSIGINTHandler(mountPoint)

		// Wait for the file system to be unmounted.
		wg.Wait()

		log.Println("Successfully exiting.")
	}

	err := app.Run(MassageMountFlags(os.Args))
	if err != nil {
		log.Fatalln(err)
	}
}
