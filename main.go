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
	"log"
	"os"
	"os/signal"

	"golang.org/x/net/context"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/codegangsta/cli"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"
)

func registerSIGINTHandler(mountPoint string) {
	// Register for SIGINT.
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	// Start a goroutine that will unmount when the signal is received.
	go func() {
		for {
			<-signalChan
			log.Println("Received SIGINT, attempting to unmount...")

			err := fuse.Unmount(mountPoint)
			if err != nil {
				log.Printf("Failed to unmount in response to SIGINT: %v", err)
			} else {
				log.Printf("Successfully unmounted in response to SIGINT.")
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
	flags *FlagStorage) (mfs *fuse.MountedFileSystem, err error) {

	// Choose UID and GID.
	uid, gid, err := MyUserAndGroup()
	if err != nil {
		err = fmt.Errorf("MyUserAndGroup: %v", err)
		return
	}

	if int32(flags.Uid) == -1 {
		flags.Uid = uid
	}

	if int32(flags.Gid) == -1 {
		flags.Gid = gid
	}

	awsConfig := &aws.Config{
		Region: aws.String("us-west-2"),
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
	server := fuseutil.NewFileSystemServer(goofys)

	// Mount the file system.
	mountCfg := &fuse.MountConfig{
		FSName:                  bucketName,
		Options:                 flags.MountOptions,
		ErrorLogger:             log.New(os.Stderr, "fuse: ", log.Flags()),
		DisableWritebackCaching: true,
	}

	if flags.DebugFuse {
		mountCfg.DebugLogger = log.New(os.Stderr, "fuse_debug: ", 0)
	}

	mfs, err = fuse.Mount(mountPoint, server, mountCfg)
	if err != nil {
		err = fmt.Errorf("Mount: %v", err)
		return
	}

	return
}

func main() {
	// Make logging output better.
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

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

		// Mount the file system.
		mfs, err := mount(
			context.Background(),
			bucketName,
			mountPoint,
			flags)

		if err != nil {
			log.Fatalf("Mounting file system: %v", err)
		}

		log.Println("File system has been successfully mounted.")

		// Let the user unmount with Ctrl-C (SIGINT).
		registerSIGINTHandler(mfs.Dir())

		// Wait for the file system to be unmounted.
		err = mfs.Join(context.Background())
		if err != nil {
			err = fmt.Errorf("MountedFileSystem.Join: %v", err)
			return
		}

		log.Println("Successfully exiting.")
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatalln(err)
	}
}
