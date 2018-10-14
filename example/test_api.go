package main

import (
	goofys "github.com/kahing/goofys/api"

	"fmt"

	"golang.org/x/net/context"
)

func main() {
	mountPoint := "/tmp/s3"
	bucketName := "goofys"

	config := goofys.Config{
		MountPoint: mountPoint,
		DirMode:    0755,
		FileMode:   0644,
	}

	ready := make(chan error, 1)
	dev, err := goofys.FuseMount(mountPoint, bucketName, &config, ready)
	if err != nil {
		panic(fmt.Sprintf("Unable to mount %v: %v", config.MountPoint, err))
	}

	_, mp, err := goofys.FuseServe(context.Background(), bucketName, dev, &config, ready)
	if err != nil {
		panic(fmt.Sprintf("Unable to mount %v: %v", config.MountPoint, err))
	} else {
		mp.Join(context.Background())
	}
}
