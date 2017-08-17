package main

import (
	goofys "github.com/kahing/goofys/api"

	"fmt"
	"golang.org/x/net/context"
)

func main() {
	config := goofys.Config{
		MountPoint: "/tmp/s3",
		DirMode:    0755,
		FileMode:   0644,
	}

	mp, err := goofys.Mount(context.Background(), "goofys", &config)
	if err != nil {
		panic(fmt.Sprintf("Unable to mount %v: %v", config.MountPoint, err))
	} else {
		mp.Join(context.Background())
	}
}
