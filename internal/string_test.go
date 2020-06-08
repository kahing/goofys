package internal

import (
	"fmt"
	"testing"
)

func TestParseBucketSpec(t *testing.T){
	spec, err := ParseBucketSpec("s3://hello")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("HELLO")
	fmt.Println(spec)
	fmt.Println(spec.Bucket)
}
