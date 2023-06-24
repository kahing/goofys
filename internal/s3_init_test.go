package internal

import (
	. "github.com/kahing/goofys/api/common"
	"net/http"
)
import . "gopkg.in/check.v1"


type S3Test struct {
	orignalTransport http.RoundTripper
}

var _ = Suite(&S3Test{})

func (s *S3Test) SetUpSuite(t *C) {
	s.orignalTransport = http.DefaultTransport
}

func (s *S3Test) TearDownSuite(t *C) {
	http.DefaultTransport = s.orignalTransport
}

type roundTripFunc func(r *http.Request) (*http.Response, error)

func (s roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return s(r)
}

func mockTransportForS3RegionDetection(t *C, expectedUrl, regionToMock string) http.RoundTripper {
	return roundTripFunc(func(r *http.Request) (*http.Response, error) {
		t.Assert(expectedUrl, Equals, r.URL.String())
		h := http.Header{}
		h.Add("X-Amz-Bucket-Region", regionToMock)
		h.Add("Server", "AmazonS3")
		return &http.Response{StatusCode: 403, Header: h, Request: r}, nil
	})
}

func (s *S3Test) TestDetectDNSSafeBucketRegionWithEndpoint(t *C) {
	// If end point is set, we should do HEAD on <endpoint>/<bucket> always.
	s3, err := NewS3("bucket1", &FlagStorage{Endpoint: "https://foo.com"}, &S3Config{})
	t.Assert(err, IsNil)
	http.DefaultTransport = mockTransportForS3RegionDetection(t, "https://foo.com/bucket1", "awesome-region1")
	err, isAws := s3.detectBucketLocationByHEAD()
	t.Assert(err, IsNil)
	t.Assert(isAws, Equals, true)
	t.Assert(*s3.awsConfig.Region, Equals, "awesome-region1")
}

func (s *S3Test) TestDetectDNSUnSafeBucketRegionWithEndpoint(t *C) {
	// If end point is set, we should do HEAD on <endpoint>/<bucket> always.
	s3, err := NewS3("buc_ket1", &FlagStorage{Endpoint: "https://foo.com"}, &S3Config{})
	t.Assert(err, IsNil)
	http.DefaultTransport = mockTransportForS3RegionDetection(t, "https://foo.com/buc_ket1", "awesome-region2")
	err, isAws := s3.detectBucketLocationByHEAD()
	t.Assert(err, IsNil)
	t.Assert(isAws, Equals, true)
	t.Assert(*s3.awsConfig.Region, Equals, "awesome-region2")
}

func (s *S3Test) TestDetectDNSSafeBucketRegion(t *C) {
	// bucket name is dns safe => use  <bucketname>.s3.amazonaws.com for region detection.
	s3, err := NewS3("bucket1", &FlagStorage{}, &S3Config{})
	t.Assert(err, IsNil)
	http.DefaultTransport = mockTransportForS3RegionDetection(t, "https://bucket1.s3.amazonaws.com", "awesome-region3")
	err, isAws := s3.detectBucketLocationByHEAD()
	t.Assert(err, IsNil)
	t.Assert(isAws, Equals, true)
	t.Assert(*s3.awsConfig.Region, Equals, "awesome-region3")
}

func (s *S3Test) TestDetectDNSUnSafeBucketRegion(t *C) {
	// bucket name is *not• dns safe => use  s3.amazonaws.com/<bucketname> for region detection.
	s3, err := NewS3("buc_ket1", &FlagStorage{}, &S3Config{})
	t.Assert(err, IsNil)
	http.DefaultTransport = mockTransportForS3RegionDetection(t, "https://s3.amazonaws.com/buc_ket1", "awesome-region4")
	err, isAws := s3.detectBucketLocationByHEAD()
	t.Assert(err, IsNil)
	t.Assert(isAws, Equals, true)
	t.Assert(*s3.awsConfig.Region, Equals, "awesome-region4")
}

func (s *S3Test) TestDetectBucketNameWithDotRegion(t *C) {
	// if the bucket name has dot, we cant use use <bucketname>.s3.amazonaws.com.
	s3, err := NewS3("buck.et1", &FlagStorage{}, &S3Config{})
	t.Assert(err, IsNil)
	http.DefaultTransport = mockTransportForS3RegionDetection(t, "https://s3.amazonaws.com/buck.et1", "awesome-region5")
	err, isAws := s3.detectBucketLocationByHEAD()
	t.Assert(err, IsNil)
	t.Assert(isAws, Equals, true)
	t.Assert(*s3.awsConfig.Region, Equals, "awesome-region5")
}
