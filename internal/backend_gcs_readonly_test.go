package internal

import (
	"cloud.google.com/go/storage"
	"context"
	"github.com/kahing/goofys/api/common"
	"github.com/spf13/viper"
	"os"

	. "gopkg.in/check.v1"
)

type GcsReadOnlyBackendTest struct {
	gcsBackend *GCSBackend
	envVal     string
}

var _ = Suite(&GcsReadOnlyBackendTest{})

func (s *GcsReadOnlyBackendTest) SetUpSuite(c *C) {
	viper.SetConfigType("yaml")
	viper.SetConfigFile(os.ExpandEnv(os.Getenv("CONFIG_FILE")))

	err := viper.ReadInConfig() // Find and read the config file
	c.Assert(err, IsNil, Commentf("ERROR: reading config file: %s \n", err))
	s.envVal = os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
}

func (s *GcsReadOnlyBackendTest) SetUpTest(c *C) {
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS",
		os.ExpandEnv(viper.GetString("goofys.gcs.readOnlyCredentials")))
	gcsBackend, _ := getGcsBackend()
	s.gcsBackend = gcsBackend
}

func (s *GcsReadOnlyBackendTest) TearDownTest(c *C) {
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", s.envVal)
}

func (s *GcsReadOnlyBackendTest) TestGCSConfig_WithoutCredentials(c *C) {
	os.Unsetenv("GOOGLE_APPLICATION_CREDENTIALS")
	_, err := common.NewGCSConfig("", viper.GetString("goofys.gcs.bucketName"), "")
	c.Assert(err, ErrorMatches, ".*could not find default credentials.*")
}

func (s *GcsReadOnlyBackendTest) TestGCS_ReadOnlyBlobDoesNotExist(c *C) {
	bkt := s.gcsBackend.client.Bucket(s.gcsBackend.Bucket())
	randomObjectName := s.gcsBackend.config.Prefix + (RandStringBytesMaskImprSrc(32))
	_, err := bkt.Object(randomObjectName).Attrs(context.Background())
	c.Assert(err, Equals, storage.ErrObjectNotExist)
	_, err = bkt.Attrs(context.Background())
	c.Assert(err, IsNil)
}

func (s *GcsReadOnlyBackendTest) TestGCS_ReadOnlyBucketDoesNotExist(c *C) {
	bkt := s.gcsBackend.client.Bucket(viper.GetString("goofys.gcs.bucketWithoutPermission"))
	randomObjectName := s.gcsBackend.config.Prefix + (RandStringBytesMaskImprSrc(32))
	_, err := bkt.Object(randomObjectName).Attrs(context.Background())
	c.Assert(err, Equals, storage.ErrObjectNotExist)
	_, err = bkt.Attrs(context.Background())
	c.Assert(err, ErrorMatches, "storage: bucket doesn't exist")
}
