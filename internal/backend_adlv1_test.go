package internal

import (
	adl "github.com/Azure/azure-sdk-for-go/services/datalake/store/2016-11-01/filesystem"
	. "github.com/kahing/goofys/api/common"
	. "gopkg.in/check.v1"
)

type Adlv1Test struct {
	adlv1 *ADLv1
}

var _ = Suite(&Adlv1Test{})

func (s *Adlv1Test) SetUpSuite(t *C) {
	var err error
	s.adlv1, err = NewADLv1("", &FlagStorage{}, &ADLv1Config{
		Endpoint: "test.endpoint",
	})
	t.Assert(err, IsNil)
}

func (s *Adlv1Test) TestAdlv1FileStatus2BlobItem(t *C) {
	type Adlv1FileStatus2BlobItemTest struct {
		description               string
		inputFileStatusProperties *adl.FileStatusProperties
		expectedBlobItemOutput    BlobItemOutput
	}

	adlv1FileStatus2BlobItemTests := []Adlv1FileStatus2BlobItemTest{
		{
			description: "Should handle nil ModificationTime and Length",
			inputFileStatusProperties: &adl.FileStatusProperties{},
			expectedBlobItemOutput: BlobItemOutput{},
		},
		{
			description: "Should handle non-nil ModificationTime and Length",
			inputFileStatusProperties: &adl.FileStatusProperties{
				ModificationTime: PInt64(1000),
				Length: PInt64(100),
			},
			expectedBlobItemOutput: BlobItemOutput{
				LastModified: PTime(adlv1LastModified(1000)),
				Size: uint64(100),
			},
		},
	}

	for i, test := range adlv1FileStatus2BlobItemTests {
		t.Logf("Test %d: \t%s", i, test.description)
		blobItemOutput := adlv1FileStatus2BlobItem(test.inputFileStatusProperties, nil)
		t.Check(blobItemOutput, DeepEquals, test.expectedBlobItemOutput)
	}
}
