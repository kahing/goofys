package internal

import (
	. "gopkg.in/check.v1"
)

type DirTest struct{}

var _ = Suite(&DirTest{})

func (s *DirTest) TestHasCharLtSlash(t *C) {
	t.Assert(hasCharLtSlash("wow"), Equals, false)
	// '-', ' ' are less than '/'
	t.Assert(hasCharLtSlash("w-o-w"), Equals, true)
	t.Assert(hasCharLtSlash("w o w"), Equals, true)
	// All unicode chars have multi-byte values and are > '/'
	t.Assert(hasCharLtSlash("wøw"), Equals, false)
}

func (s *DirTest) TestCloudPathToName(t *C) {
	t.Assert(cloudPathToName(""), Equals, "")
	t.Assert(cloudPathToName("/"), Equals, "")
	t.Assert(cloudPathToName("/a/b/c"), Equals, "c")
	t.Assert(cloudPathToName("a/b/c"), Equals, "c")
	t.Assert(cloudPathToName("/a/b/c/"), Equals, "c")
}

func (s *DirTest) TestShouldFetchNextListBlobsPage(t *C) {
	// Output is not truncated => No more pages fetch => ***FALSE***
	// (No matter what Items and Prefixes are present)
	t.Assert(shouldFetchNextListBlobsPage(
		&ListBlobsOutput{IsTruncated: false}), Equals, false)
	t.Assert(shouldFetchNextListBlobsPage(
		&ListBlobsOutput{
			IsTruncated: false,
			Prefixes:    []BlobPrefixOutput{{Prefix: PString("prefix-has-dash/")}},
		}),
		Equals, false)
	t.Assert(shouldFetchNextListBlobsPage(
		&ListBlobsOutput{
			IsTruncated: false,
			Items:       []BlobItemOutput{{Key: PString("item-has-dash")}},
		}),
		Equals, false)

	// Last Item and last Prefix are both "normal". All chars in their
	// name (not path) are > '/' => ***FALSE***
	t.Assert(shouldFetchNextListBlobsPage(
		&ListBlobsOutput{
			IsTruncated: true,
			Items: []BlobItemOutput{
				{Key: PString("w-o-w/item-has-dash")},
				{Key: PString("w-o-w/item")}},
			Prefixes: []BlobPrefixOutput{
				{Prefix: PString("w-o-w/prefix-has-dash/")},
				{Prefix: PString("w-o-w/prefix/")}},
		}),
		Equals, false)

	// Last Item's name has '-' ('-' < '/'); No prefixes => ***TRUE***
	t.Assert(shouldFetchNextListBlobsPage(
		&ListBlobsOutput{
			IsTruncated: true,
			Items:       []BlobItemOutput{{Key: PString("wow/item-has-dash")}},
		}),
		Equals, true)
	// Last Item's name has '-' ('-' < '/'); Has normal prefixes  => ***TRUE***
	t.Assert(shouldFetchNextListBlobsPage(
		&ListBlobsOutput{
			IsTruncated: true,
			Prefixes:    []BlobPrefixOutput{{Prefix: PString("wow/prefix")}},
			Items:       []BlobItemOutput{{Key: PString("wow/item-has-dash")}},
		}),
		Equals, true)

	// Last Prefix's name has '-' ('-' < '/'); No Items => ***TRUE***
	t.Assert(shouldFetchNextListBlobsPage(
		&ListBlobsOutput{
			IsTruncated: true,
			Prefixes:    []BlobPrefixOutput{{Prefix: PString("wow/prefix-has-dash/")}},
		}),
		Equals, true)
	// Last Prefix's name has '-' ('-' < '/'); Has normal items => ***TRUE***
	t.Assert(shouldFetchNextListBlobsPage(
		&ListBlobsOutput{
			IsTruncated: true,
			Items:       []BlobItemOutput{{Key: PString("wow/item")}},
			Prefixes:    []BlobPrefixOutput{{Prefix: PString("wow/prefix-has-dash/")}},
		}),
		Equals, true)

}
