/*
Copyright 2018 The Kubernetes Authors.
Copyright 2022 Google LLC
Copyright 2023 Preferred Networks, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package csimounter

import (
	"fmt"
	"os"
	"reflect"
	"testing"
)

var defaultCsiMountOptions = []string{
	"nodev",
	"nosuid",
	"allow_other",
	"default_permissions",
	"rootmode=40000",
	fmt.Sprintf("user_id=%d", os.Getuid()),
	fmt.Sprintf("group_id=%d", os.Getgid()),
}

func TestPrepareMountArgs(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                       string
		inputMountOptions          []string
		expecteCsiMountOptions     []string
		expecteSidecarMountOptions []string
	}{
		{
			name:                       "should return valid options correctly with empty input",
			inputMountOptions:          []string{},
			expecteCsiMountOptions:     defaultCsiMountOptions,
			expecteSidecarMountOptions: []string{},
		},
		{
			name:                       "should return valid options correctly with CSI mount options only",
			inputMountOptions:          []string{"ro", "o=noexec", "o=noatime", "o=invalid"},
			expecteCsiMountOptions:     append(defaultCsiMountOptions, "ro", "noexec", "noatime"),
			expecteSidecarMountOptions: []string{},
		},
		{
			name:                       "should return valid options correctly with sidecar mount options only",
			inputMountOptions:          []string{"implicit-dirs", "max-conns-per-host=10"},
			expecteCsiMountOptions:     defaultCsiMountOptions,
			expecteSidecarMountOptions: []string{"implicit-dirs", "max-conns-per-host=10"},
		},
		{
			name:                       "should return valid options correctly with CSI and sidecar mount options",
			inputMountOptions:          []string{"ro", "implicit-dirs", "max-conns-per-host=10", "o=noexec", "o=noatime", "o=invalid"},
			expecteCsiMountOptions:     append(defaultCsiMountOptions, "ro", "noexec", "noatime"),
			expecteSidecarMountOptions: []string{"implicit-dirs", "max-conns-per-host=10"},
		},
	}

	for _, tc := range testCases {
		t.Logf("test case: %s", tc.name)

		c, s := prepareMountOptions(tc.inputMountOptions)
		if !reflect.DeepEqual(countOptionOccurrence(c), countOptionOccurrence(tc.expecteCsiMountOptions)) {
			t.Errorf("Got options %v, but expected %v", c, tc.expecteCsiMountOptions)
		}

		if !reflect.DeepEqual(countOptionOccurrence(s), countOptionOccurrence(tc.expecteSidecarMountOptions)) {
			t.Errorf("Got options %v, but expected %v", s, tc.expecteSidecarMountOptions)
		}
	}
}

func countOptionOccurrence(options []string) map[string]int {
	dict := make(map[string]int)
	for _, o := range options {
		dict[o]++
	}

	return dict
}
