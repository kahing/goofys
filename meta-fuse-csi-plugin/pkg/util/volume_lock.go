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

package util

import (
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"
)

const (
	VolumeOperationAlreadyExistsFmt = "An operation with the given volume key %s already exists"
)

// VolumeLocks implements a map with atomic operations. It stores a set of all volume IDs
// with an ongoing operation.
type VolumeLocks struct {
	locks sets.Set[string]
	mux   sync.Mutex
}

func NewVolumeLocks() *VolumeLocks {
	return &VolumeLocks{
		locks: sets.Set[string]{},
	}
}

// TryAcquire tries to acquire the lock for operating on volumeID and returns true if successful.
// If another operation is already using volumeID, returns false.
func (vl *VolumeLocks) TryAcquire(volumeID string) bool {
	vl.mux.Lock()
	defer vl.mux.Unlock()
	if vl.locks.Has(volumeID) {
		return false
	}
	vl.locks.Insert(volumeID)

	return true
}

func (vl *VolumeLocks) Release(volumeID string) {
	vl.mux.Lock()
	defer vl.mux.Unlock()
	vl.locks.Delete(volumeID)
}
