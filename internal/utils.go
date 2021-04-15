// Copyright 2015 - 2017 Ka-Hing Cheung
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

package internal

import (
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/jacobsa/fuse"
	"github.com/shirou/gopsutil/process"
)

var TIME_MAX = time.Unix(1<<63-62135596801, 999999999)

func MaxInt(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinInt(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinInt64(a, b int64) int64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func MaxUInt32(a, b uint32) uint32 {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinUInt32(a, b uint32) uint32 {
	if a < b {
		return a
	} else {
		return b
	}
}

func MaxUInt64(a, b uint64) uint64 {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinUInt64(a, b uint64) uint64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func PBool(v bool) *bool {
	return &v
}

func PInt32(v int32) *int32 {
	return &v
}

func PUInt32(v uint32) *uint32 {
	return &v
}

func PInt64(v int64) *int64 {
	return &v
}

func PUInt64(v uint64) *uint64 {
	return &v
}

func PString(v string) *string {
	return &v
}

func PTime(v time.Time) *time.Time {
	return &v
}

func NilStr(v *string) string {
	if v == nil {
		return ""
	} else {
		return *v
	}
}

func NilUint32(v *uint32) uint32 {
	if v == nil {
		return 0
	} else {
		return *v
	}
}

func NilInt64(v *int64) int64 {
	if v == nil {
		return 0
	} else {
		return *v
	}
}

func NilUint64(v *uint64) uint64 {
	if v == nil {
		return 0
	} else {
		return *v
	}
}

func xattrEscape(value []byte) (s string) {
	for _, c := range value {
		if c == '%' {
			s += "%25"
		} else if unicode.IsPrint(rune(c)) {
			s += string(c)
		} else {
			s += "%" + fmt.Sprintf("%02X", c)
		}
	}

	return
}

func Dup(value []byte) []byte {
	ret := make([]byte, len(value))
	copy(ret, value)
	return ret
}

func TryUnmount(mountPoint string) (err error) {
	for i := 0; i < 20; i++ {
		err = fuse.Unmount(mountPoint)
		if err != nil {
			time.Sleep(time.Second)
		} else {
			break
		}
	}
	return
}

type empty struct{}

// TODO(dotslash/khc): Remove this semaphore in favor of
// https://godoc.org/golang.org/x/sync/semaphore
type semaphore chan empty

func (sem semaphore) P(n int) {
	for i := 0; i < n; i++ {
		sem <- empty{}
	}
}

func (sem semaphore) V(n int) {
	for i := 0; i < n; i++ {
		<-sem
	}
}

// GetTgid returns the tgid for the given pid.
func GetTgid(pid uint32) (tgid *int32, err error) {
	p, err := process.NewProcess(int32(pid))
	if err != nil {
		return nil, err
	}
	tgidVal, err := p.Tgid()
	if err != nil {
		return nil, err
	}
	return &tgidVal, nil
}

func PMetadata(m map[string]string) map[string]*string {
	metadata := make(map[string]*string)
	for k, _ := range m {
		k = strings.ToLower(k)
		v := m[k]
		metadata[k] = &v
	}
	return metadata
}

func NilMetadata(m map[string]*string) map[string]string {
	metadata := make(map[string]string)
	for k, v := range m {
		k = strings.ToLower(k)
		metadata[k] = NilStr(v)
	}
	return metadata
}

// The following functions are useful to debug byte sizes
func ConvertBytesToIEC(size int64) string {
	var unit int64 = 1024
	if size < unit {
		return fmt.Sprintf("%d %c", size, 'B')
	}
	// divide size by unit until it is less than unit
	curSize, exp := float64(size), 0
	for curSize >= float64(unit) {
		curSize /= float64(unit)
		exp += 1
	}
	return fmt.Sprintf("%.1f %ciB", curSize, "KMGTPEZY"[exp-1])
}