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
	"time"
	"unicode"

	"github.com/jacobsa/fuse"
)

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
