// Copyright 2015 Google Inc. All Rights Reserved.
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

// System permissions-related code.
package internal

import (
	"fmt"
	"os/user"
	"strconv"
)

// Return the UID and GID of this process.
func MyUserAndGroup() (uid uint32, gid uint32, err error) {
	// Ask for the current user.
	user, err := user.Current()
	if err != nil {
		panic(err)
	}

	// Parse UID.
	uid64, err := strconv.ParseUint(user.Uid, 10, 32)
	if err != nil {
		err = fmt.Errorf("Parsing UID (%s): %v", user.Uid, err)
		return
	}

	// Parse GID.
	gid64, err := strconv.ParseUint(user.Gid, 10, 32)
	if err != nil {
		err = fmt.Errorf("Parsing GID (%s): %v", user.Gid, err)
		return
	}

	uid = uint32(uid64)
	gid = uint32(gid64)

	return
}
