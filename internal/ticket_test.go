// Copyright 2016 Ka-Hing Cheung
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
	. "gopkg.in/check.v1"
)

type TicketTest struct {
}

var _ = Suite(&TicketTest{})

func (s *TicketTest) TestTicket(t *C) {
	ticket := Ticket{Total: 1}.Init()

	t.Assert(ticket.Take(1, false), Equals, true)
	t.Assert(ticket.Take(1, false), Equals, false)
	ticket.Return(1)
	t.Assert(ticket.Take(1, false), Equals, true)
	t.Assert(ticket.Take(1, false), Equals, false)
}

func (s *TicketTest) TestTicketParallel(t *C) {
	ticket := Ticket{Total: 1}.Init()
	for i := 0; i < 10; i++ {
		ticket.Take(1, true)

		go func() {
			ticket.Return(1)
		}()
	}
}
