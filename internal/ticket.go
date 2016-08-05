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
	"sync"
)

type Ticket struct {
	Total uint32

	total       uint32
	outstanding uint32

	mu   sync.Mutex
	cond *sync.Cond
}

func (ticket Ticket) Init() *Ticket {
	ticket.cond = sync.NewCond(&ticket.mu)
	ticket.total = ticket.Total
	return &ticket
}

func (ticket *Ticket) Take(howmany uint32, block bool) (took bool) {
	ticket.mu.Lock()
	defer ticket.mu.Unlock()

	for ticket.outstanding+howmany > ticket.total {
		if block {
			ticket.cond.Wait()
		} else {
			return
		}
	}

	ticket.outstanding += howmany
	took = true
	return
}

func (ticket *Ticket) Return(howmany uint32) {
	ticket.mu.Lock()
	defer ticket.mu.Unlock()

	ticket.outstanding -= howmany
	ticket.cond.Signal()
}
