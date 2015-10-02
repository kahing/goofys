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

package main

// the goal is to allow each file handle to request a limited number
// of buffers, while recycling them across many file handles

// we should support a global limit as well as a per-handle limit

// XXX investigate using sync.Pool

import (
	"sync"
)

type BufferPoolHandle struct {
	mu sync.Mutex
	cond *sync.Cond

	inUseBuffers int64
	maxBuffers int64 // maximum number of buffers for this handle
	pool *BufferPool
}

type BufferPool struct {
	mu sync.Mutex
	cond *sync.Cond

	freelist [][]byte // list of free buffers
	numBuffers int64
	maxBuffersGlobal int64
	maxBuffersPerHandle int64
}

const BUF_SIZE = 5 * 1024 * 1024

func NewBufferPool(maxSizeGlobal int64, maxSizePerHandle int64) *BufferPool {
	pool := &BufferPool{
		maxBuffersGlobal: maxSizeGlobal / BUF_SIZE,
		maxBuffersPerHandle: maxSizePerHandle / BUF_SIZE,
	}
	pool.cond = sync.NewCond(&pool.mu)
	return pool
}

func (pool *BufferPool) NewPoolHandle() *BufferPoolHandle {
	handle := &BufferPoolHandle{ maxBuffers: pool.maxBuffersPerHandle, pool: pool }
	handle.cond = sync.NewCond(&handle.mu)
	return handle
}

func (pool *BufferPool) requestBuffer() (buf []byte) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	for len(pool.freelist) == 0 {
		if pool.numBuffers < pool.maxBuffersGlobal {
			pool.numBuffers++
			buf = make([]byte, 0, BUF_SIZE)
			return
		} else {
			pool.cond.Wait()
		}
	}

	buf = pool.freelist[len(pool.freelist) - 1]
	pool.freelist = pool.freelist[0 : len(pool.freelist) - 1]
	return
}

func (pool *BufferPool) freeBuffer(buf []byte) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	// XXX return some buffers if we have too many on freelist
	pool.freelist = append(pool.freelist, buf)
	pool.cond.Signal()
}

func (h *BufferPoolHandle) Request() []byte {
	h.mu.Lock()
	defer h.mu.Unlock()

	for h.inUseBuffers == h.maxBuffers {
		h.cond.Wait()
	}

	buf := h.pool.requestBuffer()
	h.inUseBuffers++
	return buf
}

func (h *BufferPoolHandle) Free(buf []byte) {
	buf = buf[:0]
	h.pool.freeBuffer(buf)

	h.mu.Lock()
	defer h.mu.Unlock()

	h.inUseBuffers--
	h.cond.Signal()
}

// copy from -> to, reslicing to if necessary
func (h *BufferPoolHandle) Copy(to *[]byte, from []byte) (nCopied int) {
	toLen := len(*to)
	avail := cap(*to) - toLen
	if avail < len(from) {
		nCopied = avail
	} else {
		nCopied = len(from)
	}

	if nCopied != 0 {
		*to = (*to)[0 : toLen + nCopied]
		copy((*to)[toLen:], from)
	}
	return
}
