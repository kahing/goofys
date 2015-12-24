// Copyright 2015 Ka-Hing Cheung
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

// the goal is to allow each file handle to request a limited number
// of buffers, while recycling them across many file handles

// we should support a global limit as well as a per-handle limit

// XXX investigate using sync.Pool

import (
	"io"
	"runtime"
	"sync"

	"github.com/shirou/gopsutil/mem"
)

type BufferPoolHandle struct {
	mu   sync.Mutex
	cond *sync.Cond

	inUseBuffers int64
	maxBuffers   int64 // maximum number of buffers for this handle
	pool         *BufferPool
}

type BufferPool struct {
	mu   sync.Mutex
	cond *sync.Cond

	numBuffers          int64
	maxBuffersGlobal    int64
	maxBuffersPerHandle int64

	totalBuffers             int64
	computedMaxBuffersGlobal int64
}

const BUF_SIZE = 10 * 1024 * 1024

func maxMemToUse() int64 {
	m, err := mem.VirtualMemory()
	if err != nil {
		panic(err)
	}

	log.Debugf("amount of free memory: %v", m.Free)

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	log.Debugf("amount of allocated memory: %v", ms.Alloc)

	max := int64(m.Free+ms.Alloc) / 2
	maxBuffersGlobal := max/BUF_SIZE + 1
	log.Debugf("using up to %v %vMB buffers", maxBuffersGlobal, BUF_SIZE/1024/1024)
	return max
}

func (pool BufferPool) Init() *BufferPool {
	pool.maxBuffersPerHandle = 200 * 1024 * 1024 / BUF_SIZE
	pool.cond = sync.NewCond(&pool.mu)

	return &pool
}

// for testing
func NewBufferPool(maxSizeGlobal int64, maxSizePerHandle int64) *BufferPool {
	pool := &BufferPool{
		maxBuffersGlobal:    maxSizeGlobal / BUF_SIZE,
		maxBuffersPerHandle: maxSizePerHandle / BUF_SIZE,
	}
	pool.cond = sync.NewCond(&pool.mu)
	return pool
}

func (pool *BufferPool) NewPoolHandle() *BufferPoolHandle {
	handle := &BufferPoolHandle{maxBuffers: pool.maxBuffersPerHandle, pool: pool}
	handle.cond = sync.NewCond(&handle.mu)
	return handle
}

func (pool *BufferPool) requestBuffer() (buf []byte) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	maxBuffersGlobal := pool.computedMaxBuffersGlobal
	if pool.totalBuffers%100 == 0 {
		pool.computedMaxBuffersGlobal = maxMemToUse()/BUF_SIZE + 1
	}

	if maxBuffersGlobal == 0 {
		maxBuffersGlobal = pool.computedMaxBuffersGlobal
	}

	for pool.numBuffers >= maxBuffersGlobal {
		pool.cond.Wait()
	}

	pool.numBuffers++
	pool.totalBuffers++
	buf = make([]byte, 0, BUF_SIZE)
	return
}

func (pool *BufferPool) freeBuffer(buf []byte) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	pool.numBuffers--
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
		*to = (*to)[0 : toLen+nCopied]
		copy((*to)[toLen:], from)
	}
	return
}

var mbufLog = GetLogger("mbuf")

type MBuf struct {
	pool    *BufferPoolHandle
	buffers [][]byte
	rbuf    int
	wbuf    int
	rp      int
	wp      int
}

func (mb MBuf) Init(h *BufferPoolHandle, size uint64) *MBuf {
	allocated := uint64(0)
	mb.pool = h

	for allocated < size {
		b := h.Request()
		allocated += uint64(cap(b))
		mb.buffers = append(mb.buffers, b)
	}

	return &mb
}

func (mb *MBuf) Read(p []byte) (n int, err error) {
	if mb.rbuf == mb.wbuf && mb.rp == mb.wp {
		return
	}

	if mb.rp == cap(mb.buffers[mb.rbuf]) {
		mb.rbuf++
		mb.rp = 0
	}

	if mb.rbuf == len(mb.buffers) {
		err = io.EOF
		return
	} else if mb.rbuf > len(mb.buffers) {
		panic("mb.cur > len(mb.buffers)")
	}

	n = copy(p, mb.buffers[mb.rbuf][mb.rp:])
	mb.rp += n

	return
}

func (mb *MBuf) WriteFrom(r io.Reader) (n int, err error) {
	b := mb.buffers[mb.wbuf]

	if mb.wp == cap(b) {
		if mb.wbuf+1 == len(mb.buffers) {
			return
		}
		mb.wbuf++
		b = mb.buffers[mb.wbuf]
		mb.wp = 0
	} else if mb.wp > cap(b) {
		panic("mb.wp > cap(b)")
	}

	n, err = r.Read(b[mb.wp:cap(b)])
	mb.wp += n
	// resize the buffer to account for what we just read
	mb.buffers[mb.wbuf] = mb.buffers[mb.wbuf][:mb.wp]

	return
}

func (mb *MBuf) Free() {
	for _, b := range mb.buffers {
		mb.pool.Free(b)
	}
}

var bufferLog = GetLogger("buffer")

type Buffer struct {
	mu   sync.Mutex
	cond *sync.Cond

	buf    *MBuf
	rp     int
	wp     int
	reader io.ReadCloser
	err    error
}

type ReaderProvider func() (io.ReadCloser, error)

func (b Buffer) Init(buf *MBuf, r ReaderProvider) *Buffer {
	b.buf = buf
	b.cond = sync.NewCond(&b.mu)

	go func() {
		b.mu.Lock()
		defer b.mu.Unlock()

		b.readLoop(r)
	}()

	return &b
}

func (b *Buffer) readLoop(r ReaderProvider) {
	for {
		if b.reader == nil {
			b.reader, b.err = r()
			b.cond.Broadcast()
			if b.err != nil {
				break
			}
		}

		if b.buf == nil {
			break
		}

		nread, err := b.buf.WriteFrom(b.reader)
		if err != nil {
			b.err = err
			break
		}

		if nread == 0 {
			b.reader.Close()
			break
		}
	}
}

func (b *Buffer) Read(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for b.reader == nil && b.err == nil {
		bufferLog.Debugf("waiting for stream")
		b.cond.Wait()
	}

	if b.buf != nil {
		bufferLog.Debugf("reading %v from buffer", len(p))

		n, err = b.buf.Read(p)
		if n == 0 {
			b.buf.Free()
			b.buf = nil
		}
	} else if b.err != nil {
		err = b.err
	} else {
		bufferLog.Debugf("reading %v from stream", len(p))

		n, err = b.reader.Read(p)
		if err == io.ErrUnexpectedEOF {
			err = nil
		}
	}

	return
}

func (b *Buffer) Close() (err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.wp = -1

	if b.reader != nil {
		err = b.reader.Close()
	}

	if b.buf != nil {
		b.buf.Free()
	}

	return
}
