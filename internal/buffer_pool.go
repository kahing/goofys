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
	"io"
	"runtime"
	"runtime/debug"
	"sync"

	"github.com/jacobsa/fuse"
	"github.com/shirou/gopsutil/mem"
)

type BufferPool struct {
	mu   sync.Mutex
	cond *sync.Cond

	numBuffers uint64
	maxBuffers uint64

	totalBuffers       uint64
	computedMaxbuffers uint64

	pool *sync.Pool
}

const BUF_SIZE = 5 * 1024 * 1024

func maxMemToUse(buffersNow uint64) uint64 {
	m, err := mem.VirtualMemory()
	if err != nil {
		panic(err)
	}

	log.Debugf("amount of available memory: %v", m.Available/1024/1024)

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	log.Debugf("amount of allocated memory: %v %v", ms.Sys/1024/1024, ms.Alloc/1024/1024)

	max := uint64(m.Available+ms.Sys) / 2
	maxbuffers := MaxUInt64(max/BUF_SIZE, 1)
	log.Debugf("using up to %v %vMB buffers, now is %v", maxbuffers, BUF_SIZE/1024/1024, buffersNow)
	return maxbuffers
}

func rounduUp(size uint64, pageSize int) int {
	return pages(size, pageSize) * pageSize
}

func pages(size uint64, pageSize int) int {
	return int((size + uint64(pageSize) - 1) / uint64(pageSize))
}

func (pool BufferPool) Init() *BufferPool {
	pool.cond = sync.NewCond(&pool.mu)

	pool.computedMaxbuffers = pool.maxBuffers
	pool.pool = &sync.Pool{New: func() interface{} {
		return make([]byte, 0, BUF_SIZE)
	}}

	return &pool
}

// for testing
func NewBufferPool(maxSizeGlobal uint64) *BufferPool {
	pool := BufferPool{maxBuffers: maxSizeGlobal / BUF_SIZE}.Init()
	return pool
}

func (pool *BufferPool) RequestBuffer() (buf []byte) {
	return pool.RequestMultiple(BUF_SIZE, true)[0]
}

func (pool *BufferPool) recomputeBufferLimit() {
	if pool.maxBuffers == 0 {
		pool.computedMaxbuffers = maxMemToUse(pool.numBuffers)
		if pool.computedMaxbuffers == 0 {
			panic("OOM")
		}
	}
}

func (pool *BufferPool) RequestMultiple(size uint64, block bool) (buffers [][]byte) {
	nPages := pages(size, BUF_SIZE)

	pool.mu.Lock()
	defer pool.mu.Unlock()

	if pool.totalBuffers%10 == 0 {
		pool.recomputeBufferLimit()
	}

	for pool.numBuffers+uint64(nPages) > pool.computedMaxbuffers {
		if block {
			if pool.numBuffers == 0 {
				pool.MaybeGC()
				pool.recomputeBufferLimit()
				if pool.numBuffers+uint64(nPages) > pool.computedMaxbuffers {
					// we don't have any in use buffers, and we've made attempts to
					// free memory AND correct our limits, yet we still can't allocate.
					// it's likely that we are simply asking for too much
					log.Errorf("Unable to allocate %d bytes, limit is %d bytes",
						nPages*BUF_SIZE, pool.computedMaxbuffers*BUF_SIZE)
					panic("OOM")
				}
			}
			pool.cond.Wait()
		} else {
			return
		}
	}

	for i := 0; i < nPages; i++ {
		pool.numBuffers++
		pool.totalBuffers++
		buf := pool.pool.Get()
		buffers = append(buffers, buf.([]byte))
	}
	return
}

func (pool *BufferPool) MaybeGC() {
	if pool.numBuffers == 0 {
		debug.FreeOSMemory()
	}
}

func (pool *BufferPool) Free(buf []byte) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	buf = buf[:0]
	pool.pool.Put(buf)
	pool.numBuffers--
	pool.cond.Signal()
}

var mbufLog = GetLogger("mbuf")

type MBuf struct {
	pool    *BufferPool
	buffers [][]byte
	rbuf    int
	wbuf    int
	rp      int
	wp      int
}

func (mb MBuf) Init(h *BufferPool, size uint64, block bool) *MBuf {
	mb.pool = h

	if size != 0 {
		mb.buffers = h.RequestMultiple(size, block)
		if mb.buffers == nil {
			return nil
		}
	}

	return &mb
}

// seek only seeks the reader
func (mb *MBuf) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0: // relative to beginning
		if offset == 0 {
			mb.rbuf = 0
			mb.rp = 0
			return 0, nil
		}
	case 1: // relative to current position
		if offset == 0 {
			for i := 0; i < mb.rbuf; i++ {
				offset += int64(len(mb.buffers[i]))
			}
			offset += int64(mb.rp)
			return offset, nil
		}

	case 2: // relative to the end
		if offset == 0 {
			for i := 0; i < len(mb.buffers); i++ {
				offset += int64(len(mb.buffers[i]))
			}
			mb.rbuf = len(mb.buffers)
			mb.rp = 0
			return offset, nil
		}
	}

	log.Errorf("Seek %d %d", offset, whence)
	panic(fuse.EINVAL)

	return 0, fuse.EINVAL
}

func (mb *MBuf) Read(p []byte) (n int, err error) {
	if mb.rbuf == mb.wbuf && mb.rp == mb.wp {
		err = io.EOF
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

func (mb *MBuf) Full() bool {
	return mb.buffers == nil || (mb.wp == cap(mb.buffers[mb.wbuf]) && mb.wbuf+1 == len(mb.buffers))
}

func (mb *MBuf) Write(p []byte) (n int, err error) {
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

	n = copy(b[mb.wp:cap(b)], p)
	mb.wp += n
	// resize the buffer to account for what we just read
	mb.buffers[mb.wbuf] = mb.buffers[mb.wbuf][:mb.wp]

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

	mb.buffers = nil
}

var bufferLog = GetLogger("buffer")

type Buffer struct {
	mu   sync.Mutex
	cond *sync.Cond

	buf    *MBuf
	reader io.ReadCloser
	err    error
}

type ReaderProvider func() (io.ReadCloser, error)

func (b Buffer) Init(buf *MBuf, r ReaderProvider) *Buffer {
	b.buf = buf
	b.cond = sync.NewCond(&b.mu)

	go func() {
		b.readLoop(r)
	}()

	return &b
}

func (b *Buffer) readLoop(r ReaderProvider) {
	for {
		b.mu.Lock()
		if b.reader == nil {
			b.reader, b.err = r()
			b.cond.Broadcast()
			if b.err != nil {
				b.mu.Unlock()
				break
			}
		}

		if b.buf == nil {
			// buffer was drained
			b.mu.Unlock()
			break
		}

		nread, err := b.buf.WriteFrom(b.reader)
		if err != nil {
			b.err = err
			b.mu.Unlock()
			break
		}
		bufferLog.Debugf("wrote %v into buffer", nread)

		if nread == 0 {
			b.reader.Close()
			b.mu.Unlock()
			break
		}

		b.mu.Unlock()
		// if we get here we've read _something_, bounce this goroutine
		// to allow another one to read
		runtime.Gosched()
	}
	bufferLog.Debugf("<-- readLoop()")
}

func (b *Buffer) readFromStream(p []byte) (n int, err error) {
	bufferLog.Debugf("reading %v from stream", len(p))

	n, err = b.reader.Read(p)
	if n != 0 && err == io.ErrUnexpectedEOF {
		err = nil
	} else {
		bufferLog.Debugf("read %v from stream", n)
	}
	return
}

func (b *Buffer) Read(p []byte) (n int, err error) {
	bufferLog.Debugf("Buffer.Read(%v)", len(p))

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
			bufferLog.Debugf("drained buffer")
			n, err = b.readFromStream(p)
		} else {
			bufferLog.Debugf("read %v from buffer", n)
		}
	} else if b.err != nil {
		err = b.err
	} else {
		n, err = b.readFromStream(p)
	}

	return
}

func (b *Buffer) Close() (err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.reader != nil {
		err = b.reader.Close()
	}

	if b.buf != nil {
		b.buf.Free()
		b.buf = nil
	}

	return
}
