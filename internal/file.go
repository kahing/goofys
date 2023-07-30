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
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
)

type FileHandle struct {
	inode *Inode
	cloud StorageBackend
	key   string

	mpuName   *string
	dirty     bool
	writeInit sync.Once
	mpuWG     sync.WaitGroup

	mu              sync.Mutex
	mpuId           *MultipartBlobCommitInput
	nextWriteOffset int64
	lastPartId      uint32

	poolHandle *BufferPool
	buf        *MBuf

	lastWriteError error

	// read
	reader        io.ReadCloser
	readBufOffset int64

	// parallel read
	buffers           []*S3ReadBuffer
	existingReadahead int
	seqReadAmount     uint64
	numOOORead        uint64 // number of out of order read
	// User space PID. All threads created by a process will have the same TGID,
	// but different PIDs[1].
	// This value can be nil if we fail to get TGID from PID[2].
	// [1] : https://godoc.org/github.com/shirou/gopsutil/process#Process.Tgid
	// [2] : https://github.com/shirou/gopsutil#process-class
	Tgid *int32

	keepPageCache bool // the same value we returned to OpenFile
}

const MAX_READAHEAD = uint32(400 * 1024 * 1024)
const READAHEAD_CHUNK = uint32(20 * 1024 * 1024)

// NewFileHandle returns a new file handle for the given `inode` triggered by fuse
// operation with the given `opMetadata`
func NewFileHandle(inode *Inode, opMetadata fuseops.OpContext) *FileHandle {
	tgid, err := GetTgid(opMetadata.Pid)
	if err != nil {
		log.Debugf(
			"Failed to retrieve tgid for the given pid. pid: %v err: %v inode id: %v err: %v",
			opMetadata.Pid, err, inode.Id, err)
	}
	fh := &FileHandle{inode: inode, Tgid: tgid}
	fh.cloud, fh.key = inode.cloud()
	return fh
}

func (fh *FileHandle) initWrite() {
	fh.writeInit.Do(func() {
		fh.mpuWG.Add(1)
		go fh.initMPU()
	})
}

func (fh *FileHandle) initMPU() {
	defer func() {
		fh.mpuWG.Done()
	}()

	fs := fh.inode.fs
	fh.mpuName = &fh.key

	resp, err := fh.cloud.MultipartBlobBegin(&MultipartBlobBeginInput{
		Key:         *fh.mpuName,
		ContentType: fs.flags.GetMimeType(*fh.mpuName),
	})

	fh.mu.Lock()
	defer fh.mu.Unlock()

	if err != nil {
		fh.lastWriteError = mapAwsError(err)
	} else {
		fh.mpuId = resp
	}

	return
}

func (fh *FileHandle) mpuPartNoSpawn(buf *MBuf, part uint32, total int64, last bool) (err error) {
	fs := fh.inode.fs

	fs.replicators.Take(1, true)
	defer fs.replicators.Return(1)

	if part == 0 || part > 10000 {
		return errors.New(fmt.Sprintf("invalid part number: %v", part))
	}

	mpu := MultipartBlobAddInput{
		Commit:     fh.mpuId,
		PartNumber: part,
		Body:       buf,
		Size:       uint64(buf.Len()),
		Last:       last,
		Offset:     uint64(total - int64(buf.Len())),
	}

	defer func() {
		if mpu.Body != nil {
			bufferLog.Debugf("Free %T", buf)
			buf.Free()
		}
	}()

	_, err = fh.cloud.MultipartBlobAdd(&mpu)

	return
}

func (fh *FileHandle) mpuPart(buf *MBuf, part uint32, total int64) {
	defer func() {
		fh.mpuWG.Done()
	}()

	// maybe wait for CreateMultipartUpload
	if fh.mpuId == nil {
		fh.mpuWG.Wait()
		// initMPU might have errored
		if fh.mpuId == nil {
			return
		}
	}

	err := fh.mpuPartNoSpawn(buf, part, total, false)
	if err != nil {
		if fh.lastWriteError == nil {
			fh.lastWriteError = err
		}
	}
}

func (fh *FileHandle) waitForCreateMPU() (err error) {
	if fh.mpuId == nil {
		fh.mu.Unlock()
		fh.initWrite()
		fh.mpuWG.Wait() // wait for initMPU
		fh.mu.Lock()

		if fh.lastWriteError != nil {
			return fh.lastWriteError
		}
	}

	return
}

func (fh *FileHandle) partSize() uint64 {
	var size uint64

	if fh.lastPartId < 500 {
		size = 5 * 1024 * 1024
	} else if fh.lastPartId < 1000 {
		size = 25 * 1024 * 1024
	} else if fh.lastPartId < 2000 {
		size = 125 * 1024 * 1024
	} else {
		size = 625 * 1024 * 1024
	}

	maxPartSize := fh.cloud.Capabilities().MaxMultipartSize
	if maxPartSize != 0 {
		size = MinUInt64(maxPartSize, size)
	}
	return size
}

func (fh *FileHandle) uploadCurrentBuf(parallel bool) (err error) {
	err = fh.waitForCreateMPU()
	if err != nil {
		return
	}

	fh.lastPartId++
	part := fh.lastPartId
	buf := fh.buf
	fh.buf = nil

	if parallel {
		fh.mpuWG.Add(1)
		go fh.mpuPart(buf, part, fh.nextWriteOffset)
	} else {
		err = fh.mpuPartNoSpawn(buf, part, fh.nextWriteOffset, false)
		if fh.lastWriteError == nil {
			fh.lastWriteError = err
		}
	}

	return
}

func (fh *FileHandle) WriteFile(offset int64, data []byte) (err error) {
	fh.inode.logFuse("WriteFile", offset, len(data))

	fh.mu.Lock()
	defer fh.mu.Unlock()

	if fh.lastWriteError != nil {
		fh.inode.mu.Lock()
		// our write failed, next time we open we should not
		// use page cache so we will read from cloud again
		fh.inode.invalidateCache = true
		fh.inode.mu.Unlock()
		return fh.lastWriteError
	}

	if offset < fh.nextWriteOffset {
		fh.inode.errFuse("WriteFile: only sequential writes supported", fh.nextWriteOffset, offset)
		fh.lastWriteError = syscall.ENOTSUP
		return fh.lastWriteError
	}

	if offset == 0 {
		fh.poolHandle = fh.inode.fs.bufferPool
		fh.dirty = true
		fh.inode.mu.Lock()
		// we are updating this file, set knownETag to nil so
		// on next lookup we won't think it's changed, to
		// always prefer to read back our own write. We set
		// this back to the ETag at flush time
		//
		// XXX this doesn't actually work, see the notes in
		// Goofys.OpenFile about KeepPageCache
		fh.inode.knownETag = nil
		fh.inode.invalidateCache = false
		fh.inode.mu.Unlock()
	}

	// fill the hole with zero
	if offset > fh.nextWriteOffset {
		toSkip := offset - fh.nextWriteOffset
		n := 10240
		data := make([]byte, n)

		for {
			if fh.buf == nil {
				fh.buf = MBuf{}.Init(fh.poolHandle, fh.partSize(), true)
			}

			if toSkip < int64(n) {
				n = int(toSkip)
			}
			nCopied, _ := fh.buf.Write(data[:n])
			toSkip -= int64(nCopied)
			fh.nextWriteOffset += int64(nCopied)

			if fh.buf.Full() {
				err = fh.uploadCurrentBuf(!fh.cloud.Capabilities().NoParallelMultipart)
				if err != nil {
					return
				}
			}

			if toSkip == 0 {
				break
			}
		}
	}

	for {
		if fh.buf == nil {
			fh.buf = MBuf{}.Init(fh.poolHandle, fh.partSize(), true)
		}

		nCopied, _ := fh.buf.Write(data)
		fh.nextWriteOffset += int64(nCopied)

		if fh.buf.Full() {
			err = fh.uploadCurrentBuf(!fh.cloud.Capabilities().NoParallelMultipart)
			if err != nil {
				return
			}
		}

		if nCopied == len(data) {
			break
		}

		data = data[nCopied:]
	}

	fh.inode.Attributes.Size = uint64(fh.nextWriteOffset)
	fh.inode.Attributes.Mtime = time.Now()

	return
}

type S3ReadBuffer struct {
	s3          StorageBackend
	startOffset uint64
	nRetries    uint8
	mbuf        *MBuf

	offset uint64
	size   uint32
	buf    *Buffer
}

func (b S3ReadBuffer) Init(fh *FileHandle, offset uint64, size uint32) *S3ReadBuffer {
	b.s3 = fh.cloud
	b.offset = offset
	b.startOffset = offset
	b.size = size
	b.nRetries = 3

	b.mbuf = MBuf{}.Init(fh.poolHandle, uint64(size), false)
	if b.mbuf == nil {
		return nil
	}

	b.initBuffer(fh, offset, size)
	return &b
}

func (b *S3ReadBuffer) initBuffer(fh *FileHandle, offset uint64, size uint32) {
	getFunc := func() (io.ReadCloser, error) {
		resp, err := b.s3.GetBlob(&GetBlobInput{
			Key:   fh.key,
			Start: offset,
			Count: uint64(size),
		})
		if err != nil {
			return nil, err
		}

		return resp.Body, nil
	}

	if b.buf == nil {
		b.buf = Buffer{}.Init(b.mbuf, getFunc)
	} else {
		b.buf.ReInit(getFunc)
	}
}

func (b *S3ReadBuffer) Read(offset uint64, p []byte) (n int, err error) {
	if b.offset == offset {
		n, err = io.ReadFull(b.buf, p)
		if n != 0 && err == io.ErrUnexpectedEOF {
			err = nil
		}
		if n > 0 {
			if uint32(n) > b.size {
				panic(fmt.Sprintf("read more than available %v %v", n, b.size))
			}

			b.offset += uint64(n)
			b.size -= uint32(n)
		}
		if b.size == 0 && err != nil {
			// we've read everything, sometimes we may
			// request for more bytes then there's left in
			// this chunk so we could get an error back,
			// ex: http2: response body closed this
			// doesn't tend to happen because our chunks
			// are aligned to 4K and also 128K (except for
			// the last chunk, but seems kernel requests
			// for a smaller buffer for the last chunk)
			err = nil
		}

		return
	} else {
		panic(fmt.Sprintf("not the right buffer, expecting %v got %v, %v left", b.offset, offset, b.size))
		err = errors.New(fmt.Sprintf("not the right buffer, expecting %v got %v", b.offset, offset))
		return
	}
}

func (fh *FileHandle) readFromReadAhead(offset uint64, buf []byte) (bytesRead int, err error) {
	var nread int
	for len(fh.buffers) != 0 {
		readAheadBuf := fh.buffers[0]

		nread, err = readAheadBuf.Read(offset+uint64(bytesRead), buf)
		bytesRead += nread
		if err != nil {
			if err == io.EOF && readAheadBuf.size != 0 {
				// in case we hit
				// https://github.com/kahing/goofys/issues/464
				// again, this will convert that into
				// an error
				fuseLog.Errorf("got EOF when data remains: %v", *fh.inode.FullName())
				err = io.ErrUnexpectedEOF
			} else if err != io.EOF && readAheadBuf.size > 0 {
				// we hit some other errors when
				// reading from this part. If we can
				// retry, do that
				if readAheadBuf.nRetries > 0 {
					readAheadBuf.nRetries -= 1
					readAheadBuf.initBuffer(fh, readAheadBuf.offset, readAheadBuf.size)
					// we unset error and return,
					// so upper layer will retry
					// this read
					err = nil
				}
			}
			return
		}

		if readAheadBuf.size == 0 {
			// we've exhausted the first buffer
			readAheadBuf.buf.Close()
			fh.buffers = fh.buffers[1:]
		}

		buf = buf[nread:]

		if len(buf) == 0 {
			// we've filled the user buffer
			return
		}
	}

	return
}

func (fh *FileHandle) readAhead(offset uint64, needAtLeast int) (err error) {
	existingReadahead := uint32(0)
	for _, b := range fh.buffers {
		existingReadahead += b.size
	}

	readAheadAmount := MAX_READAHEAD

	for readAheadAmount-existingReadahead >= READAHEAD_CHUNK {
		off := offset + uint64(existingReadahead)
		remaining := fh.inode.Attributes.Size - off

		// only read up to readahead chunk each time
		size := MinUInt32(readAheadAmount-existingReadahead, READAHEAD_CHUNK)
		// but don't read past the file
		size = uint32(MinUInt64(uint64(size), remaining))

		if size != 0 {
			fh.inode.logFuse("readahead", off, size, existingReadahead)

			readAheadBuf := S3ReadBuffer{}.Init(fh, off, size)
			if readAheadBuf != nil {
				fh.buffers = append(fh.buffers, readAheadBuf)
				existingReadahead += size
			} else {
				if existingReadahead != 0 {
					// don't do more readahead now, but don't fail, cross our
					// fingers that we will be able to allocate the buffers
					// later
					return nil
				} else {
					return syscall.ENOMEM
				}
			}
		}

		if size != READAHEAD_CHUNK {
			// that was the last remaining chunk to readahead
			break
		}
	}

	return nil
}

func (fh *FileHandle) ReadFile(offset int64, buf []byte) (bytesRead int, err error) {
	fh.inode.logFuse("ReadFile", offset, len(buf))
	defer func() {
		fh.inode.logFuse("< ReadFile", bytesRead, err)

		if err != nil {
			if err == io.EOF {
				err = nil
			}
		}
	}()

	fh.mu.Lock()
	defer fh.mu.Unlock()

	nwant := len(buf)
	var nread int

	for bytesRead < nwant && err == nil {
		nread, err = fh.readFile(offset+int64(bytesRead), buf[bytesRead:])
		if nread > 0 {
			bytesRead += nread
		}
	}

	return
}

func (fh *FileHandle) readFile(offset int64, buf []byte) (bytesRead int, err error) {
	defer func() {
		if bytesRead > 0 {
			fh.readBufOffset += int64(bytesRead)
			fh.seqReadAmount += uint64(bytesRead)
		}

		fh.inode.logFuse("< readFile", bytesRead, err)
	}()

	if uint64(offset) >= fh.inode.Attributes.Size {
		// nothing to read
		if fh.inode.Invalid {
			err = fuse.ENOENT
		} else if fh.inode.KnownSize == nil {
			err = io.EOF
		} else {
			err = io.EOF
		}
		return
	}

	fs := fh.inode.fs

	if fh.poolHandle == nil {
		fh.poolHandle = fs.bufferPool
	}

	if fh.readBufOffset != offset {
		// XXX out of order read, maybe disable prefetching
		fh.inode.logFuse("out of order read", offset, fh.readBufOffset)

		fh.readBufOffset = offset
		fh.seqReadAmount = 0
		if fh.reader != nil {
			fh.reader.Close()
			fh.reader = nil
		}

		if fh.buffers != nil {
			// we misdetected
			fh.numOOORead++
		}

		for _, b := range fh.buffers {
			b.buf.Close()
		}
		fh.buffers = nil
	}

	if !fs.flags.Cheap && fh.seqReadAmount >= uint64(READAHEAD_CHUNK) && fh.numOOORead < 3 {
		if fh.reader != nil {
			fh.inode.logFuse("cutover to the parallel algorithm")
			fh.reader.Close()
			fh.reader = nil
		}

		err = fh.readAhead(uint64(offset), len(buf))
		if err == nil {
			bytesRead, err = fh.readFromReadAhead(uint64(offset), buf)
			return
		} else {
			// fall back to read serially
			fh.inode.logFuse("not enough memory, fallback to serial read")
			fh.seqReadAmount = 0
			for _, b := range fh.buffers {
				b.buf.Close()
			}
			fh.buffers = nil
		}
	}

	bytesRead, err = fh.readFromStream(offset, buf)

	return
}

func (fh *FileHandle) Release() {
	// read buffers
	for _, b := range fh.buffers {
		b.buf.Close()
	}
	fh.buffers = nil

	if fh.reader != nil {
		fh.reader.Close()
	}

	// write buffers
	if fh.poolHandle != nil {
		if fh.buf != nil && fh.buf.buffers != nil {
			if fh.lastWriteError == nil {
				panic("buf not freed but error is nil")
			}

			fh.buf.Free()
			// the other in-flight multipart PUT buffers will be
			// freed when they finish/error out
		}
	}

	fh.inode.mu.Lock()
	defer fh.inode.mu.Unlock()

	if atomic.AddInt32(&fh.inode.fileHandles, -1) == -1 {
		panic(fh.inode.fileHandles)
	}
}

func (fh *FileHandle) readFromStream(offset int64, buf []byte) (bytesRead int, err error) {
	defer func() {
		if fh.inode.fs.flags.DebugFuse {
			fh.inode.logFuse("< readFromStream", bytesRead)
		}
	}()

	if uint64(offset) >= fh.inode.Attributes.Size {
		// nothing to read
		return
	}

	if fh.reader == nil {
		resp, err := fh.cloud.GetBlob(&GetBlobInput{
			Key:   fh.key,
			Start: uint64(offset),
		})
		if err != nil {
			return bytesRead, err
		}

		fh.reader = resp.Body
	}

	bytesRead, err = fh.reader.Read(buf)
	if err != nil {
		if err != io.EOF {
			fh.inode.logFuse("< readFromStream error", bytesRead, err)
		}
		// always retry error on read
		fh.reader.Close()
		fh.reader = nil
		err = nil
	}

	return
}

func (fh *FileHandle) flushSmallFile() (err error) {
	buf := fh.buf
	fh.buf = nil

	if buf == nil {
		buf = MBuf{}.Init(fh.poolHandle, 0, true)
	}

	defer buf.Free()

	fs := fh.inode.fs

	fs.replicators.Take(1, true)
	defer fs.replicators.Return(1)

	// we want to get key from inode because the file could have been renamed
	_, key := fh.inode.cloud()
	resp, err := fh.cloud.PutBlob(&PutBlobInput{
		Key:         key,
		Body:        buf,
		Size:        PUInt64(uint64(buf.Len())),
		ContentType: fs.flags.GetMimeType(*fh.inode.FullName()),
	})
	if err != nil {
		fh.lastWriteError = err
	} else {
		fh.updateFromFlush(resp.ETag, resp.LastModified, resp.StorageClass)
	}
	return
}

// LOCKS_EXCLUDED(fh.inode.mu)
func (fh *FileHandle) updateFromFlush(etag *string, lastModified *time.Time, storageClass *string) {
	inode := fh.inode
	inode.mu.Lock()
	defer inode.mu.Unlock()

	if etag != nil {
		inode.s3Metadata["etag"] = []byte(*etag)
	}
	if storageClass != nil {
		inode.s3Metadata["storage-class"] = []byte(*storageClass)
	}
	if fh.keepPageCache {
		// if this write didn't update page cache, don't try
		// to update these values so on next lookup, we would
		// invalidate the cache. We want to do that because
		// our cache could have been populated by subsequent
		// reads
		if lastModified != nil {
			inode.Attributes.Mtime = *lastModified
		}
		inode.knownETag = etag
	}
}

func (fh *FileHandle) resetToKnownSize() {
	if fh.inode.KnownSize != nil {
		fh.inode.Attributes.Size = *fh.inode.KnownSize
	} else {
		fh.inode.Attributes.Size = 0
		fh.inode.Invalid = true
	}
}

func (fh *FileHandle) FlushFile() (err error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	fh.inode.logFuse("FlushFile")

	if !fh.dirty || fh.lastWriteError != nil {
		if fh.lastWriteError != nil {
			err = fh.lastWriteError
			fh.resetToKnownSize()
		}
		return
	}

	if fh.inode.Parent == nil {
		// the file is deleted
		if fh.mpuId != nil {
			go func() {
				_, _ = fh.cloud.MultipartBlobAbort(fh.mpuId)
				fh.mpuId = nil
			}()
		}
		return
	}

	fs := fh.inode.fs

	// abort mpu on error
	defer func() {
		if err != nil {
			if fh.mpuId != nil {
				go func() {
					_, _ = fh.cloud.MultipartBlobAbort(fh.mpuId)
					fh.mpuId = nil
				}()
			}

			fh.resetToKnownSize()
		} else {
			if fh.dirty {
				// don't unset this if we never actually flushed
				size := fh.inode.Attributes.Size
				fh.inode.KnownSize = &size
				fh.inode.Invalid = false
			}
			fh.dirty = false
		}

		fh.writeInit = sync.Once{}
		fh.nextWriteOffset = 0
		fh.lastPartId = 0
	}()

	if fh.lastPartId == 0 {
		return fh.flushSmallFile()
	}

	fh.mpuWG.Wait()

	if fh.lastWriteError != nil {
		return fh.lastWriteError
	}

	if fh.mpuId == nil {
		return
	}

	nParts := fh.lastPartId
	if fh.buf != nil {
		// upload last part
		nParts++
		err = fh.mpuPartNoSpawn(fh.buf, nParts, fh.nextWriteOffset, true)
		if err != nil {
			return
		}
		fh.buf = nil
	}

	resp, err := fh.cloud.MultipartBlobCommit(fh.mpuId)
	if err != nil {
		return
	}

	fh.updateFromFlush(resp.ETag, resp.LastModified, resp.StorageClass)

	fh.mpuId = nil

	// we want to get key from inode because the file could have been renamed
	_, key := fh.inode.cloud()
	if *fh.mpuName != key {
		// the file was renamed
		err = fh.inode.renameObject(fs, PUInt64(uint64(fh.nextWriteOffset)), *fh.mpuName, *fh.inode.FullName())
	}

	return
}
