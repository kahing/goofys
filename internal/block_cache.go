package internal

import (
	. "github.com/kahing/goofys/api/common"

	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/golang-lru/v2"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/pbnjay/memory"
)

// file smaller than this will be read at once
const BLOCK_CACHE_READ_ALL_SIZE = 20 * 1024 * 1024 // 20MiB

// maximum number of threads working on prefetch or readahead
// note: prefetch is to utilize latency and match throughput; readahead is to
// aggressively read future reads
const MAX_PREFETCH_THREADS = int32(16)

type cacheKey struct {
	inode fuseops.InodeID
	blkId uint64 // ID of the block (offset div by BLOCK_SIZE)
}

type CacheValue struct {
	mu   sync.RWMutex
	Data []byte // nil if error is present
	Err  error  // should be checked before trying to access the value
}

// cache for read-only files based on large blocks
type BlockCache struct {
	mu        sync.RWMutex
	lru       *lru.Cache[cacheKey, *CacheValue]
	blockSize uint64

	// number of alive prefetch threads
	numPrefetchThreads int32

	nrLatencyBlock    int // max blocks that can be read without more latency
	nrThroughputBlock int // min blocks that can be read with max throughput
	benchmarkDone     sync.WaitGroup
	benchmarkOnce     sync.Once
}

// result of reading the cache
type cacheReadResult struct {
	blkId uint64      // block ID
	block *CacheValue // the cache block; guaranteed to be non-nil

	// whether this block is newly created; if it is true, write lock of block
	// is acquired
	isNewAlloc bool
}

func NewBlockCache(flags *FlagStorage) *BlockCache {
	size := int(float64(memory.TotalMemory()/flags.BlockReadCacheSize) *
		flags.BlockReadCacheMemRatio)
	size = MaxInt(size,
		(BLOCK_CACHE_READ_ALL_SIZE*2-1)/int(flags.BlockReadCacheSize)+1)
	cache, err := lru.New[cacheKey, *CacheValue](size)
	if err != nil {
		log.Panic("failed to allocate LRU cache", err)
	}
	log.Infof("block cache: size=%d mem=%.2fGiB", size,
		float64(size)*float64(flags.BlockReadCacheSize)/1024/1024/1024)
	ret := &BlockCache{
		lru:       cache,
		blockSize: flags.BlockReadCacheSize,
	}
	ret.benchmarkDone.Add(1)
	return ret
}

// Remove cache entries for a specific inode
func (bc *BlockCache) RemoveCache(inode *Inode) {
	var keys []cacheKey
	var key cacheKey
	bc.mu.Lock()
	defer bc.mu.Unlock()

	for _, key = range bc.lru.Keys() {
		if key.inode == inode.Id {
			keys = append(keys, key)
		}
	}

	for _, key = range keys {
		bc.lru.Remove(key)
	}
}

// Get a cache node containing given offset (which must be within the file
// size); this function requests the remote header if cache is unavailable yet.
// Remember to check the error marker before accessing the data.
// If aggressive_prefetch is true, will try to prefetch further blocks.
func (bc *BlockCache) Get(fh *FileHandle, offset uint64) (
	cacheBlock *CacheValue, cacheOffset uint64) {

	fileSize := fh.inode.Attributes.Size
	assert(offset < fileSize, "invalid Get")

	blkId := offset / bc.blockSize
	cacheOffset = blkId * bc.blockSize

	headRead := bc.getOrAllocate(fh.inode, blkId)
	cacheBlock = headRead.block

	if headRead.isNewAlloc {
		var blocks []cacheReadResult

		if fileSize <= BLOCK_CACHE_READ_ALL_SIZE {
			numBlocks := int(
				DivUpUint64(fileSize, bc.blockSize) - headRead.blkId)
			blocks = make([]cacheReadResult, 0, numBlocks)
			blocks = append(blocks, headRead)
			blocks = append(blocks,
				bc.getOrAllocateMany(fh.inode, blkId+1, numBlocks-1)...)
		} else {
			bc.ensureBenchmark(fh)
			blocks = bc.getReadAheadBlocks(fh, headRead)
		}

		assert(len(blocks) > 0, "invalid read ahead blocks")
		if len(blocks) == 1 {
			bc.readMultipleBlocks(fh, blocks, false)
		} else {
			go bc.readMultipleBlocks(
				fh, blocks,
				fileSize > BLOCK_CACHE_READ_ALL_SIZE &&
					len(blocks) > bc.nrLatencyBlock)
		}
	}

	cacheBlock.mu.RLock() // wait the writer to finish
	cacheBlock.mu.RUnlock()
	assert((cacheBlock.Err == nil) == (cacheBlock.Data != nil),
		"block not read")
	return
}

// Start a thread to do read ahead at given location. Return false if prefetch
// threads are exhausted, and true otherwise.
func (bc *BlockCache) StartReadAhead(
	fh *FileHandle, offset uint64, size uint64) bool {

	assert(size > 0 && offset < fh.inode.Attributes.Size, "invalid readahead")

	if !bc.acquirePrefetchThread() {
		return false
	}

	head := bc.getOrAllocate(fh.inode, offset/bc.blockSize)
	if !head.isNewAlloc {
		// already cached (probably being worked on by existing read ahead), do
		// not try to read again
		bc.releasePrefetchThread()
		return true
	}

	go func() {
		nrBlock := int(DivUpUint64(
			MinUInt64(fh.inode.Attributes.Size, offset+size),
			bc.blockSize) - head.blkId)
		assert(nrBlock > 0, "invalid nrBlock")
		blocks := make([]cacheReadResult, 0, nrBlock)
		blocks = append(blocks, head)
		blocks = append(blocks,
			bc.getOrAllocateMany(fh.inode, head.blkId+1, nrBlock-1)...)
		bc.readMultipleBlocks(fh, blocks, true)
	}()
	return true
}

// Read into consicutive blocks.
// Write locks will be released after the read is finished.
// If ownPrefetchThread is true, the prefetch thread will be released
func (bc *BlockCache) readMultipleBlocks(
	fh *FileHandle, blocks []cacheReadResult, ownPrefetchThread bool) {

	assert(len(blocks) > 0 && blocks[0].isNewAlloc,
		"empty blocks for readMultipleBlocks")

	for i := 0; i < len(blocks); i++ {
		assert(blocks[i].blkId-blocks[0].blkId == uint64(i),
			"block not consecutive")
	}

	// remove trailing blocks that are already prepared
	for !blocks[len(blocks)-1].isNewAlloc {
		blocks = blocks[:len(blocks)-1]
	}

	totSize := MinUInt64(
		bc.blockSize*uint64(len(blocks)),
		fh.inode.Attributes.Size-blocks[0].blkId*bc.blockSize)
	resp, err := fh.cloud.GetBlob(&GetBlobInput{
		Key:   fh.key,
		Start: blocks[0].blkId * bc.blockSize,
		Count: totSize,
	})
	finished := 0

	// release the thread, and set error markers
	defer func() {
		if ownPrefetchThread {
			bc.releasePrefetchThread()
		}

		for i := finished; i < len(blocks); i++ {
			assert(err != nil, "unhandled block read")
			if blocks[i].isNewAlloc {
				c := blocks[i].block
				c.Err = err
				c.mu.Unlock()
			}
		}
	}()

	if err != nil {
		return
	}

	reader := resp.Body
	defer reader.Close()

	remainSize := totSize

	for ; finished < len(blocks); finished++ {
		blkDone := 0
		curRead := 0
		assert(remainSize > 0, "remain_size=0: blocks exceed file size")
		bufSize := int(MinUInt64(remainSize, bc.blockSize))
		remainSize -= uint64(bufSize)
		buf := make([]byte, bufSize)
		for blkDone < bufSize {
			curRead, err = reader.Read(buf[blkDone:])
			blkDone += curRead
			if err != nil && (err != io.EOF || blkDone != bufSize) {
				// the deferred function will set error markers
				return
			}
		}
		if blocks[finished].isNewAlloc {
			c := blocks[finished].block
			c.Data = buf
			c.mu.Unlock()
		}
	}
}

// Get blocks for read ahead; blk_id is the first block requested by client
// This function also acquires a prefetch thread if return length is more than
// latency_blocks
func (bc *BlockCache) getReadAheadBlocks(
	fh *FileHandle, head cacheReadResult) (ret []cacheReadResult) {

	ret = make([]cacheReadResult, 0, bc.nrThroughputBlock)
	ret = append(ret, head)

	// max number of blocks to read
	maxBlocks := int(MinUInt64(
		DivUpUint64(fh.inode.Attributes.Size, bc.blockSize)-head.blkId,
		uint64(bc.nrThroughputBlock)))

	ret = append(ret, bc.getOrAllocateMany(fh.inode, head.blkId+1,
		MinInt(maxBlocks, bc.nrLatencyBlock)-1)...)

	if bc.nrLatencyBlock >= maxBlocks {
		return
	}

	assert(len(ret) == bc.nrLatencyBlock, "ret size does not match")

	if !bc.acquirePrefetchThread() {
		return
	}

	// read ahead to get max throughput, but stop at the first existing block
	ret = append(ret,
		bc.getOrAllocateMany2(
			fh.inode,
			head.blkId+uint64(bc.nrLatencyBlock),
			maxBlocks-bc.nrLatencyBlock,
			true)...)

	if len(ret) == bc.nrLatencyBlock {
		// no new blocks are added
		bc.releasePrefetchThread()
	}

	return
}

// try to acquire a prefetch thread; return false if the limit is reached
func (bc *BlockCache) acquirePrefetchThread() bool {
	num := atomic.AddInt32(&bc.numPrefetchThreads, 1)
	if num > MAX_PREFETCH_THREADS {
		bc.releasePrefetchThread()
		assert(num <= MAX_PREFETCH_THREADS+10,
			"num_prefetch_threads too high")
		return false
	}
	return true
}

func (bc *BlockCache) releasePrefetchThread() {
	num := atomic.AddInt32(&bc.numPrefetchThreads, -1)
	assert(num >= 0, "num_prefetch_threads < 0")
}

// Get a cache block. new_alloc indicates if this block is newly allocated. If
// it is true, the write lock is acquired, and the caller should fill in the
// data.
func (bc *BlockCache) getOrAllocate(
	inode *Inode, blkId uint64) (ret cacheReadResult) {
	return bc.getOrAllocateMany(inode, blkId, 1)[0]
}

// Get consecutive cache blocks starting at blk_id
func (bc *BlockCache) getOrAllocateMany(
	inode *Inode, blkId uint64, num int) (ret []cacheReadResult) {
	return bc.getOrAllocateMany2(inode, blkId, num, false)
}

// return immediately if requireNewAlloc is true but the block already exists
func (bc *BlockCache) getOrAllocateMany2(
	inode *Inode, blkId uint64, num int, requireNewAlloc bool) (
	ret []cacheReadResult) {

	assert(num >= 0, "invalid num")
	ret = make([]cacheReadResult, 0, num)

	if num == 0 {
		return nil
	}

	var ok bool
	var block *CacheValue

	hasBcWriteLock := false
	bc.mu.RLock()

	for i := 0; i < num; i++ {
		key := cacheKey{inode.Id, blkId + uint64(i)}
		assert(key.blkId*bc.blockSize < inode.Attributes.Size,
			"block exceeds file size")

		block, ok = bc.lru.Get(key)
		if ok {
			if requireNewAlloc {
				break
			}
			ret = append(ret, cacheReadResult{
				blkId:      key.blkId,
				block:      block,
				isNewAlloc: false,
			})
			continue
		}

		if !hasBcWriteLock {
			bc.mu.RUnlock()
			bc.mu.Lock()
			i-- // try getting again with write lock
			hasBcWriteLock = true
			continue
		}

		// create a new block now
		block = &CacheValue{}
		block.mu.Lock() // acquire cache wlock before returning
		ret = append(ret, cacheReadResult{
			blkId:      key.blkId,
			block:      block,
			isNewAlloc: true,
		})
		bc.lru.Add(key, block)
	}

	if hasBcWriteLock {
		bc.mu.Unlock()
	} else {
		bc.mu.RUnlock()
	}
	return
}

// remove a cache entry, usually used due to read error
func (bc *BlockCache) Remove(inode *Inode, offset uint64) {
	key := cacheKey{inode.Id, offset / bc.blockSize}
	bc.mu.Lock()
	bc.lru.Remove(key)
	bc.mu.Unlock()
}

func assert(cond bool, msg string) {
	if !cond {
		panic(msg)
	}
}

// Run the benchmark if it is not done; file size must be at least
// BLOCK_CACHE_FILE_SIZE_MIN
func (bc *BlockCache) ensureBenchmark(fh *FileHandle) {

	// run a single benchmark and return its execution time in seconds
	benchmark_one := func(size int) (secs float64) {
		buf := make([]byte, size)

		do := func() float64 {
			begin := time.Now()
			resp, err := fh.cloud.GetBlob(&GetBlobInput{
				Key:   fh.key,
				Start: 0,
				Count: uint64(size),
			})
			if err != nil {
				panic(fmt.Sprintf("failed to benchmark: %v", err))
			}
			reader := resp.Body
			defer reader.Close()
			done := 0
			var n int
			for done < size {
				n, err = reader.Read(buf[done:])
				done += n
				if err != nil && (err != io.EOF || done != size) {
					panic(fmt.Sprintf("failed to benchmark: %v", err))
				}
			}
			return time.Since(begin).Seconds()
		}

		do() // warm up

		nrun := 0
		totTime := float64(0)
		totTime2 := float64(0)
		var std float64
		for {
			nrun++
			thisTime := do()
			totTime += thisTime
			totTime2 += thisTime * thisTime
			if nrun >= 2 {
				n := float64(nrun)
				mean := totTime / n
				std = math.Sqrt(math.Max(
					(totTime2/n-mean*mean)*(1+1/(n-1)), 0))
				if std/mean < 0.1 || totTime > 2 {
					secs = mean
					break
				}
			}
		}
		fuseLog.Infof("benchmark: size=%.2fMiB time=%.2fms"+
			"(+-%.2f) thrpt=%.2fMiB/s (%d runs, tot %.2fs)",
			float64(size)/1024/1024, secs*1000, std*1000,
			float64(size)/1024/1024/secs, nrun, totTime)
		return secs
	}

	benchmark := func() {
		blockSize := int(bc.blockSize)
		latency := benchmark_one(blockSize)
		throughput := BLOCK_CACHE_READ_ALL_SIZE /
			benchmark_one(BLOCK_CACHE_READ_ALL_SIZE)

		maxNrBlock := BLOCK_CACHE_READ_ALL_SIZE / blockSize

		nrBlock := 2
		bc.nrLatencyBlock = maxNrBlock
		for nrBlock < maxNrBlock {
			curLat := benchmark_one(blockSize * nrBlock)
			if curLat > latency*1.2 {
				bc.nrLatencyBlock = nrBlock / 2
				break
			}
			nrBlock *= 2
		}

		nrBlock = maxNrBlock / 2
		bc.nrThroughputBlock = bc.nrLatencyBlock
		for nrBlock > bc.nrLatencyBlock {
			size := blockSize * nrBlock
			curThrpt := float64(size) / benchmark_one(size)
			if curThrpt < throughput*0.8 {
				bc.nrThroughputBlock = nrBlock * 2
				break
			}
			nrBlock /= 2
		}
		bc.benchmarkDone.Done()

		blockSizeMb := float64(blockSize) / 1024 / 1024
		fuseLog.Infof(
			"block cache benchmark: latency=%.2fms throughput=%.2fMiB/s"+
				" latency_blocks=%d(%.2fMiB) throughput_blocks=%d(%.2fMiB)",
			latency*1000, throughput/1024/1024,
			bc.nrLatencyBlock,
			float64(bc.nrLatencyBlock)*blockSizeMb,
			bc.nrThroughputBlock,
			float64(bc.nrThroughputBlock)*blockSizeMb)
	}

	bc.benchmarkOnce.Do(benchmark)
	bc.benchmarkDone.Wait()
	assert(0 < bc.nrLatencyBlock && bc.nrLatencyBlock <= bc.nrThroughputBlock,
		"invalid benchmark result")
}
