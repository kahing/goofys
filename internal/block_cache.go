package internal

import (
	. "github.com/kahing/goofys/api/common"

	"sync"

	"github.com/hashicorp/golang-lru/v2"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/pbnjay/memory"
)

type cacheKey struct {
	inode fuseops.InodeID
	blkId uint64 // ID of the block (offset div by BLOCK_SIZE)
}

type CacheValue struct {
	mu   sync.RWMutex
	data []byte
	err  error // should be checked before trying to access the value
}

// cache for read-only files based on large blocks
type BlockCache struct {
	mu         sync.RWMutex
	lru        *lru.Cache[cacheKey, *CacheValue]
	block_size uint64
}

func NewBlockCache(flags *FlagStorage) *BlockCache {
	size := int(float64(memory.TotalMemory()/flags.BlockReadCacheSize) *
		flags.BlockReadCacheMemRatio)
	if size <= 0 {
		size = 1
	}
	cache, err := lru.New[cacheKey, *CacheValue](size)
	if err != nil {
		log.Panic("failed to allocate LRU cache", err)
	}
	log.Infof("block cache: size=%d mem=%.2fGiB", size,
		float64(size)*float64(flags.BlockReadCacheSize)/1024/1024/1024)
	return &BlockCache{
		lru:        cache,
		block_size: flags.BlockReadCacheSize,
	}
}

// Get a cache block. alignedOffset is the beginning offset of this cache block.
// newAlloc indicates if this block is newly allocated. If it is true, the
// write lock is acquired, and the caller should fill in the data; if it is
// false, the read lock is acquired, and the caller can read the data.
func (bc *BlockCache) GetOrAllocateWithLock(
	inode *Inode, offset uint64) (
	cache *CacheValue, alignedOffset uint64, newAlloc bool) {

	key := cacheKey{inode.Id, offset / bc.block_size}
	alignedOffset = key.blkId * bc.block_size
	var ok bool

	// first try rlock to get existing cache block
	bc.mu.RLock()
	cache, ok = bc.lru.Get(key)
	bc.mu.RUnlock()

	newAlloc = false
	if ok {
		cache.mu.RLock()
		return
	}

	// try wlock to create a new cache block
	bc.mu.Lock()

	// first check if some other thread has filled in the cache
	cache, ok = bc.lru.Get(key)
	if ok {
		bc.mu.Unlock()
		cache.mu.RLock()
		return
	}

	// create a new block now
	newAlloc = true
	cache = &CacheValue{}
	bc.lru.Add(key, cache)
	cache.mu.Lock() // acquire cache wlock before releasing bc lock
	bc.mu.Unlock()

	cache.data = make([]byte,
		MinUInt64(inode.Attributes.Size-alignedOffset,
			bc.block_size))
	return
}

// remove a cache entry, usually used due to read error
func (bc *BlockCache) Remove(inode *Inode, offset uint64) {
	key := cacheKey{inode.Id, offset / bc.block_size}
	bc.mu.Lock()
	bc.lru.Remove(key)
	bc.mu.Unlock()
}
