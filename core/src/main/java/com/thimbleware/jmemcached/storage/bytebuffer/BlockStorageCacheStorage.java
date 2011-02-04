package com.thimbleware.jmemcached.storage.bytebuffer;

import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.storage.CacheStorage;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of the cache using the block buffer storage back end.
 */
public final class BlockStorageCacheStorage implements CacheStorage<Key, LocalCacheElement> {

    Partition[] partitions;

    final AtomicInteger ceilingBytes;
    final AtomicInteger maximumItems;
    final AtomicInteger numberItems;
    final long maximumSizeBytes;

    public BlockStorageCacheStorage(int blockStoreBuckets, int ceilingBytesParam, int blockSizeBytes, long maximumSizeBytes, int maximumItemsVal, BlockStoreFactory factory) {
        this.partitions = new Partition[blockStoreBuckets];

        long bucketSizeBytes = maximumSizeBytes / blockStoreBuckets;
        for (int i = 0; i < blockStoreBuckets; i++) {
            this.partitions[i] = new Partition(factory.manufacture(bucketSizeBytes, blockSizeBytes));
        }

        this.numberItems = new AtomicInteger();
        this.ceilingBytes = new AtomicInteger(ceilingBytesParam);
        this.maximumItems = new AtomicInteger(maximumItemsVal);
        this.maximumSizeBytes = maximumSizeBytes;
    }

    private Partition pickPartition(Key key) {
        return partitions[hash(key.hashCode()) & (partitions.length - 1)];
    }

    public final long getMemoryCapacity() {
        long capacity = 0;
        for (Partition byteBufferBlockStore : partitions) {
            capacity += byteBufferBlockStore.blockStore.getStoreSizeBytes();
        }
        return capacity;
    }

    public final long getMemoryUsed() {
        long memUsed = 0;
        for (Partition byteBufferBlockStore : partitions) {
            memUsed += (byteBufferBlockStore.blockStore.getStoreSizeBytes() - byteBufferBlockStore.blockStore.getFreeBytes());
        }
        return memUsed;
    }

    public final int capacity() {
        return maximumItems.get();
    }

    public final void close() throws IOException {
        // first clear all items
        clear();

        // then ask the block store to close
        for (Partition byteBufferBlockStore : partitions) {
            byteBufferBlockStore.blockStore.close();
        }
        this.partitions = null;
    }

    public final LocalCacheElement putIfAbsent(Key key, LocalCacheElement item) {
        Partition partition = pickPartition(key);

        partition.storageLock.readLock().lock();
        try {
            Region region = partition.find(key);

            // not there? add it
            if (region == null) {
                partition.storageLock.readLock().unlock();
                partition.storageLock.writeLock().lock();
                try {
                    numberItems.incrementAndGet();
                    partition.add(key, item);
                } finally {
                    partition.storageLock.readLock().lock();
                    partition.storageLock.writeLock().unlock();
                }

                return null;
            } else {
                // there? return its value
                return region.toValue();
            }
        } finally {
            partition.storageLock.readLock().unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    public final boolean remove(Object okey, Object value) {
        if (!(okey instanceof Key) || (!(value instanceof LocalCacheElement))) return false;

        Key key = (Key) okey;
        Partition partition = pickPartition(key);


        try {
            partition.storageLock.readLock().lock();
            Region region = partition.find(key);
            if (region == null) return false;
            else {
                partition.storageLock.readLock().unlock();
                partition.storageLock.writeLock().lock();
                try {
                    partition.blockStore.free(region);
                    partition.remove(key, region);
                    numberItems.decrementAndGet();
                    return true;
                } finally {
                    partition.storageLock.readLock().lock();
                    partition.storageLock.writeLock().unlock();
                }

            }
        } finally {
            partition.storageLock.readLock().unlock();
        }
    }

    public final boolean replace(Key key, LocalCacheElement original, LocalCacheElement replace) {
        Partition partition = pickPartition(key);

        partition.storageLock.readLock().lock();
        try {
            Region region = partition.find(key);

            // not there? that's a fail
            if (region == null) return false;

            // there, check for equivalence of value
            LocalCacheElement el = null;
            el = region.toValue();
            if (!el.equals(original)) {
                return false;
            } else {
                partition.storageLock.readLock().unlock();
                partition.storageLock.writeLock().lock();
                try {
                    partition.remove(key, region);
                    partition.add(key, replace);
                    return true;
                } finally {
                    partition.storageLock.readLock().lock();
                    partition.storageLock.writeLock().unlock();
                }

            }

        } finally {
            partition.storageLock.readLock().unlock();
        }
    }

    public final LocalCacheElement replace(Key key, LocalCacheElement replace) {
        Partition partition = pickPartition(key);

        partition.storageLock.readLock().lock();
        try {
            Region region = partition.find(key);

            // not there? that's a fail
            if (region == null) return null;

            // there,
            LocalCacheElement el = null;
            el = region.toValue();
            partition.storageLock.readLock().unlock();
            partition.storageLock.writeLock().lock();
            try {
                partition.remove(key, region);
                partition.add(key, replace);
                return el;
            } finally {
                partition.storageLock.readLock().lock();
                partition.storageLock.writeLock().unlock();
            }


        } finally {
            partition.storageLock.readLock().unlock();
        }
    }

    public final int size() {
        return numberItems.get();
    }

    public final boolean isEmpty() {
        return numberItems.get() == 0;
    }

    public final boolean containsKey(Object okey) {
        if (!(okey instanceof Key)) return false;

        Key key = (Key) okey;
        Partition partition = pickPartition(key);

        Region region;
        try {
            partition.storageLock.readLock().lock();
            region = partition.find(key);
            return region != null;
        } finally {
            partition.storageLock.readLock().unlock();
        }
    }

    public final boolean containsValue(Object o) {
        throw new UnsupportedOperationException("operation not supported");
    }

    public final LocalCacheElement get(Object okey) {
        if (!(okey instanceof Key)) return null;

        Key key = (Key) okey;
        Partition partition = pickPartition(key);

        try {
            partition.storageLock.readLock().lock();
            Region region = partition.find(key);
            if (region == null) return null;
            return region.toValue();
        } finally {
            partition.storageLock.readLock().unlock();
        }
    }

    public final LocalCacheElement put(final Key key, final LocalCacheElement item) {
        Partition partition = pickPartition(key);

        partition.storageLock.readLock().lock();
        try {
            Region region = partition.find(key);

            partition.storageLock.readLock().unlock();
            partition.storageLock.writeLock().lock();
            try {
                LocalCacheElement old = null;
                if (region != null) {
                    old = region.toValue();
                }
                if (region != null) partition.remove(key, region);
                partition.add(key, item);
                numberItems.incrementAndGet();
                return old;
            } finally {
                partition.storageLock.readLock().lock();
                partition.storageLock.writeLock().unlock();
            }


        } finally {
            partition.storageLock.readLock().unlock();
        }
    }

    public final LocalCacheElement remove(Object okey) {
        if (!(okey instanceof Key)) return null;

        Key key = (Key) okey;
        Partition partition = pickPartition(key);

        try {
            partition.storageLock.readLock().lock();
            Region region = partition.find(key);
            if (region == null) return null;
            else {
                partition.storageLock.readLock().unlock();
                partition.storageLock.writeLock().lock();
                try {
                    LocalCacheElement old = null;
                    old = region.toValue();
                    partition.blockStore.free(region);
                    partition.remove(key, region);
                    numberItems.decrementAndGet();
                    return old;
                } finally {
                    partition.storageLock.readLock().lock();
                    partition.storageLock.writeLock().unlock();
                }

            }
        } finally {
            partition.storageLock.readLock().unlock();
        }
    }

    public final void putAll(Map<? extends Key, ? extends LocalCacheElement> map) {
        // absent, lock the store and put the new value in
        for (Entry<? extends Key, ? extends LocalCacheElement> entry : map.entrySet()) {
            Key key = entry.getKey();
            LocalCacheElement item;
            item = entry.getValue();
            put(key, item);
        }
    }

    public final void clear() {
        for (Partition partition : partitions) {
            partition.storageLock.writeLock().lock();
            numberItems.addAndGet(partition.keys().size() * - 1);
            try {
                partition.clear();
            } finally {
                partition.storageLock.writeLock().unlock();
            }
        }

    }

    public Set<Key> keySet() {
        Set<Key> keys = new HashSet<Key>();
        for (Partition partition : partitions) {
            keys.addAll(partition.keys());
        }

        return keys;
    }

    public Collection<LocalCacheElement> values() {
        throw new UnsupportedOperationException("operation not supported");
    }

    public Set<Entry<Key, LocalCacheElement>> entrySet() {
        throw new UnsupportedOperationException("operation not supported");
    }

    protected static int hash(int h) {
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h <<  15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h <<   3);
        h ^= (h >>>  6);
        h += (h <<   2) + (h << 14);
        return h ^ (h >>> 16);
    }


}
