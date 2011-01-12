package com.thimbleware.jmemcached.storage.bytebuffer;

import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.bytebuffer.ByteBufferBlockStore;
import com.thimbleware.jmemcached.storage.bytebuffer.Region;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;
import com.thimbleware.jmemcached.storage.hash.SizedItem;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Implementation of the concurrent (linked) sized map using the block buffer storage back end.
 *
 * TODO Rather sub-optimal global locking strategy could be improved with a more intricate :dstriped locking implementation.
 */
public final class BlockStorageCacheStorage implements CacheStorage<Key, LocalCacheElement> {

    ByteBufferBlockStore[] blockStorage;
    ReentrantReadWriteLock[] storageLock;

    final AtomicInteger ceilingBytes;
    final AtomicInteger maximumItems;
    final ConcurrentLinkedHashMap<Key, StoredValue> index;
    final long maximumSizeBytes;


    /**
     * Representation of the stored value, encoding its expiration, block region, and attached flags.
     * TODO investigate whether this can be collapsed into a subclass of LocalCacheElement instead?
     */
    class StoredValue implements SizedItem {
        final int flags;
        final int expire;
        final long casUnique;
        final int[] buckets;
        final Region[] regions;

        StoredValue(int flags, int expire, long casUnique, int[] buckets, Region ... regions) {
            this.flags = flags;
            this.expire = expire;
            this.buckets = buckets;
            this.regions = regions;
            this.casUnique = casUnique;
        }

        public LocalCacheElement toElement(Key key) {
            final LocalCacheElement element = new LocalCacheElement(key, flags, expire, casUnique);
            element.setData(getData());
            return element;
        }

        public void free() {
            for (int i = 0; i < regions.length; i++) {
                final int bucket = buckets[i];

                blockStorage[bucket].free(regions[i]);
            }
        }

        public ChannelBuffer getData() {
            ChannelBuffer result = ChannelBuffers.buffer(size());
            for (int i = 0; i < regions.length; i++) {
                final int bucket = buckets[i];
                result.writeBytes(blockStorage[bucket].get(regions[i]));
            }
            return result;
        }

        public int size() {
            int size = 0;
            for (Region region : regions) {
                size+= region.size;
            }
            return size;
        }
    }

    public BlockStorageCacheStorage(int blockStoreBuckets, int ceilingBytesParam, int blockSizeBytes, long maximumSizeBytes, int maximumItemsVal, BlockStoreFactory factory) {
        this.blockStorage = new ByteBufferBlockStore[blockStoreBuckets];
        this.storageLock= new ReentrantReadWriteLock[blockStoreBuckets];

        long bucketSizeBytes = maximumSizeBytes / blockStoreBuckets;
        for (int i = 0; i < blockStoreBuckets; i++) {
            this.blockStorage[i] = factory.manufacture(bucketSizeBytes, blockSizeBytes);
            this.storageLock[i] = new ReentrantReadWriteLock();
        }

        this.ceilingBytes = new AtomicInteger(ceilingBytesParam);
        this.maximumItems = new AtomicInteger(maximumItemsVal);
        this.maximumSizeBytes = maximumSizeBytes;

        this.index = ConcurrentLinkedHashMap.create(ConcurrentLinkedHashMap.EvictionPolicy.LRU, maximumItemsVal, maximumSizeBytes, new ConcurrentLinkedHashMap.EvictionListener<Key, StoredValue>(){
            public void onEviction(Key key, StoredValue value) {
                value.free();
            }
        });
    }

    private int pickBucket(Key key, int partitionNum) {
        return new Random().nextInt(blockStorage.length);
//        return Math.abs(key.hashCode() * partitionNum) % blockStorage.length;
    }

    public long getMemoryCapacity() {
        long capacity = 0;
        for (ByteBufferBlockStore byteBufferBlockStore : blockStorage) {
            capacity += byteBufferBlockStore.getStoreSizeBytes();
        }
        return capacity;
    }

    public long getMemoryUsed() {
        long memUsed = 0;
        for (ByteBufferBlockStore byteBufferBlockStore : blockStorage) {
            memUsed += (byteBufferBlockStore.getStoreSizeBytes() - byteBufferBlockStore.getFreeBytes());
        }
        return memUsed;
    }

    public int capacity() {
        return maximumItems.get();
    }

    public void close() throws IOException {
        // first clear all items
        clear();

        // then ask the block store to close
        for (ByteBufferBlockStore byteBufferBlockStore : blockStorage) {
            byteBufferBlockStore.close();
        }
        this.blockStorage = null;
        this.storageLock = null;
    }

    public LocalCacheElement putIfAbsent(Key key, LocalCacheElement item) {
        // if the item already exists in the store, don't replace!
        StoredValue val = index.get(key);

        // present, return current value
        if (val != null) {
            return val.toElement(key);
        }

        put(key, item);

        return null;
    }

    /**
     * {@inheritDoc}
     */
    public boolean remove(Object key, Object value) {
        if (!(key instanceof Key) || (!(value instanceof LocalCacheElement))) return false;
        StoredValue val = index.get(key);
        LocalCacheElement el = val.toElement((Key) key);

        if (!el.equals(value)) {
            return false;
        } else {
            index.remove(key);
            val.free();

            return true;
        }
    }

    public boolean replace(Key key, LocalCacheElement original, LocalCacheElement replace) {
        StoredValue val = index.get(key);
        LocalCacheElement el = val.toElement(key);

        if (!el.equals(original)) {
            return false;
        } else {
            // TODO not atomic; should share the write lock here
            remove(key);
            put(key, replace);

            return true;
        }

    }

    public LocalCacheElement replace(Key key, LocalCacheElement replace) {
        StoredValue val = index.get(key);
        if (!index.containsKey(key)) {
            return null;
        } else {
            // TODO not atomic; should share the write lock here
            remove(key);
            put(key, replace);

            return replace;
        }
    }

    public int size() {
        return index.size();
    }

    public boolean isEmpty() {
        return index.isEmpty();
    }

    public boolean containsKey(Object o) {
        return index.containsKey(o);
    }

    public boolean containsValue(Object o) {
        throw new RuntimeException("operation not supporteded");
    }

    public LocalCacheElement get(Object key) {
        if (!(key instanceof Key)) return null;
        StoredValue val = index.get(key);
        if (val == null) return null;
        try {
            lockRead(val);
            return val.toElement((Key) key);
        } finally {
            unlockRead(val);
        }
    }

    private long getMaxPerBucketItemSize() {
        return this.maximumSizeBytes / this.blockStorage.length;
    }

    public static int numBuckets(int size, int bucketSize) {
        int mod = size % bucketSize;
        int div = size / bucketSize;
        return mod == 0 ? div : div + 1;
    }

    public LocalCacheElement put(Key key, LocalCacheElement item) {
        if (index.containsKey(key)) remove(key);
        
        int numBuckets = numBuckets(item.size(), (int) getMaxPerBucketItemSize());
        ChannelBuffer readBuffer = item.getData();
        Region[] regions = new Region[numBuckets];
        int buckets[] = new int[numBuckets];
        for (int i = 0; i < numBuckets; i++) {
            int bucket = pickBucket(key, i);
            buckets[i] = bucket;
            storageLock[bucket].writeLock().lock();
        }


        for (int i = 0; i < numBuckets; i++) {
            int bucket = buckets[i];
            final int fragmentSize = (i < numBuckets - 1) ? item.size() / numBuckets : readBuffer.readableBytes();
            byte[] fragment = new byte[fragmentSize];
            readBuffer.readBytes(fragment);
            Region region = blockStorage[bucket].alloc(fragmentSize, fragment);
            regions[i] = region;
        }

        for (int bucket : buckets) {
            storageLock[bucket].writeLock().unlock();
        }

        index.put(key, new StoredValue(item.getFlags(), item.getExpire(), item.getCasUnique(), buckets, regions));

        return null;
    }

    public LocalCacheElement remove(Object key) {
        if (!(key instanceof Key)) return null;
        StoredValue val = index.get(key);
        if (val != null) {
            LocalCacheElement el = val.toElement((Key) key);
            lockWrite(val);
            val.free();
            unlockWrite(val);
            el.setData(val.getData());

            index.remove(key);

            return el;
        } else
            return null;
    }

    public void putAll(Map<? extends Key, ? extends LocalCacheElement> map) {
        // absent, lock the store and put the new value in
        for (Entry<? extends Key, ? extends LocalCacheElement> entry : map.entrySet()) {
            Key key = entry.getKey();
            LocalCacheElement item;
            item = entry.getValue();
            put(key, item);
        }
    }


    public void clear() {
        lockWriteAll();
        index.clear();
        for (ByteBufferBlockStore aBlockStorage : blockStorage) {
            aBlockStorage.clear();
        }
        unlockWriteAll();
    }

    public void lockReadAll() {
        for (ReentrantReadWriteLock reentrantReadWriteLock : storageLock) {
            reentrantReadWriteLock.readLock().lock();
        }
    }

    public void unlockReadAll() {
        for (ReentrantReadWriteLock reentrantReadWriteLock : storageLock) {
            reentrantReadWriteLock.readLock().unlock();
        }
    }


    public void lockWriteAll() {
        for (ReentrantReadWriteLock reentrantReadWriteLock : storageLock) {
            reentrantReadWriteLock.writeLock().lock();
        }
    }

    public void unlockWriteAll() {
        for (ReentrantReadWriteLock reentrantReadWriteLock : storageLock) {
            reentrantReadWriteLock.writeLock().unlock();
        }
    }

    public void lockRead(StoredValue value) {
        for (int bucket : value.buckets) {
            this.storageLock[bucket].readLock().lock();
        }
    }

    public void unlockRead(StoredValue value) {
        for (int bucket : value.buckets) {
            this.storageLock[bucket].readLock().unlock();
        }
    }

    public void lockWrite(StoredValue value) {
        for (int bucket : value.buckets) {
            this.storageLock[bucket].writeLock().lock();
        }
    }

    public void unlockWrite(StoredValue value) {
        for (int bucket : value.buckets) {
            this.storageLock[bucket].writeLock().unlock();
        }
    }

    public Set<Key> keySet() {
        return index.keySet();
    }

    public Collection<LocalCacheElement> values() {
        throw new RuntimeException("operation not supporteded");
    }

    public Set<Entry<Key, LocalCacheElement>> entrySet() {
        throw new RuntimeException("operation not supporteded");
    }
}
