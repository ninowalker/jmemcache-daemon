package com.thimbleware.jmemcached.storage.bytebuffer;

import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.bytebuffer.ByteBufferBlockStore;
import com.thimbleware.jmemcached.storage.bytebuffer.Region;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Implementation of the concurrent (linked) sized map using the block buffer storage back end.
 *
 * TODO Rather sub-optimal global locking strategy could be improved with a more intricate striped locking implementation.
 */
public final class BlockStorageCacheStorage implements CacheStorage<String, LocalCacheElement> {

    final ByteBufferBlockStore blockStorage;
    final AtomicInteger ceilingBytes;
    final AtomicInteger maximumItems;
    final LinkedHashMap<String, StoredValue> index;

    final ReentrantReadWriteLock storageLock;

    /**
     * Representation of the stored value, encoding its expiration, block region, and attached flags.
     * TODO investigate whether this can be collapsed into a subclass of LocalCacheElement instead?
     */
    class StoredValue {
        int flags;
        int expire;
        Region region;

        StoredValue(int flags, int expire, Region region) {
            this.flags = flags;
            this.expire = expire;
            this.region = region;
        }
    }

    public BlockStorageCacheStorage(ByteBufferBlockStore blockStorageParam, int ceilingBytesParam, int maximumItemsVal) {
        this.blockStorage = blockStorageParam;
        this.ceilingBytes = new AtomicInteger(ceilingBytesParam);
        this.maximumItems = new AtomicInteger(maximumItemsVal);
        this.storageLock = new ReentrantReadWriteLock();

        this.index = new LinkedHashMap<String, StoredValue>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, StoredValue> stringStoredValueEntry) {
                try {
                    storageLock.readLock().lock();
                    if (blockStorage.getFreeBytes() < ceilingBytes.get() || index.size() > maximumItems.get()) {
                        storageLock.readLock().unlock();
                        storageLock.writeLock().lock();
                        blockStorage.free(stringStoredValueEntry.getValue().region);
                        storageLock.writeLock().unlock();
                        storageLock.readLock().lock();

                        return true;
                    } else return false;
                } finally {
                    storageLock.readLock().unlock();
                }

            }
        };
    }

    public long getMemoryCapacity() {
        return blockStorage.getStoreSizeBytes();
    }

    public long getMemoryUsed() {
        return blockStorage.getStoreSizeBytes() - blockStorage.getFreeBytes();
    }

    public int capacity() {
        return maximumItems.get();
    }

    public void close() throws IOException {
        // first clear all items
        clear();

        // then ask the block store to close
        blockStorage.close();
    }

    public LocalCacheElement putIfAbsent(String key, LocalCacheElement item) {
        try {
            storageLock.readLock().lock();

            // if the item already exists in the store, don't replace!
            StoredValue val = index.get(key);

            // present, return current value
            if (val != null) {
                LocalCacheElement el = new LocalCacheElement(key, val.flags, val.expire);
                el.setData(blockStorage.get(val.region));

                return el;
            }

            storageLock.readLock().unlock();
            put(key, item);
            storageLock.readLock().lock();

            return null;
        } finally {
            storageLock.readLock().unlock();
        }

    }

    /**
     * {@inheritDoc}
     */
    public boolean remove(Object key, Object value) {
        try {
            storageLock.readLock().lock();
            if (!(key instanceof String) || (!(value instanceof LocalCacheElement))) return false;
            StoredValue val = index.get((String)key);
            LocalCacheElement el = new LocalCacheElement((String) key, val.flags, val.expire);
            el.setData(blockStorage.get(val.region));

            if (!el.equals(value)) {
                return false;
            } else {
                storageLock.readLock().unlock();
                storageLock.writeLock().lock();
                index.remove((String) key);
                blockStorage.free(val.region);
                storageLock.readLock().lock();
                storageLock.writeLock().unlock();

                return true;
            }
        } finally {
            storageLock.readLock().unlock();
        }

    }

    public boolean replace(String key, LocalCacheElement LocalCacheElement, LocalCacheElement LocalCacheElement1) {
        try {
            storageLock.readLock().lock();
            StoredValue val = index.get(key);
            LocalCacheElement el = new LocalCacheElement((String) key, val.flags, val.expire);
            el.setData(blockStorage.get(val.region));

            if (!el.equals(LocalCacheElement)) {
                return false;
            } else {
                storageLock.readLock().unlock();
                put(key, LocalCacheElement1);
                storageLock.readLock().lock();

                return true;
            }
        } finally {
            storageLock.readLock().unlock();
        }

    }

    public LocalCacheElement replace(String key, LocalCacheElement LocalCacheElement) {
        try {
            storageLock.readLock().lock();
            StoredValue val = index.get(key);
            if (!index.containsKey(key)) {
                return null;
            } else {
                storageLock.readLock().unlock();
                put(key, LocalCacheElement);
                storageLock.readLock().lock();

                return LocalCacheElement;
            }
        } finally {
            storageLock.readLock().unlock();
        }
    }

    public int size() {
        try {
            storageLock.readLock().lock();
            return index.size();
        } finally {
            storageLock.readLock().unlock();
        }
    }

    public boolean isEmpty() {
        try {
            storageLock.readLock().lock();
            return index.isEmpty();
        } finally {
            storageLock.readLock().unlock();
        }
    }

    public boolean containsKey(Object o) {
        try {
            storageLock.readLock().lock();
            return index.containsKey(o);
        } finally {
            storageLock.readLock().unlock();
        }
    }

    public boolean containsValue(Object o) {
        throw new RuntimeException("operation not supporteded");
    }

    public LocalCacheElement get(Object key) {
        try {
            storageLock.readLock().lock();
            if (!(key instanceof String)) return null;
            StoredValue val = index.get(key);
            if (val == null) return null;
            LocalCacheElement el = new LocalCacheElement((String) key, val.flags, val.expire);
            el.setData(blockStorage.get(val.region));

            return el;
        } finally {
            storageLock.readLock().unlock();
        }

    }

    public LocalCacheElement put(String key, LocalCacheElement item) {
        // absent, lock the store and put the new value in
        try {
            storageLock.writeLock().lock();
            Region region = blockStorage.alloc(item.getData().length, item.getData());

            index.put(key, new StoredValue(item.getFlags(), item.getExpire(), region));

            return null;
        } finally {
            storageLock.writeLock().unlock();
        }
    }

    public LocalCacheElement remove(Object key) {
        try {
            storageLock.readLock().lock();
            if (!(key instanceof String)) return null;
            StoredValue val = index.get((String)key);
            if (val != null) {
                LocalCacheElement el = new LocalCacheElement((String) key, val.flags, val.expire);
                el.setData(blockStorage.get(val.region));

                storageLock.readLock().unlock();
                storageLock.writeLock().lock();
                blockStorage.free(val.region);
                index.remove((String) key);
                storageLock.readLock().lock();
                storageLock.writeLock().unlock();

                return el;
            } else
                return null;
        } finally {
            storageLock.readLock().unlock();
        }
    }

    public void putAll(Map<? extends String, ? extends LocalCacheElement> map) {
        // absent, lock the store and put the new value in
        try {
            storageLock.writeLock().lock();
            for (Entry<? extends String, ? extends LocalCacheElement> entry : map.entrySet()) {
                String key = entry.getKey();
                LocalCacheElement item = entry.getValue();
                Region region = blockStorage.alloc(item.getData().length, item.getData());

                index.put(key, new StoredValue(item.getFlags(), item.getExpire(), region));

            }
        } finally {
            storageLock.writeLock().unlock();
        }
    }


    public void clear() {
        try {
            storageLock.writeLock().lock();
            index.clear();
            blockStorage.clear();
        } finally {
            storageLock.writeLock().unlock();
        }
    }

    public Set<String> keySet() {
        try {
            storageLock.readLock().lock();
            return index.keySet();
        } finally {
            storageLock.readLock().unlock();
        }
    }

    public Collection<LocalCacheElement> values() {
        throw new RuntimeException("operation not supporteded");
    }

    public Set<Entry<String, LocalCacheElement>> entrySet() {
        throw new RuntimeException("operation not supporteded");
    }
}
