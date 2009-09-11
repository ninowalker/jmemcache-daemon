package com.thimbleware.jmemcached.storage;

import com.thimbleware.jmemcached.storage.bytebuffer.ByteBufferBlockStore;
import com.thimbleware.jmemcached.storage.bytebuffer.Region;
import com.thimbleware.jmemcached.MCElement;

import java.util.Map;
import java.util.Set;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 */
public final class ConcurrentSizedBlockStorageMap implements ConcurrentSizedMap<String, MCElement> {

    final ByteBufferBlockStore blockStorage;
    final AtomicInteger ceilingBytes;
    final AtomicInteger maximumItems;
    final LinkedHashMap<String, StoredValue> index;

    final ReentrantReadWriteLock storageLock;

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

    public ConcurrentSizedBlockStorageMap(ByteBufferBlockStore blockStorageParam, int ceilingBytesParam, int maximumItemsVal) {
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

    public MCElement putIfAbsent(String key, MCElement item) {
        try {
            storageLock.readLock().lock();

            // if the item already exists in the store, don't replace!
            StoredValue val = index.get(key);

            // present, return current value
            if (val != null) {
                MCElement el = new MCElement(key, val.flags, val.expire, val.region.size);
                el.data = blockStorage.get(val.region);

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
            if (!(key instanceof String) || (!(value instanceof MCElement))) return false;
            StoredValue val = index.get((String)key);
            MCElement el = new MCElement((String) key, val.flags, val.expire, val.region.size);
            el.data = blockStorage.get(val.region);

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

    public boolean replace(String key, MCElement mcElement, MCElement mcElement1) {
        try {
            storageLock.readLock().lock();
            StoredValue val = index.get(key);
            MCElement el = new MCElement((String) key, val.flags, val.expire, val.region.size);
            el.data = blockStorage.get(val.region);

            if (!el.equals(mcElement)) {
                return false;
            } else {
                storageLock.readLock().unlock();
                put(key, mcElement1);
                storageLock.readLock().lock();

                return true;
            }
        } finally {
            storageLock.readLock().unlock();
        }

    }

    public MCElement replace(String key, MCElement mcElement) {
        try {
            storageLock.readLock().lock();
            StoredValue val = index.get(key);
            if (!index.containsKey(key)) {
                return null;
            } else {
                storageLock.readLock().unlock();
                put(key, mcElement);
                storageLock.readLock().lock();

                return mcElement;
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

    public MCElement get(Object key) {
        try {
            storageLock.readLock().lock();
            if (!(key instanceof String)) return null;
            StoredValue val = index.get(key);
            if (val == null) return null;
            MCElement el = new MCElement((String) key, val.flags, val.expire, val.region.size);
            el.data = blockStorage.get(val.region);

            return el;
        } finally {
            storageLock.readLock().unlock();
        }

    }

    public MCElement put(String key, MCElement item) {
        // absent, lock the store and put the new value in
        try {
            storageLock.writeLock().lock();
            Region region = blockStorage.alloc(item.dataLength, item.data);

            index.put(key, new StoredValue(item.flags, item.expire, region));

            return null;
        } finally {
            storageLock.writeLock().unlock();
        }
    }

    public MCElement remove(Object key) {
        try {
            storageLock.readLock().lock();
            if (!(key instanceof String)) return null;
            StoredValue val = index.get((String)key);
            if (val != null) {
                MCElement el = new MCElement((String) key, val.flags, val.expire, val.region.size);
                el.data = blockStorage.get(val.region);

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

    public void putAll(Map<? extends String, ? extends MCElement> map) {
        // absent, lock the store and put the new value in
        try {
            storageLock.writeLock().lock();
            for (Entry<? extends String, ? extends MCElement> entry : map.entrySet()) {
                String key = entry.getKey();
                MCElement item = entry.getValue();
                Region region = blockStorage.alloc(item.dataLength, item.data);

                index.put(key, new StoredValue(item.flags, item.expire, region));

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

    public Collection<MCElement> values() {
        throw new RuntimeException("operation not supporteded");
    }

    public Set<Entry<String, MCElement>> entrySet() {
        throw new RuntimeException("operation not supporteded");
    }
}
