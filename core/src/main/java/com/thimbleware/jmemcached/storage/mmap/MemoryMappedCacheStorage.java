package com.thimbleware.jmemcached.storage.mmap;

import com.thimbleware.jmemcached.MCElement;
import com.thimbleware.jmemcached.storage.CacheStorage;

import java.util.Set;
import java.util.Map;
import java.util.LinkedHashMap;
import java.io.IOException;

/**
 * Cache storage delegate for the memory mapped storage mechanism.
 */
public class MemoryMappedCacheStorage implements CacheStorage {
    private int maximumItems;
    private long ceilingBytes;

    class StoredValue {
        int flags;
        int expire;
        MemoryMappedBlockStore.Region region;

        StoredValue(int flags, int expire, MemoryMappedBlockStore.Region region) {
            this.flags = flags;
            this.expire = expire;
            this.region = region;
        }
    }

    private MemoryMappedBlockStore store;
    private Map<String, StoredValue> index;

    public MemoryMappedCacheStorage(final MemoryMappedBlockStore store, int maximumItems, long ceilingBytes) {
        this.maximumItems = maximumItems;
        this.ceilingBytes = ceilingBytes;
        this.store = store;
        this.index = new LinkedHashMap<String, StoredValue>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, StoredValue> stringStoredValueEntry) {
                if (store.getFreeBytes() < getCeilingBytes() || size() > getMaximumItems()) {
                    store.free(stringStoredValueEntry.getValue().region);
                    return true;
                } else return false;
            }
        };
    }

    public MCElement get(String keystring) {
        if (keystring == null) throw new IllegalArgumentException("Id must not be null.");

        StoredValue result;
        if (index.containsKey(keystring)) {
            result = index.get(keystring);
            if (result == null) {
                throw new IllegalStateException("Stored item should not be null. Id:" + keystring);
            }
        } else {
            return null;
        }
        MCElement el = new MCElement(keystring, result.flags, result.expire, result.region.size);
        el.data = new byte[result.region.size];

        result.region.buffer.get(el.data);

        return el;
    }

    public void put(String id, MCElement item, int dataLength) {
        if (id == null) throw new IllegalArgumentException("Id must not be null.");
        if (item == null) throw new IllegalArgumentException("Item must not be null.");

        // if the item already exists in the store, free it and replace it
        StoredValue val = index.get(id);
        if (val != null) {
            store.free(val.region);
        }

        MemoryMappedBlockStore.Region region = store.alloc(dataLength, item.data);

        index.put(id, new StoredValue(item.flags, item.expire, region));
    }

    public void remove(String keystring) {
        StoredValue item = index.get(keystring);

        if (item != null) {
            index.remove(keystring);
            store.free(item.region);
        }
    }

    public Set<String> keys() {
        return index.keySet();
    }

    public void clear() {
        index.clear();
        store.clear();
    }

    public void close() {
        clear();
        try {
            store.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long getCurrentSizeBytes() {
        return store.getStoreSizeBytes() - store.getFreeBytes();
    }

    public int getMaximumItems() {
        return maximumItems;
    }

    public long getMaximumSizeBytes() {
        return store.getStoreSizeBytes();
    }

    public long getCurrentItemCount() {
        return index.size();
    }

    public long getCeilingBytes() {
        return ceilingBytes;
    }
}
