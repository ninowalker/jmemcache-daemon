package com.thimbleware.jmemcached.storage.bytebuffer;

import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 */
public final class Partition {
    private static final int NUM_BUCKETS = 32768;

    ReentrantReadWriteLock storageLock = new ReentrantReadWriteLock();

    public final static class Buckets {
        List<Region> regions = new ArrayList<Region>(32);
    }

    Buckets[] buckets = new Buckets[NUM_BUCKETS];

    ByteBufferBlockStore blockStore;

    int numberItems;

    Partition(ByteBufferBlockStore blockStore) {
        this.blockStore = blockStore;
        for (int i = 0; i < NUM_BUCKETS; i++) buckets[i] = new Buckets();
    }

    public Region find(Key key) {
        int bucket = findBucketNum(key);

        for (Region region : buckets[bucket].regions) {
            if (region.sameAs(key)) return region;
        }
        return null;
    }

    private int findBucketNum(Key key) {
        int hash = BlockStorageCacheStorage.hash(key.hashCode());
        return hash & (buckets.length - 1);
    }

    public void remove(Key key, Region region) {
        int bucket = findBucketNum(key);
        buckets[bucket].regions.remove(region);
        numberItems--;
    }

    public Region add(Key key, LocalCacheElement e) {
        Region region = blockStore.alloc(e.bufferSize());
        e.writeToBuffer(region.slice);
        int bucket = findBucketNum(key);
        buckets[bucket].regions.add(region);

        numberItems++;

        // check # buckets, trigger resize
//            if ((double)numberItems * 0.75 > buckets.length)
//                System.err.println("grow");

        return region;
    }

    public void clear() {
        for (Buckets bucket : buckets) {
            bucket.regions.clear();
        }
        blockStore.clear();
        numberItems = 0;
    }

    public Collection<Key> keys() {
        Set<Key> keys = new HashSet<Key>();
        for (Buckets bucket : buckets) {
            for (Region region : bucket.regions) {
                keys.add(region.keyFromRegion());
            }
        }
        return keys;
    }

    public int getNumberItems() {
        return numberItems;
    }
}
