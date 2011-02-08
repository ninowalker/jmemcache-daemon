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

    ChannelBuffer[] buckets = new ChannelBuffer[NUM_BUCKETS];

    ByteBufferBlockStore blockStore;

    int numberItems;

    Partition(ByteBufferBlockStore blockStore) {
        this.blockStore = blockStore;
    }

    public Region find(Key key) {
        int bucket = findBucketNum(key);

        if (buckets[bucket] == null) return null;
        ChannelBuffer regions = buckets[bucket].slice();

        regions.readerIndex(0);
        while (regions.readableBytes() > 0) {
            int totsize = regions.readInt();
            int rsize = regions.readInt();
            int rusedBlocks = regions.readInt();
            int rstartBlock = regions.readInt();
            long expiry = regions.readLong();
            long timestamp = regions.readLong();
            int rkeySize = regions.readInt();

            if (rkeySize == key.bytes.capacity()) {
                ChannelBuffer rkey = regions.readSlice(rkeySize);

                key.bytes.readerIndex(0);
                if (rkey.equals(key.bytes)) return new Region(rsize, rusedBlocks, rstartBlock, blockStore.get(rstartBlock, rsize), expiry, timestamp);
            } else {
                regions.skipBytes(rkeySize);
            }
        }

        return null;
    }

    public boolean has(Key key) {
        int bucket = findBucketNum(key);

        if (buckets[bucket] == null) return false;
        ChannelBuffer regions = buckets[bucket].slice();


        regions.readerIndex(0);
        while (regions.readableBytes() > 0) {
            int totsize = regions.readInt();
            regions.skipBytes(28);
            int rkeySize = regions.readInt();

            if (rkeySize == key.bytes.capacity()) {
                ChannelBuffer rkey = regions.readSlice(rkeySize);

                key.bytes.readerIndex(0);
                if (rkey.equals(key.bytes)) return true;
            } else {
                regions.skipBytes(rkeySize);
            }
        }

        return false;
    }

    private int findBucketNum(Key key) {
        int hash = BlockStorageCacheStorage.hash(key.hashCode());
        return hash & (buckets.length - 1);
    }

    public void remove(Key key, Region region) {
        int bucket = findBucketNum(key);

        ChannelBuffer newRegion = ChannelBuffers.dynamicBuffer(128);
        ChannelBuffer regions = buckets[bucket].slice();
        if (regions == null) return;

        regions.readerIndex(0);
        while (regions.readableBytes() > 0) {
            // read key portion then region portion
            int pos = regions.readerIndex();
            int totsize = regions.readInt();
            regions.skipBytes(28);
            int rkeySize = regions.readInt();
            ChannelBuffer rkey = regions.readBytes(rkeySize);

            if (rkeySize != key.bytes.capacity() || !rkey.equals(key.bytes)) {
                newRegion.writeBytes(regions.slice(pos, regions.readerIndex()));
            }
        }

        buckets[bucket] = newRegion;

        numberItems--;
    }

    public Region add(Key key, LocalCacheElement e) {
        Region region = blockStore.alloc(e.bufferSize(), e.getExpire(), System.currentTimeMillis());
        e.writeToBuffer(region.slice);
        int bucket = findBucketNum(key);

        ChannelBuffer outbuf = ChannelBuffers.directBuffer(32 + key.bytes.capacity());
        outbuf.writeInt(region.size);
        outbuf.writeInt(region.usedBlocks);
        outbuf.writeInt(region.startBlock);
        outbuf.writeLong(region.expiry);
        outbuf.writeLong(region.timestamp);
        outbuf.writeInt(key.bytes.capacity());
        key.bytes.readerIndex(0);
        outbuf.writeBytes(key.bytes);

        ChannelBuffer regions = buckets[bucket];
        if (regions == null) {
            regions = ChannelBuffers.dynamicBuffer(128);
            buckets[bucket] = regions;
        }

        regions.writeInt(outbuf.capacity());
        regions.writeBytes(outbuf);

        numberItems++;

        return region;
    }

    public void clear() {
        for (ChannelBuffer bucket : buckets) {
            if (bucket != null)
                bucket.clear();
        }
        blockStore.clear();
        numberItems = 0;
    }

    public Collection<Key> keys() {
        Set<Key> keys = new HashSet<Key>();

        for (ChannelBuffer regionsa : buckets) {
            if (regionsa != null) {
                ChannelBuffer regions = regionsa.slice();
                regions.readerIndex(0);
                while (regions.readableBytes() > 0) {
                    // read key portion then region portion
                    int totsize = regions.readInt();
                    regions.skipBytes(28);
                    int rkeySize = regions.readInt();
                    ChannelBuffer rkey = regions.readBytes(rkeySize);

                    keys.add(new Key(rkey));
                }
            }
        }
        return keys;
    }

    public int getNumberItems() {
        return numberItems;
    }
}
