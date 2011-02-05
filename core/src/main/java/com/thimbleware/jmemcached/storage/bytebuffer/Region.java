package com.thimbleware.jmemcached.storage.bytebuffer;

import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;

import java.io.IOException;
import java.io.ObjectInputStream;

/**
     * Represents a number of allocated blocks in the store
 */
public final class Region {
    /**
     * Size in bytes of the requested area
     */
    public final int size;

    /**
     * Size in blocks of the requested area
     */
    public final int usedBlocks;

    /**
     * Offset into the memory region
     */
    final int startBlock;


    final long timestamp;

    final long expiry;

    /**
     * Flag which is true if the region is valid and in use.
     * Set to false on free()
     */
    public boolean valid = false;

    public ChannelBuffer slice;

    public Region(int size, int usedBlocks, int startBlock, ChannelBuffer slice, long expiry, long timestamp) {
        this.size = size;
        this.usedBlocks = usedBlocks;
        this.startBlock = startBlock;
        this.slice = slice;
        this.expiry = expiry;
        this.timestamp = timestamp;
        this.valid = true;
    }

    public Key keyFromRegion() {
        slice.readerIndex(0);

        int length = slice.readInt();
        return new Key(slice.slice(slice.readerIndex(), length));
    }

    public LocalCacheElement toValue() {
        slice.readerIndex(0);
        return LocalCacheElement.readFromBuffer(slice);
    }

}
