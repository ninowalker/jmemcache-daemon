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

    /**
     * Flag which is true if the region is valid and in use.
     * Set to false on free()
     */
    public boolean valid = false;

    public final ChannelBuffer slice;

    public Region(int size, int usedBlocks, int startBlock, ChannelBuffer slice) {
        this.size = size;
        this.usedBlocks = usedBlocks;
        this.startBlock = startBlock;
        this.slice = slice;
        this.valid = true;
    }

    public Key keyFromRegion() {
        slice.readerIndex(0);

        int length = slice.readInt();
        return new Key(slice.copy(slice.readerIndex(), length));
    }

    public LocalCacheElement toValue() {
        slice.readerIndex(0);
        return LocalCacheElement.readFromBuffer(slice);
    }

    public boolean sameAs(Region r) {
        slice.readerIndex(0);

        int lengthA = slice.readInt();

        ChannelBuffer keyA = slice.slice(slice.readerIndex(), lengthA);

        ChannelBuffer bufferB = r.slice.slice();
        int lengthB = bufferB.readInt();
        ChannelBuffer keyB = slice.slice(slice.readerIndex(), lengthB);

        keyA.readerIndex(0);
        keyB.readerIndex(0);

        return keyA.equals(keyB);
    }

    public boolean sameAs(Key r) {
        slice.readerIndex(0);

        int lengthA = slice.readInt();
        ChannelBuffer keyA = slice.slice(slice.readerIndex(), lengthA);

        ChannelBuffer keyB = r.bytes;

        keyA.readerIndex(0);
        keyB.readerIndex(0);

        return keyA.equals(keyB);
    }
}
