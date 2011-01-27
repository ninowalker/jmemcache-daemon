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

    public Region(int size, int usedBlocks, int startBlock) {
        this.size = size;
        this.usedBlocks = usedBlocks;
        this.startBlock = startBlock;
        this.valid = true;
    }

    public Key keyFromRegion(ByteBufferBlockStore store) {
        ChannelBuffer buffer = store.get(this).slice();

        int length = buffer.readInt();
        return new Key(buffer.copy(buffer.readerIndex(), length));
    }

    public LocalCacheElement toValue(ByteBufferBlockStore store) throws IOException, ClassNotFoundException {
        return LocalCacheElement.readFromBuffer(store.get(this).slice());
    }
}
