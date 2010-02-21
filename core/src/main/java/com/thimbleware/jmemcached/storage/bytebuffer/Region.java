package com.thimbleware.jmemcached.storage.bytebuffer;

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

}
