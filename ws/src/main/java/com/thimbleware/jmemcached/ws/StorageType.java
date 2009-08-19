package com.thimbleware.jmemcached.ws;

/**
 */
public enum StorageType {
    LRU(false),
    BLOCK(true),
    MMAPPED_BLOCK(true);

    private boolean blockStore = false;

    StorageType(boolean blockStore) {
        this.blockStore = blockStore;
    }


    public boolean isBlockStore() {
        return blockStore;
    }
}
