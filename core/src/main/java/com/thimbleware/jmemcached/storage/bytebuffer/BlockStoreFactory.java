package com.thimbleware.jmemcached.storage.bytebuffer;

/**
 */
public interface BlockStoreFactory<BS extends ByteBufferBlockStore> {
    BS manufacture(long sizeBytes, int blockSizeBytes);
}
