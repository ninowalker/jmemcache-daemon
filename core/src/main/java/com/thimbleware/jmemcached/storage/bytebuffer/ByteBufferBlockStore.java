package com.thimbleware.jmemcached.storage.bytebuffer;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import java.io.IOException;

import com.thimbleware.jmemcached.util.BitSet;

/**
 * Memory mapped block storage mechanism with a free-list maintained by TreeMap
 *
 * Allows memory for storage to be mapped outside of the VM's main memory, and outside the purvey
 * of the GC.
 *
 * Should offer O(Log(N)) search and free of blocks.
 */
public class ByteBufferBlockStore {

    protected ChannelBuffer storageBuffer;

    private long freeBytes;

    private long storeSizeBytes;
    private final int blockSizeBytes;

    private BitSet allocated;
    private static final ByteBufferBlockStoreFactory BYTE_BUFFER_BLOCK_STORE_FACTORY = new ByteBufferBlockStoreFactory();


    /**
     * Exception thrown on inability to allocate a new block
     */
    public static class BadAllocationException extends RuntimeException {
        public BadAllocationException(String s) {
            super(s);
        }
    }
    public static BlockStoreFactory getFactory() {
        return BYTE_BUFFER_BLOCK_STORE_FACTORY;
    }

    public static class ByteBufferBlockStoreFactory implements BlockStoreFactory<ByteBufferBlockStore> {

        public ByteBufferBlockStore manufacture(long sizeBytes, int blockSizeBytes) {
            try {
                ChannelBuffer buffer = ChannelBuffers.directBuffer((int) sizeBytes);
                return new ByteBufferBlockStore(buffer, sizeBytes, blockSizeBytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Construct a new memory mapped block storage against a filename, with a certain size
     * and block size.
     * @param storageBuffer
     * @param blockSizeBytes the size of a block in the store
     * @throws java.io.IOException thrown on failure to open the store or map the file
     */
    private ByteBufferBlockStore(ChannelBuffer storageBuffer, long sizeBytes, int blockSizeBytes) throws IOException {
        this.storageBuffer = storageBuffer;
        this.blockSizeBytes = blockSizeBytes;
        initialize((int)sizeBytes);
    }

    /**
     * Constructor used only be subclasses, allowing them to provide their own buffer.
     */
    protected ByteBufferBlockStore(int blockSizeBytes) {
        this.blockSizeBytes = blockSizeBytes;
    }

    protected void initialize(int storeSizeBytes) {
        // set the size of the store in bytes
        this.storeSizeBytes = storageBuffer.capacity();

        // the number of free bytes starts out as the entire store
        freeBytes = storeSizeBytes;

        // clear the buffer
        storageBuffer.clear();

        allocated = new BitSet(storeSizeBytes / blockSizeBytes);
        allocated.set((int) (roundUp(storeSizeBytes, blockSizeBytes) / blockSizeBytes), false);

        clear();
    }


    /**
     * Rounds up a requested size to the nearest block width.
     * @param size the requested size
     * @param blockSize the block size to use
     * @return the actual mount to use
     */
    public static long roundUp( long size, long blockSize ) {
        return size - 1L + blockSize - (size - 1L) % blockSize;
    }

    /**
     * Close the store, destroying all data and closing the backing file
     * @throws java.io.IOException thrown on failure to close file
     */
    public void close() throws IOException {
        // clear the region list
        clear();

        //
        freeResources();

        // null out the storage to allow the GC to get rid of it
        storageBuffer = null;
    }

    protected void freeResources() throws IOException {
        // noop
    }

    private int findPos(int numBlocks) {
        int startPos = 0;
        while (startPos < allocated.size()) {
            int clearSpot = allocated.nextClearBit(startPos);
            int endSpot = allocated.nextSetBit(clearSpot);
            if ( (endSpot - clearSpot) >= numBlocks || endSpot == -1)
                return clearSpot;
            else
                startPos = endSpot;
        }
        throw new BadAllocationException("unable to allocate room; all blocks consumed");
    }

    private void markPos(int start, int numBlocks) {
        allocated.set(start, start + numBlocks);
    }

    private void clear(int start, int numBlocks) {
        allocated.set(start, start + numBlocks, false);
    }

    /**
     * Allocate a region in the block storage
     *
     * @param desiredSize size (in bytes) desired for the region
     * @param expiry expiry time in ms since epoch
     *@param timestamp allocation timestamp of the entry
     * @return the region descriptor
     */
    public Region alloc(int desiredSize, long expiry, long timestamp) {
        final long desiredBlockSize = roundUp(desiredSize, blockSizeBytes);
        int numBlocks = (int) (desiredBlockSize / blockSizeBytes);

        int pos = findPos(numBlocks);
        markPos(pos, numBlocks);

        freeBytes -= desiredBlockSize;

        // get the buffer to it
        int position = pos * blockSizeBytes;
        ChannelBuffer slice = storageBuffer.slice(position, desiredSize);
        slice.writerIndex(0);
        slice.readerIndex(0);

        return new Region(desiredSize, numBlocks, pos, slice, expiry, timestamp);
    }

    public ChannelBuffer get(int startBlock, int size) {
        return storageBuffer.slice(startBlock * blockSizeBytes, size);
    }

    public void free(Region region) {
        freeBytes += (region.usedBlocks * blockSizeBytes);
        region.valid = false;
        region.slice = null;
        int pos = region.startBlock;
        clear(pos, region.size / blockSizeBytes);
    }

    public void clear()
    {
        // say goodbye to the region list
        allocated.clear();

        // reset the # of free bytes back to the max size
        freeBytes = storeSizeBytes;
    }



    public long getStoreSizeBytes() {
        return storeSizeBytes;
    }

    public int getBlockSizeBytes() {
        return blockSizeBytes;
    }

    public long getFreeBytes() {
        return freeBytes;
    }



}