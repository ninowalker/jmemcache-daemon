package com.thimbleware.jmemcached.storage.mmap;

import java.io.*;
import java.nio.*;
import static java.nio.channels.FileChannel.MapMode.*;
import java.util.*;

/**
 * Memory mapped block storage mechanism with a free-list maintained by TreeMap
 *
 * Allows memory for storage to be mapped outside of the VM's main memory, and outside the purvey
 * of the GC.
 *
 * Should offer O(Log(N)) search and free of blocks.
 */
public final class MemoryMappedBlockStore {

    private RandomAccessFile fileStorage;
    private MappedByteBuffer storage;

    private String fileName;

    private long freeBytes;

    private final long storeSizeBytes;
    private final int blockSizeBytes;

    private SortedMap<Long, Long> freeList;
    private Set<Region> currentRegions;


    /**
     * Exception thrown on inability to allocate a new block
     */
    public static class BadAllocationException extends RuntimeException {
        public BadAllocationException(String s) {
            super(s);
        }
    }

    /**
     * Represents a number of allocated blocks in the store
     */
    public static final class Region {
        /**
         * Size in bytes of the requested area
         */
        public final int size;

        /**
         * Actual size the data rounded up to the nearest block.
         */
        public final long physicalSize;

        /**
         * Offset into the memory region
         */
        private final long offset;

        /**
         * NIO buffer for reading and writing bytes from/to the store.
         */
        public final ByteBuffer buffer;

        /**
         * Flag which is true if the region is valid and in use.
         * Set to false on free()
         */
        public boolean valid = false;

        public Region(int size, long physicalSize, long offset, ByteBuffer buffer) {
            this.size = size;
            this.physicalSize = physicalSize;
            this.offset = offset;
            this.buffer = buffer;
            this.valid = true;
        }

        public void setValid(boolean valid) {
            this.valid = valid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Region)) return false;

            Region region = (Region) o;

            if (physicalSize != region.physicalSize) return false;
            if (offset != region.offset) return false;
            if (size != region.size) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = size;
            result = 31 * result + (int) (physicalSize ^ (physicalSize >>> 32));
            result = 31 * result + (int) (offset ^ (offset >>> 32));
            return result;
        }
    }


    /**
     * Rounds up a requested size to the nearest block width.
     * @param size the requested size
     * @param blockSize the block size to use
     * @return the actual mount to use
     */
    private static long roundUp( long size, long blockSize ) {
        return size - 1L + blockSize - (size - 1L) % blockSize;
    }

    /**
     * Construct a new memory mapped block storage against a filename, with a certain size
     * and block size.
     * @param maxBytes the number of bytes to allocate in the file
     * @param fileName the filename to use
     * @param blockSizeBytes the size of a block in the store
     * @throws java.io.IOException thrown on failure to open the store or map the file
     */
    public MemoryMappedBlockStore(long maxBytes, String fileName, int blockSizeBytes) throws IOException {
        // set region size used throughout
        this.blockSizeBytes = blockSizeBytes;

        // set the size of the store in bytes
        storeSizeBytes = maxBytes;

        // the number of free bytes starts out as the entire store
        freeBytes = storeSizeBytes;

        // open the file for read-write
        this.fileName = fileName;
        fileStorage = new RandomAccessFile(fileName, "rw");

        // memory map it out the requested size
        storage = fileStorage.getChannel().map(PRIVATE, 0, maxBytes);

        // clear the buffer
        storage.clear();

        // the free list starts with one giant free region; we create a tree map as index, then set initial values
        // with one empty region.
        freeList = new TreeMap<Long, Long>();
        currentRegions = new HashSet<Region>();
        clear();

    }

    /**
     * Close the store, destroying all data and closing the backing file
     * @throws IOException thrown on failure to close file
     */
    public void close() throws IOException {
        clearRegions();

        // force a sync of the file, writes all data to disk; in fact however we'll never use this data,
        // but the option remains for future versions, just uncomment
//        storage.force();

        // close the actual file
        fileStorage.close();

        // delete the file; it is no longer of any use
        new File(fileName).delete();
    }

    /**
     * Allocate a region in the
     * @param desiredSize
     * @param data
     * @return
     */
    public Region alloc(int desiredSize, byte[] data) {
        final long desiredBlockSize = roundUp(desiredSize, blockSizeBytes);

        /** Find a free entry.  headMap should give us O(log(N)) search
         *  time.
         */
        Iterator<Map.Entry<Long,Long>> entryIterator = freeList.tailMap(desiredBlockSize).entrySet().iterator();

        if (!entryIterator.hasNext()) {
            /** No more room.
             *  We now have three options - we can throw error, grow the store, or compact
             *  the holes in the existing one.
             *
             *  We usually want to grow (fast), and compact (slow) only if necessary
             *  (i.e. some periodic interval
             *  has been reached or a maximum store size constant hit.)
             *
             *  Incremental compaction is a Nice To Have.
             *
             *  TODO implement compaction routine; throw; in theory the cache on top of us should be removing elements before we fill
             */
            //    compact();

            throw new BadAllocationException("unable to allocate room; all blocks consumed");
        } else {
            Map.Entry<Long, Long> freeEntry = entryIterator.next();

            /** Don't let this region overlap a page boundary
             */
            // PUT CODE HERE

            long position = freeEntry.getValue();

            /** Size of the free block to be placed back on the free list
             */
            long newFreeSize = freeEntry.getKey() - desiredBlockSize;

            /** Split the free entry and add the entry to the allocated list
             */
            freeList.remove(freeEntry.getKey());

            /** Add it back to the free list if needed
             */
            if ( newFreeSize > 0L ) {
                freeList.put( newFreeSize, position + desiredBlockSize) ;
            }

            freeBytes -= desiredBlockSize;

            // get the buffer to it

            Buffer buf = storage.position((int) position);
            ((ByteBuffer)buf).put(data);
            buf.rewind();

            Region region = new Region(desiredSize, desiredBlockSize, position, ((ByteBuffer) buf));

            currentRegions.add(region);

            return region;
        }
    }


    public void free(Region region) {
        freeList.put(region.physicalSize, region.offset);
        freeBytes += region.physicalSize;
        region.valid = false;
        currentRegions.remove(region);
    }

    public void clear()
    {
        // mark all blocks invalid
        clearRegions();

        // say goodbye to the region list
        freeList.clear();

        // Add a free entry for the entire thing at the start
        freeList.put(storeSizeBytes, 0L);

        // reset the # of free bytes back to the max size
        freeBytes = storeSizeBytes;
    }

    private void clearRegions() {
        // mark all blocks invalid
        for (Region currentRegion : currentRegions) {
            currentRegion.setValid(false);
        }

        // clear the list of blocks so we're not holding onto them
        currentRegions.clear();
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
