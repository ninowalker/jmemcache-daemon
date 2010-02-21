package com.thimbleware.jmemcached.storage.mmap;

import com.thimbleware.jmemcached.storage.bytebuffer.BlockStoreFactory;
import com.thimbleware.jmemcached.storage.bytebuffer.ByteBufferBlockStore;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import static java.nio.channels.FileChannel.MapMode.*;

/**
 * Memory mapped block storage mechanism with a free-list maintained by TreeMap
 *
 * Allows memory for storage to be mapped outside of the VM's main memory, and outside the purvey
 * of the GC.
 *
 * Should offer O(Log(N)) search and free of blocks.
 */
public final class MemoryMappedBlockStore extends ByteBufferBlockStore {

    private File physicalFile;
    private RandomAccessFile fileStorage;
    private static final MemoryMappedBlockStoreFactory MEMORY_MAPPED_BLOCK_STORE_FACTORY = new MemoryMappedBlockStoreFactory();

    /**
     * Construct a new memory mapped block storage against a filename, with a certain size
     * and block size.
     * @param maxBytes the number of bytes to allocate in the file
     * @param file the file to use
     * @param blockSizeBytes the size of a block in the store
     * @throws java.io.IOException thrown on failure to open the store or map the file
     */
    private MemoryMappedBlockStore(long maxBytes, File file, int blockSizeBytes) throws IOException {
        super(blockSizeBytes);
        storageBuffer = getMemoryMappedFileStorage(maxBytes, file);
        initialize(storageBuffer.capacity());
    }

    public static BlockStoreFactory getFactory() {
        return MEMORY_MAPPED_BLOCK_STORE_FACTORY;
    }

    private MappedByteBuffer getMemoryMappedFileStorage(long maxBytes, File file) throws IOException {
        this.physicalFile = file;

        // open the file for read-write
        fileStorage = new RandomAccessFile(file, "rw");
        fileStorage.seek(maxBytes);

        return fileStorage.getChannel().map(PRIVATE, 0, maxBytes);
    }

    @Override
    protected void freeResources() throws IOException {
        super.freeResources();

        // close the actual file
        fileStorage.close();

        // delete the file; it is no longer of any use
        physicalFile.delete();

        physicalFile = null;
        fileStorage = null;
    }


    public static class MemoryMappedBlockStoreFactory implements BlockStoreFactory<MemoryMappedBlockStore> {

        public MemoryMappedBlockStore manufacture(long sizeBytes, int blockSizeBytes) {
            try {
                final File tempFile = File.createTempFile("jmemcached", "blockStore");
                tempFile.deleteOnExit();
                return new MemoryMappedBlockStore(sizeBytes, tempFile, blockSizeBytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
