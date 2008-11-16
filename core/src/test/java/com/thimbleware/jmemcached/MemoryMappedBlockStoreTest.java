package com.thimbleware.jmemcached;

import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;

import com.thimbleware.jmemcached.storage.mmap.MemoryMappedBlockStore;

/**
 */
public class MemoryMappedBlockStoreTest {
    private MemoryMappedBlockStore bs;

    @Before
    public void setup() throws IOException {
        bs = new MemoryMappedBlockStore(400000, "test.dat", 8);
    }

    @After
    public void teardown() throws IOException {
        bs.close();
    }

    @Test
    public void writeBlocks() {
        for (int i = 0; i < 1800; i++){
            byte[] data = new byte[256];
            data[0] = 'h';
            data[1] = 'i';
            data[2] = '\n';

            long before = bs.getFreeBytes();
            MemoryMappedBlockStore.Region region = bs.alloc(250, data);

            long after = bs.getFreeBytes();
            assertEquals("after allocating region, free space available", after, before - region.physicalSize);

            assertNotNull("region returned", region);
            assertTrue("size is less or equal to region size", region.size <= region.physicalSize);
            assertTrue("region is valid", region.valid);
            assertEquals("rounded up to nearest region boundary", region.physicalSize, 256, 0);

            byte[] altData = new byte[256];
            region.buffer.get(altData);
            assertEquals("region data matches",  new String(data), new String(altData));
            
            if (i % 5 == 0) {
                before = bs.getFreeBytes();
                bs.free(region);
                after = bs.getFreeBytes();
                assertEquals("after freeing region, free space available", before + region.physicalSize, after);
                assertFalse("after freeing, region is not valid", region.valid);
            }

        }
        
    }
}
