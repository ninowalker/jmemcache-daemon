package com.thimbleware.jmemcached;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;

import com.thimbleware.jmemcached.storage.mmap.MemoryMappedBlockStore;
import com.thimbleware.jmemcached.storage.bytebuffer.Region;

/**
 */
public class MemoryMappedBlockStoreTest {
    private MemoryMappedBlockStore bs;

    @Before
    public void setup() throws IOException {
        bs = new MemoryMappedBlockStore.MemoryMappedBlockStoreFactory().manufacture(40000, 8);
    }

    @After
    public void teardown() throws IOException {
        bs.close();
    }

    @Test
    public void writeBlocks() {
        for (int i = 0; i < 1800; i++){
            ChannelBuffer sentData = ChannelBuffers.buffer(3);
            sentData.writeByte('h');
            sentData.writeByte('i');
            sentData.writeByte('\n');

            long before = bs.getFreeBytes();
            Region region = bs.alloc(3, sentData);

            long after = bs.getFreeBytes();
            assertEquals("after allocating region, free space available", after, before - region.usedBlocks * 8);

            assertNotNull("region returned", region);
            assertTrue("size is less or equal to region size", region.size <= region.usedBlocks * 8);
            assertTrue("region is valid", region.valid);
            assertEquals("rounded up to nearest region boundary", region.usedBlocks * 8, 8L);

            ChannelBuffer receivedData = bs.get(region);
            assertEquals("region data matches",  sentData, receivedData);

            if (i % 5 == 0) {
                before = bs.getFreeBytes();
                bs.free(region);
                after = bs.getFreeBytes();
                assertEquals("after freeing region, free space available", before + region.usedBlocks * 8, after);
                assertFalse("after freeing, region is not valid", region.valid);
            }

        }

    }
}
