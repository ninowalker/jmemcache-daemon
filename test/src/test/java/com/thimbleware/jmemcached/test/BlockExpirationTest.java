package com.thimbleware.jmemcached.test;

import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.thimbleware.jmemcached.*;
import com.thimbleware.jmemcached.storage.mmap.MemoryMappedBlockStore;
import com.thimbleware.jmemcached.storage.mmap.MemoryMappedCacheStorage;
import static com.thimbleware.jmemcached.MCElement.Now;
import static junit.framework.Assert.*;

/**
 */
public class BlockExpirationTest {
    private static final int MAX_BYTES = 1024 * 1024 * 1024;
    private static final int CEILING_SIZE = 1024 * 1024;
    private static final int MAX_SIZE = 1000;

    private MemCacheDaemon daemon;
    private static final int BLOCK_SIZE = 8;

    @Before
    public void setup() throws IOException {
        // create daemon and start it
        daemon = new MemCacheDaemon();
        MemoryMappedCacheStorage cacheStorage = new MemoryMappedCacheStorage(
                new MemoryMappedBlockStore(MAX_BYTES, "block_store.dat", BLOCK_SIZE), MAX_SIZE, CEILING_SIZE);
        daemon.setCache(new Cache(cacheStorage));
        daemon.setAddr(new InetSocketAddress("localhost", 12345));
        daemon.setVerbose(false);
        daemon.start();
    }

    @After
    public void teardown() {
        daemon.stop();
    }

    @Test
    public void testExpire() {
        // max MAX_SIZE items in cache, so create fillSize items and then verify that only a MAX_SIZE are ever in the cache
        int fillSize = 2000;

        for (int i = 0; i < fillSize; i++) {
            MCElement el = createElement("" + i , "x");

            assertEquals(daemon.getCache().add(el), Cache.StoreResponse.STORED);

            // verify that the size of the cache is correct
            int maximum = i < MAX_SIZE ? i + 1 : MAX_SIZE;

            assertEquals("correct number of bytes stored", maximum * BLOCK_SIZE, daemon.getCache().getCurrentBytes());
            assertEquals("correct number of items stored", maximum, daemon.getCache().getCurrentItems());
        }

        // verify that the size of the cache is correct
        assertEquals(daemon.getCache().getCurrentBytes(), MAX_SIZE * BLOCK_SIZE);

        // verify that only the last 1000 items are actually physically in there
        for (int i = 0; i < fillSize; i++) {
            MCElement result = daemon.getCache().get("" + i);
            if (i < MAX_SIZE) {
                assertTrue(result == null);
            } else {
                assertEquals(result.keystring, "" + i);
            }
        }
        assertEquals("correct number of cache misses", fillSize - MAX_SIZE, daemon.getCache().getGetMisses());
        assertEquals("correct number of cache hits", MAX_SIZE, daemon.getCache().getGetHits());
    }

    private MCElement createElement(String testKey, String testvalue) {
        MCElement element = new MCElement(testKey, "", Now(), testvalue.length());
        element.data = testvalue.getBytes();

        return element;
    }
}