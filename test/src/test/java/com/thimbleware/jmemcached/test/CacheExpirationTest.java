package com.thimbleware.jmemcached.test;

import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Arrays;

import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.storage.hash.LRUCacheStorageDelegate;
import com.thimbleware.jmemcached.storage.mmap.MemoryMappedBlockStore;
import com.thimbleware.jmemcached.storage.bytebuffer.ByteBufferCacheStorage;
import com.thimbleware.jmemcached.Cache;
import com.thimbleware.jmemcached.MCElement;
import com.thimbleware.jmemcached.util.Bytes;
import static com.thimbleware.jmemcached.MCElement.Now;
import static junit.framework.Assert.*;

/**
 */
@RunWith(Parameterized.class)
public class CacheExpirationTest {
    private static final int MAX_BYTES = (int) Bytes.valueOf("32m").bytes();
    private static final int CEILING_SIZE = (int)Bytes.valueOf("4m").bytes();
    private static final int MAX_SIZE = 1000;

    private MemCacheDaemon daemon;
    private int PORT;

    public static enum CacheType {
        MAPPED, LOCAL
    }

    private CacheType cacheType;
    private int blockSize;

    public CacheExpirationTest(CacheType cacheType, int blockSize) {
        this.cacheType = cacheType;
        this.blockSize = blockSize;
    }

    @Parameterized.Parameters
    public static Collection blockSizeValues() {
        return Arrays.asList(new Object[][] {
                {CacheType.LOCAL, 1 },
                {CacheType.MAPPED, 8 }});
    }


    @Before
    public void setup() throws IOException {
        // create daemon and start it
        daemon = new MemCacheDaemon();
        if (cacheType == CacheType.LOCAL) {
            LRUCacheStorageDelegate cacheStorage = new LRUCacheStorageDelegate(MAX_SIZE, MAX_BYTES, CEILING_SIZE);
            daemon.setCache(new Cache(cacheStorage));
        } else {
            ByteBufferCacheStorage cacheStorage = new ByteBufferCacheStorage(
                    new MemoryMappedBlockStore(MAX_BYTES, "block_store.dat", blockSize), MAX_SIZE, CEILING_SIZE);
            daemon.setCache(new Cache(cacheStorage));
        }
        PORT = AvailablePortFinder.getNextAvailable();
        daemon.setAddr(new InetSocketAddress("localhost", PORT));
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

        long totalSize = 0;
        for (int i = 0; i < fillSize; i++) {
            String testvalue = i + "x";
            MCElement el = createElement("" + i , testvalue);

            totalSize += MemoryMappedBlockStore.roundUp(blockSize, testvalue.length());

            assertEquals(daemon.getCache().add(el), Cache.StoreResponse.STORED);

            // verify that the size of the cache is correct
            int maximum = i < MAX_SIZE ? i + 1 : MAX_SIZE;

//            assertEquals("correct number of bytes stored", totalSize, daemon.getCache().getCurrentBytes());
            assertEquals("correct number of items stored", maximum, daemon.getCache().getCurrentItems());
        }

        // verify that the size of the cache is correct
        assertEquals("maximum items stored", MAX_SIZE, daemon.getCache().getCurrentItems());

        // verify that only the last 1000 items are actually physically in there
        for (int i = 0; i < fillSize; i++) {
            MCElement result = daemon.getCache().get("" + i)[0];
            if (i < MAX_SIZE) {
                assertTrue(result == null);
            } else {
                assertNotNull(result);
                assertNotNull(i + "th result is there", result.keystring);
                assertEquals("key matches" , "" + i, result.keystring);
                assertEquals(new String(result.data), i + "x");
            }
        }
        assertEquals("correct number of cache misses", fillSize - MAX_SIZE, daemon.getCache().getGetMisses());
        assertEquals("correct number of cache hits", MAX_SIZE, daemon.getCache().getGetHits());
    }

    private MCElement createElement(String testKey, String testvalue) {
        MCElement element = new MCElement(testKey, 0, Now(), testvalue.length());
        element.data = testvalue.getBytes();

        return element;
    }
}
