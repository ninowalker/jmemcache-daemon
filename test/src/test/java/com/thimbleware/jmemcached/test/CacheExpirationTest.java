package com.thimbleware.jmemcached.test;

import com.thimbleware.jmemcached.*;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Arrays;

import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;
import com.thimbleware.jmemcached.storage.mmap.MemoryMappedBlockStore;
import com.thimbleware.jmemcached.storage.ConcurrentSizedBlockStorageMap;
import com.thimbleware.jmemcached.storage.ConcurrentSizedMap;
import com.thimbleware.jmemcached.util.Bytes;
import static com.thimbleware.jmemcached.LocalCacheElement.Now;
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
                {CacheType.MAPPED, 4 }});
    }


    @Before
    public void setup() throws IOException {
        // create daemon and start it
        daemon = new MemCacheDaemon();
        ConcurrentSizedMap<String, LocalCacheElement> cacheStorage;
        if (cacheType == CacheType.LOCAL) {
            cacheStorage = ConcurrentLinkedHashMap.create(ConcurrentLinkedHashMap.EvictionPolicy.FIFO, MAX_SIZE, MAX_BYTES);
            daemon.setCache(new CacheImpl(cacheStorage));
        } else {
            cacheStorage = new ConcurrentSizedBlockStorageMap(
                    new MemoryMappedBlockStore(MAX_BYTES, "block_store.dat", blockSize), CEILING_SIZE, MAX_SIZE);
            daemon.setCache(new CacheImpl(cacheStorage));
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

        for (int i = 0; i < fillSize; i++) {
            String testvalue = i + "x";
            LocalCacheElement el = createElement("" + i , testvalue);

            assertEquals(daemon.getCache().add(el), Cache.StoreResponse.STORED);

            // verify that the size of the cache is correct
            int maximum = i < MAX_SIZE ? i + 1 : MAX_SIZE;

            assertEquals("correct number of items stored", maximum, daemon.getCache().getCurrentItems());
        }

        // verify that the size of the cache is correct
        assertEquals("maximum items stored", MAX_SIZE, daemon.getCache().getCurrentItems());

        // verify that only the last 1000 items are actually physically in there
        for (int i = 0; i < fillSize; i++) {
            CacheElement result = daemon.getCache().get("" + i)[0];
            if (i < MAX_SIZE) {
                assertTrue(i + "th result absence", result == null);
            } else {
                assertNotNull(i + "th result presence", result);
                assertNotNull(i + "th result's presence", result.getKeystring());
                assertEquals("key matches" , "" + i, result.getKeystring());
                assertEquals(new String(result.getData()), i + "x");
            }
        }
        assertEquals("correct number of cache misses", fillSize - MAX_SIZE, daemon.getCache().getGetMisses());
        assertEquals("correct number of cache hits", MAX_SIZE, daemon.getCache().getGetHits());
    }

    private LocalCacheElement createElement(String testKey, String testvalue) {
        LocalCacheElement element = new LocalCacheElement(testKey, 0, Now(), testvalue.length());
        element.setData(testvalue.getBytes());

        return element;
    }
}
