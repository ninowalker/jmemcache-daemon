package com.thimbleware.jmemcached.test;

import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.LRUCacheStorageDelegate;
import com.thimbleware.jmemcached.Cache;
import com.thimbleware.jmemcached.MCElement;
import static com.thimbleware.jmemcached.MCElement.Now;
import junit.framework.Assert;
import static junit.framework.Assert.*;

/**
 */
public class CacheExpirationTest {
    private static final int MAX_BYTES = 1024 * 1024 * 1024;
    private static final int CEILING_SIZE = 1024 * 1024;
    private static final int MAX_SIZE = 1000;

    private MemCacheDaemon daemon;

    @Before
    public void setup() throws IOException {
        // create daemon and start it
        daemon = new MemCacheDaemon();
        LRUCacheStorageDelegate cacheStorage = new LRUCacheStorageDelegate(MAX_SIZE, MAX_BYTES, CEILING_SIZE);
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
            assertEquals("correct number of bytes stored", i < MAX_SIZE ? i + 1 : MAX_SIZE, daemon.getCache().getCurrentBytes());
            assertEquals("correct number of items stored", i < MAX_SIZE ? i + 1 : MAX_SIZE, daemon.getCache().getCurrentItems());
        }

        // verify that the size of the cache is correct
        assertEquals(daemon.getCache().getCurrentBytes(), MAX_SIZE);

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
