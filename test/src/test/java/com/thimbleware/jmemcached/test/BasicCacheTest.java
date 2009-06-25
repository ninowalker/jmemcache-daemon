package com.thimbleware.jmemcached.test;

import static com.thimbleware.jmemcached.MCElement.Now;
import com.thimbleware.jmemcached.*;
import com.thimbleware.jmemcached.util.Bytes;
import com.thimbleware.jmemcached.storage.hash.LRUCacheStorageDelegate;
import com.thimbleware.jmemcached.storage.mmap.MemoryMappedBlockStore;
import com.thimbleware.jmemcached.storage.bytebuffer.ByteBufferCacheStorage;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;

/**
 */
@RunWith(Parameterized.class)
public class BasicCacheTest {
    private static final int MAX_BYTES = (int)Bytes.valueOf("32m").bytes();
    private static final int CEILING_SIZE = (int)Bytes.valueOf("4m").bytes();
    private static final int MAX_SIZE = 1000;

    private MemCacheDaemon daemon;
    private int PORT;

    public static enum CacheType {
        MAPPED, LOCAL
    }

    private CacheType cacheType;
    private int blockSize;

    public BasicCacheTest(CacheType cacheType, int blockSize) {
        this.cacheType = cacheType;
        this.blockSize = blockSize;
    }

    @Parameterized.Parameters
    public static Collection regExValues() {
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
    public void testPresence() {
        assertNotNull(daemon.getCache());
        assertEquals("initial cache is empty", 0, daemon.getCache().getCurrentItems());
        assertEquals("initialize maximum size matches max bytes", MAX_BYTES, daemon.getCache().getLimitMaxBytes());
        assertEquals("initialize size is empty", 0, daemon.getCache().getCurrentBytes());
    }

    @Test
    public void testAddGet() {
        String testKey = "12345678";
        String testvalue = "87654321";

        MCElement element = new MCElement(testKey, 0, Now(), testvalue.length());
        element.data = testvalue.getBytes();

        // put in cache
        assertEquals(daemon.getCache().add(element), Cache.StoreResponse.STORED);

        // get result
        MCElement result = daemon.getCache().get(testKey)[0];

        // assert no miss
        assertEquals("confirmed no misses", 0, daemon.getCache().getGetMisses());

        // must be non null and match the original
        assertNotNull("got result", result);
        assertEquals("data length matches", result.dataLength, element.dataLength);
        assertEquals("data matches", new String(element.data), new String(result.data));
        assertEquals("key matches", element.keystring, result.keystring);

        assertEquals("size of cache matches element entered", element.dataLength, daemon.getCache().getCurrentBytes());
        assertEquals("cache has 1 element", 1, daemon.getCache().getCurrentItems());
    }

    @Test
    public void testAddReplace() {
        String testKey = "12345678";
        String testvalue = "87654321";

        MCElement element = new MCElement(testKey, 0, Now(), testvalue.length());
        element.data = testvalue.getBytes();

        // put in cache
        assertEquals(daemon.getCache().add(element), Cache.StoreResponse.STORED);

        // get result
        MCElement result = daemon.getCache().get(testKey)[0];

        // assert no miss
        assertEquals("confirmed no misses", 0, daemon.getCache().getGetMisses());

        // must be non null and match the original
        assertNotNull("got result", result);
        assertEquals("data length matches", result.dataLength, element.dataLength);
        assertEquals("data matches", new String(element.data), new String(result.data));

        assertEquals("size of cache matches element entered", element.dataLength, daemon.getCache().getCurrentBytes());
        assertEquals("cache has 1 element", 1, daemon.getCache().getCurrentItems());

        // now replace
        testvalue = "54321";
        element = new MCElement(testKey, 0, Now(), testvalue.length());
        element.data = testvalue.getBytes();

        // put in cache
        assertEquals(daemon.getCache().replace(element), Cache.StoreResponse.STORED);

        // get result
        result = daemon.getCache().get(testKey)[0];

        // assert no miss
        assertEquals("confirmed no misses", 0, daemon.getCache().getGetMisses());

        // must be non null and match the original
        assertNotNull("got result", result);
        assertEquals("data length matches", result.dataLength, element.dataLength);
        assertEquals("data matches", new String(element.data), new String(result.data));
        assertEquals("key matches", result.keystring, element.keystring);
        assertEquals("cache has 1 element", 1, daemon.getCache().getCurrentItems());

    }

    @Test
    public void testReplaceFail() {
        String testKey = "12345678";
        String testvalue = "87654321";

        MCElement element = new MCElement(testKey, 0, Now(), testvalue.length());
        element.data = testvalue.getBytes();

        // put in cache
        assertEquals(daemon.getCache().replace(element), Cache.StoreResponse.NOT_STORED);

        // get result
        MCElement result = daemon.getCache().get(testKey)[0];

        // assert miss
        assertEquals("confirmed no misses", 1, daemon.getCache().getGetMisses());

        assertEquals("cache is empty", 0, daemon.getCache().getCurrentItems());
        assertEquals("maximum size matches max bytes", MAX_BYTES, daemon.getCache().getLimitMaxBytes());
        assertEquals("size is empty", 0, daemon.getCache().getCurrentBytes());
    }

    @Test
    public void testSet() {
        String testKey = "12345678";
        String testvalue = "87654321";

        MCElement element = new MCElement(testKey, 0, Now(), testvalue.length());
        element.data = testvalue.getBytes();

        // put in cache
        assertEquals(daemon.getCache().set(element), Cache.StoreResponse.STORED);

        // get result
        MCElement result = daemon.getCache().get(testKey)[0];

        // assert no miss
        assertEquals("confirmed no misses", 0, daemon.getCache().getGetMisses());

        // must be non null and match the original
        assertNotNull("got result", result);
        assertEquals("data length matches", result.dataLength, element.dataLength);
        assertEquals("data matches", new String(element.data), new String(result.data));
        assertEquals("key matches", result.keystring, element.keystring);

        assertEquals("size of cache matches element entered", element.dataLength, daemon.getCache().getCurrentBytes());
        assertEquals("cache has 1 element", 1, daemon.getCache().getCurrentItems());
    }

    @Test
    public void testAddAddFail() {
        String testKey = "12345678";
        String testvalue = "87654321";

        MCElement element = new MCElement(testKey, 0, Now(), testvalue.length());
        element.data = testvalue.getBytes();

        // put in cache
        assertEquals(daemon.getCache().add(element), Cache.StoreResponse.STORED);

        // put in cache again and fail
        assertEquals(daemon.getCache().add(element), Cache.StoreResponse.NOT_STORED);

        assertEquals("size of cache matches single element entered", element.dataLength, daemon.getCache().getCurrentBytes());
        assertEquals("cache has only 1 element", 1, daemon.getCache().getCurrentItems());
    }

    @Test
    public void testAddFlush() {
        String testKey = "12345678";
        String testvalue = "87654321";

        MCElement element = new MCElement(testKey, 0, Now(), testvalue.length());
        element.data = testvalue.getBytes();

        // put in cache, then flush
        daemon.getCache().add(element);

        daemon.getCache().flush_all();

        assertEquals("size of cache matches is empty after flush", 0, daemon.getCache().getCurrentBytes());
        assertEquals("cache has no elements after flush", 0, daemon.getCache().getCurrentItems());
    }

    @Test
    public void testSetAndIncrement() {
        String testKey = "12345678";
        String testvalue = "1";

        MCElement element = new MCElement(testKey, 0, Now(), testvalue.length());
        element.data = testvalue.getBytes();

        // put in cache
        assertEquals(daemon.getCache().set(element), Cache.StoreResponse.STORED);

        // increment
        assertEquals("value correctly incremented", daemon.getCache().get_add(testKey, 1), (Integer)2);

        // increment by more
        assertEquals("value correctly incremented", daemon.getCache().get_add(testKey, 5), (Integer)7);

        // decrement
        assertEquals("value correctly decremented", daemon.getCache().get_add(testKey, -5), (Integer)2);
    }


}
