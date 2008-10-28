package com.thimbleware.jmemcached.test;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.LRUCacheStorageDelegate;
import com.thimbleware.jmemcached.Cache;
import com.thimbleware.jmemcached.MCElement;
import static com.thimbleware.jmemcached.MCElement.*;

import java.net.InetSocketAddress;
import java.io.IOException;

import junit.framework.Assert;
import static junit.framework.Assert.*;

/**
 */
public class BasicSetupTest {
    private static final int MAX_BYTES = 1024 * 1024 * 1024;
    private static final int CEILING_SIZE = 1024 * 1024;
    private MemCacheDaemon daemon;
    private static final int MAX_SIZE = 1000;

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

        MCElement element = new MCElement(testKey, "", Now(), testvalue.length());
        element.data = testvalue.getBytes();

        // put in cache
        assertEquals(daemon.getCache().add(element), Cache.StoreResponse.STORED);

        // get result
        MCElement result = daemon.getCache().get(testKey);

        // assert no miss
        assertEquals("confirmed no misses", 0, daemon.getCache().getGetMisses());

        // must be non null and match the original
        assertNotNull("got result", result);
        assertEquals("data length matches", result.data_length, element.data_length);
        assertEquals("data matches", result.data, element.data);
        assertEquals("key matches", result.keystring, element.keystring);

        assertEquals("size of cache matches element entered", element.data_length, daemon.getCache().getCurrentBytes());
        assertEquals("cache has 1 element", 1, daemon.getCache().getCurrentItems());
    }

    @Test
    public void testAddReplace() {
        String testKey = "12345678";
        String testvalue = "87654321";

        MCElement element = new MCElement(testKey, "", Now(), testvalue.length());
        element.data = testvalue.getBytes();

        // put in cache
        assertEquals(daemon.getCache().add(element), Cache.StoreResponse.STORED);

        // get result
        MCElement result = daemon.getCache().get(testKey);

        // assert no miss
        assertEquals("confirmed no misses", 0, daemon.getCache().getGetMisses());

        // must be non null and match the original
        assertNotNull("got result", result);
        assertEquals("data length matches", result.data_length, element.data_length);
        assertEquals("data matches", result.data, element.data);

        assertEquals("size of cache matches element entered", element.data_length, daemon.getCache().getCurrentBytes());
        assertEquals("cache has 1 element", 1, daemon.getCache().getCurrentItems());

        // now replace
        testvalue = "54321";
        element = new MCElement(testKey, "", Now(), testvalue.length());
        element.data = testvalue.getBytes();

        // put in cache
        assertEquals(daemon.getCache().replace(element), Cache.StoreResponse.STORED);

        // get result
        result = daemon.getCache().get(testKey);

        // assert no miss
        assertEquals("confirmed no misses", 0, daemon.getCache().getGetMisses());

        // must be non null and match the original
        assertNotNull("got result", result);
        assertEquals("data length matches", result.data_length, element.data_length);
        assertEquals("data matches", result.data, element.data);
        assertEquals("key matches", result.keystring, element.keystring);
        assertEquals("size of cache matches element entered", element.data_length, daemon.getCache().getCurrentBytes());
        assertEquals("cache has 1 element", 1, daemon.getCache().getCurrentItems());

    }

    @Test
    public void testReplaceFail() {
        String testKey = "12345678";
        String testvalue = "87654321";

        MCElement element = new MCElement(testKey, "", Now(), testvalue.length());
        element.data = testvalue.getBytes();

        // put in cache
        assertEquals(daemon.getCache().replace(element), Cache.StoreResponse.NOT_STORED);

        // get result
        MCElement result = daemon.getCache().get(testKey);

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

        MCElement element = new MCElement(testKey, "", Now(), testvalue.length());
        element.data = testvalue.getBytes();

        // put in cache
        assertEquals(daemon.getCache().set(element), Cache.StoreResponse.STORED);

        // get result
        MCElement result = daemon.getCache().get(testKey);

        // assert no miss
        assertEquals("confirmed no misses", 0, daemon.getCache().getGetMisses());

        // must be non null and match the original
        assertNotNull("got result", result);
        assertEquals("data length matches", result.data_length, element.data_length);
        assertEquals("data matches", result.data, element.data);
        assertEquals("key matches", result.keystring, element.keystring);

        assertEquals("size of cache matches element entered", element.data_length, daemon.getCache().getCurrentBytes());
        assertEquals("cache has 1 element", 1, daemon.getCache().getCurrentItems());
    }

    @Test
    public void testAddAddFail() {
        String testKey = "12345678";
        String testvalue = "87654321";

        MCElement element = new MCElement(testKey, "", Now(), testvalue.length());
        element.data = testvalue.getBytes();

        // put in cache
        assertEquals(daemon.getCache().add(element), Cache.StoreResponse.STORED);

        // put in cache again and fail
        assertEquals(daemon.getCache().add(element), Cache.StoreResponse.NOT_STORED);

        assertEquals("size of cache matches single element entered", element.data_length, daemon.getCache().getCurrentBytes());
        assertEquals("cache has only 1 element", 1, daemon.getCache().getCurrentItems());
    }

    @Test
    public void testAddFlush() {
        String testKey = "12345678";
        String testvalue = "87654321";

        MCElement element = new MCElement(testKey, "", Now(), testvalue.length());
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

        MCElement element = new MCElement(testKey, "", Now(), testvalue.length());
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
