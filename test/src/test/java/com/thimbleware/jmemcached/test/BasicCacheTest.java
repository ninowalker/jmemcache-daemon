package com.thimbleware.jmemcached.test;

import static com.thimbleware.jmemcached.LocalCacheElement.Now;
import com.thimbleware.jmemcached.*;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 */
@RunWith(Parameterized.class)
public class BasicCacheTest extends AbstractCacheTest {

    public static final int NO_EXPIRE = 0;

    public BasicCacheTest(CacheType cacheType, int blockSize, ProtocolMode protocolMode) {
        super(cacheType, blockSize, protocolMode);
    }

    @Test
    public void testPresence() {
        assertNotNull(cache);
        assertEquals("initial cache is empty", 0, cache.getCurrentItems());
        assertEquals("initialize maximum size matches max bytes", MAX_BYTES, cache.getLimitMaxBytes());
        assertEquals("initialize size is empty", 0, cache.getCurrentBytes());
    }

    @Test
    public void testAddGet() {
        String testKey = "12345678";
        String testvalue = "87654321";

        LocalCacheElement element = new LocalCacheElement(testKey, 0, NO_EXPIRE);
        element.setData(testvalue.getBytes());

        // put in cache
        assertEquals(cache.add(element), Cache.StoreResponse.STORED);

        // get result
        CacheElement result = cache.get(testKey)[0];

        // assert no miss
        assertEquals("confirmed no misses", 0, cache.getGetMisses());

        // must be non null and match the original
        assertNotNull("got result", result);
        assertEquals("data length matches", result.getData().length, element.getData().length);
        assertEquals("data matches", new String(element.getData()), new String(result.getData()));
        assertEquals("key matches", element.getKeystring(), result.getKeystring());

        assertEquals("size of cache matches element entered", element.getData().length, cache.getCurrentBytes(), 0);
        assertEquals("cache has 1 element", 1, cache.getCurrentItems());
    }

    @Test
    public void testAddReplace() {
        String testKey = "12345678";
        String testvalue = "87654321";

        LocalCacheElement element = new LocalCacheElement(testKey, 0, NO_EXPIRE);
        element.setData(testvalue.getBytes());

        // put in cache
        assertEquals(cache.add(element), Cache.StoreResponse.STORED);

        // get result
        CacheElement result = cache.get(testKey)[0];

        // assert no miss
        assertEquals("confirmed no misses", 0, cache.getGetMisses());

        // must be non null and match the original
        assertNotNull("got result", result);
        assertEquals("data length matches", result.getData().length, element.getData().length);
        assertEquals("data matches", new String(element.getData()), new String(result.getData()));

        assertEquals("size of cache matches element entered", element.getData().length, cache.getCurrentBytes(), 0);
        assertEquals("cache has 1 element", 1, cache.getCurrentItems());

        // now replace
        testvalue = "54321";
        element = new LocalCacheElement(testKey, 0, Now());
        element.setData(testvalue.getBytes());

        // put in cache
        assertEquals(cache.replace(element), Cache.StoreResponse.STORED);

        // get result
        result = cache.get(testKey)[0];

        // assert no miss
        assertEquals("confirmed no misses", 0, cache.getGetMisses());

        // must be non null and match the original
        assertNotNull("got result", result);
        assertEquals("data length matches", result.getData().length, element.getData().length);
        assertEquals("data matches", new String(element.getData()), new String(result.getData()));
        assertEquals("key matches", result.getKeystring(), element.getKeystring());
        assertEquals("cache has 1 element", 1, cache.getCurrentItems());

    }

    @Test
    public void testReplaceFail() {
        String testKey = "12345678";
        String testvalue = "87654321";

        LocalCacheElement element = new LocalCacheElement(testKey, 0, NO_EXPIRE);
        element.setData(testvalue.getBytes());

        // put in cache
        assertEquals(cache.replace(element), Cache.StoreResponse.NOT_STORED);

        // get result
        CacheElement result = cache.get(testKey)[0];

        // assert miss
        assertEquals("confirmed no misses", 1, cache.getGetMisses());

        assertEquals("cache is empty", 0, cache.getCurrentItems());
        assertEquals("maximum size matches max bytes", MAX_BYTES, cache.getLimitMaxBytes());
        assertEquals("size is empty", 0, cache.getCurrentBytes());
    }

    @Test
    public void testSet() {
        String testKey = "12345678";
        String testvalue = "87654321";

        LocalCacheElement element = new LocalCacheElement(testKey, 0, NO_EXPIRE);
        element.setData(testvalue.getBytes());

        // put in cache
        assertEquals(cache.set(element), Cache.StoreResponse.STORED);

        // get result
        CacheElement result = cache.get(testKey)[0];

        // assert no miss
        assertEquals("confirmed no misses", 0, cache.getGetMisses());

        // must be non null and match the original
        assertNotNull("got result", result);
        assertEquals("data length matches", result.getData().length, element.getData().length);
        assertEquals("data matches", new String(element.getData()), new String(result.getData()));
        assertEquals("key matches", result.getKeystring(), element.getKeystring());

        assertEquals("size of cache matches element entered", element.getData().length, cache.getCurrentBytes(), 0);
        assertEquals("cache has 1 element", 1, cache.getCurrentItems());
    }

    @Test
    public void testAddAddFail() {
        String testKey = "12345678";
        String testvalue = "87654321";

        LocalCacheElement element = new LocalCacheElement(testKey, 0, NO_EXPIRE);
        element.setData(testvalue.getBytes());

        // put in cache
        assertEquals(cache.add(element), Cache.StoreResponse.STORED);

        // put in cache again and fail
        assertEquals(cache.add(element), Cache.StoreResponse.NOT_STORED);

        assertEquals("size of cache matches single element entered", element.getData().length, cache.getCurrentBytes(), 0);
        assertEquals("cache has only 1 element", 1, cache.getCurrentItems());
    }

    @Test
    public void testAddFlush() {
        String testKey = "12345678";
        String testvalue = "87654321";

        LocalCacheElement element = new LocalCacheElement(testKey, 0, NO_EXPIRE);
        element.setData(testvalue.getBytes());

        // put in cache, then flush
        cache.add(element);

        cache.flush_all();

        assertEquals("size of cache matches is empty after flush", 0, cache.getCurrentBytes());
        assertEquals("cache has no elements after flush", 0, cache.getCurrentItems());
    }

    @Test
    public void testSetAndIncrement() {
        String testKey = "12345678";
        String testvalue = "1";

        LocalCacheElement element = new LocalCacheElement(testKey, 0, NO_EXPIRE);
        element.setData(testvalue.getBytes());

        // put in cache
        assertEquals(cache.set(element), Cache.StoreResponse.STORED);

        // increment
        assertEquals("value correctly incremented", (Integer)2, cache.get_add(testKey, 1));

        // increment by more
        assertEquals("value correctly incremented", (Integer)7, cache.get_add(testKey, 5));

        // decrement
        assertEquals("value correctly decremented", (Integer)2, cache.get_add(testKey, -5));
    }


}
