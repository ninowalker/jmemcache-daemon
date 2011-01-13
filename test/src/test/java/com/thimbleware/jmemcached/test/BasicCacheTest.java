package com.thimbleware.jmemcached.test;

import static com.thimbleware.jmemcached.LocalCacheElement.Now;
import com.thimbleware.jmemcached.*;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;

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
        Key testKey = new Key(ChannelBuffers.wrappedBuffer("12345678".getBytes()));
        String testvalue = "87654321";

        LocalCacheElement element = new LocalCacheElement(testKey, 0, NO_EXPIRE, 0L);
        element.setData(ChannelBuffers.wrappedBuffer(testvalue.getBytes()));

        // put in cache
        assertEquals(cache.add(element), Cache.StoreResponse.STORED);

        // get result
        CacheElement result = cache.get(testKey)[0];

        // assert no miss
        assertEquals("confirmed no misses", 0, cache.getGetMisses());

        // must be non null and match the original
        assertNotNull("got result", result);
        assertEquals("data length matches", result.size(), element.size());
        assertEquals("data matches", element.getData(), result.getData());
        assertEquals("key matches", element.getKey(), result.getKey());

        assertEquals("size of cache matches element entered", element.size(), cache.getCurrentBytes(), 0);
        assertEquals("cache has 1 element", 1, cache.getCurrentItems());
    }

    @Test
    public void testAddReplace() {
        Key testKey = new Key(ChannelBuffers.wrappedBuffer("12345678".getBytes()));

        String testvalue = "87654321";

        LocalCacheElement element = new LocalCacheElement(testKey, 0, NO_EXPIRE, 0L);
        element.setData(ChannelBuffers.wrappedBuffer(testvalue.getBytes()));

        // put in cache
        assertEquals(cache.add(element), Cache.StoreResponse.STORED);

        // get result
        CacheElement result = cache.get(testKey)[0];

        // assert no miss
        assertEquals("confirmed no misses", 0, cache.getGetMisses());

        // must be non null and match the original
        assertNotNull("got result", result);
        assertEquals("data length matches", result.size(), element.size());
        assertEquals("data matches", element.getData(), result.getData());

        assertEquals("size of cache matches element entered", element.size(), cache.getCurrentBytes(), 0);
        assertEquals("cache has 1 element", 1, cache.getCurrentItems());

        // now replace
        testvalue = "54321";
        element = new LocalCacheElement(testKey, 0, Now(), 0L);
        element.setData(ChannelBuffers.wrappedBuffer(testvalue.getBytes()));

        // put in cache
        assertEquals(cache.replace(element), Cache.StoreResponse.STORED);

        // get result
        result = cache.get(testKey)[0];

        // assert no miss
        assertEquals("confirmed no misses", 0, cache.getGetMisses());

        // must be non null and match the original
        assertNotNull("got result", result);
        assertEquals("data length matches", result.size(), element.size());
        assertEquals("data matches", element.getData(), result.getData());
        assertEquals("key matches", result.getKey(), element.getKey());
        assertEquals("cache has 1 element", 1, cache.getCurrentItems());

    }

    @Test
    public void testReplaceFail() {
        Key testKey = new Key(ChannelBuffers.wrappedBuffer("12345678".getBytes()));

        String testvalue = "87654321";

        LocalCacheElement element = new LocalCacheElement(testKey, 0, NO_EXPIRE, 0L);
        element.setData(ChannelBuffers.wrappedBuffer(testvalue.getBytes()));

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
        Key testKey = new Key(ChannelBuffers.wrappedBuffer("12345678".getBytes()));

        String testvalue = "87654321";

        LocalCacheElement element = new LocalCacheElement(testKey, 0, NO_EXPIRE, 0L);
        element.setData(ChannelBuffers.wrappedBuffer(testvalue.getBytes()));

        // put in cache
        assertEquals(cache.set(element), Cache.StoreResponse.STORED);

        // get result
        CacheElement result = cache.get(testKey)[0];

        // assert no miss
        assertEquals("confirmed no misses", 0, cache.getGetMisses());

        // must be non null and match the original
        assertNotNull("got result", result);
        assertEquals("data length matches", result.size(), element.size());
        assertEquals("data matches", element.getData(), result.getData());
        assertEquals("key matches", result.getKey(), element.getKey());

        assertEquals("size of cache matches element entered", element.size(), cache.getCurrentBytes(), 0);
        assertEquals("cache has 1 element", 1, cache.getCurrentItems());
    }

    @Test
    public void testAddAddFail() {
        Key testKey = new Key(ChannelBuffers.wrappedBuffer("12345678".getBytes()));

        String testvalue = "87654321";

        LocalCacheElement element = new LocalCacheElement(testKey, 0, NO_EXPIRE, 0L);
        element.setData(ChannelBuffers.wrappedBuffer(testvalue.getBytes()));

        // put in cache
        assertEquals(cache.add(element), Cache.StoreResponse.STORED);

        // put in cache again and fail
        assertEquals(cache.add(element), Cache.StoreResponse.NOT_STORED);

        assertEquals("size of cache matches single element entered", element.size(), cache.getCurrentBytes(), 0);
        assertEquals("cache has only 1 element", 1, cache.getCurrentItems());
    }

    @Test
    public void testAddFlush() {
        Key testKey = new Key(ChannelBuffers.wrappedBuffer("12345678".getBytes()));

        String testvalue = "87654321";

        LocalCacheElement element = new LocalCacheElement(testKey, 0, NO_EXPIRE, 0L);
        element.setData(ChannelBuffers.wrappedBuffer(testvalue.getBytes()));

        // put in cache, then flush
        cache.add(element);

        cache.flush_all();

        assertEquals("size of cache matches is empty after flush", 0, cache.getCurrentBytes());
        assertEquals("cache has no elements after flush", 0, cache.getCurrentItems());
    }

    @Test
    public void testSetAndIncrement() {
        Key testKey = new Key(ChannelBuffers.wrappedBuffer("12345678".getBytes()));

        String testvalue = "1";

        LocalCacheElement element = new LocalCacheElement(testKey, 0, NO_EXPIRE, 0L);
        element.setData(ChannelBuffers.wrappedBuffer(testvalue.getBytes()));

        // put in cache
        assertEquals(cache.set(element), Cache.StoreResponse.STORED);

        // increment
        assertEquals("value correctly incremented", (Integer)2, cache.get_add(testKey, 1));

        // increment by more
        assertEquals("value correctly incremented", (Integer)7, cache.get_add(testKey, 5));

        // decrement
        assertEquals("value correctly decremented", (Integer)2, cache.get_add(testKey, -5));
    }


    @Test
    public void testSetAndAppendPrepend() {
        Key testKey = new Key(ChannelBuffers.wrappedBuffer("12345678".getBytes()));

        String testvalue = "1";

        LocalCacheElement element = new LocalCacheElement(testKey, 0, NO_EXPIRE, 0L);
        element.setData(ChannelBuffers.wrappedBuffer(testvalue.getBytes()));

        // put in cache
        assertEquals(cache.set(element), Cache.StoreResponse.STORED);

        // increment
        LocalCacheElement appendEl = new LocalCacheElement(testKey, 0, NO_EXPIRE, 0L);
        appendEl.setData(ChannelBuffers.wrappedBuffer(testvalue.getBytes()));
        Cache.StoreResponse append = cache.append(appendEl);
        assertEquals("correct response", append, Cache.StoreResponse.STORED);
        LocalCacheElement[] elements = cache.get(testKey);
        assertEquals("right # elements", 1, elements.length);
        ChannelBuffer data = elements[0].getData();
        assertEquals(ChannelBuffers.wrappedBuffer("11".getBytes()), data);
    }

}
