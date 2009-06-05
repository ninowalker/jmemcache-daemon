package com.thimbleware.jmemcached.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

import net.spy.memcached.MemcachedClient;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.apache.mina.util.AvailablePortFinder;

import com.thimbleware.jmemcached.Cache;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.storage.hash.LRUCacheStorageDelegate;

/**
 * Test basic functionality of Spy Memcached client 2.3 to JMemcached
 * seee http://thimbleware.com/projects/jmemcached/ticket/1
 * @author martin.grotzke@javakaffee.de
 */
public class SpyMemcached23Test {

    private int PORT;

    private MemCacheDaemon _daemon;
    private MemcachedClient _client;

    @Before
    public void setUp() throws Exception {
        PORT = AvailablePortFinder.getNextAvailable();
        final InetSocketAddress address = new InetSocketAddress( "localhost", PORT);
        _daemon = createDaemon( address );
        _daemon.start(); // hello side effects
        _client = new MemcachedClient( Arrays.asList( address ) );
    }

    @After
    public void tearDown() throws Exception {
        _daemon.stop();
    }

    @Test
    public void testPresence() {
        assertNotNull(_daemon.getCache());
        assertEquals("initial cache is empty", 0, _daemon.getCache().getCurrentItems());
        assertEquals("initialize size is empty", 0, _daemon.getCache().getCurrentBytes());
    }

    @Test
    public void testGetSet() throws IOException, InterruptedException {
        _client.set( "foo", 5000, "bar" );
        Assert.assertEquals( "bar", _client.get( "foo" ) );
    }

    @Test
    public void testBulkGet() throws IOException, InterruptedException {
        ArrayList<String> allStrings = new ArrayList<String>();
        for (int i = 0; i < 10000; i++) {
            _client.set("foo" + i, 360000, "bar" + i);
            allStrings.add("foo" + i);
        }

        Map<String,Object> results = _client.getBulk(allStrings.subList(0, 2000));
        Map<String,Object> results2 = _client.getBulk(allStrings.subList(2000, 4000));
        Map<String,Object> results3 = _client.getBulk(allStrings.subList(4000, 6000));
        Map<String,Object> results4 = _client.getBulk(allStrings.subList(6000, 8000));
        Map<String,Object> results5 = _client.getBulk(allStrings.subList(8000, 10000));
        for (int i = 0; i < 2000; i++) {
            Assert.assertEquals("bar" + i, results.get("foo" + i));
        }
        for (int i = 2000; i < 4000; i++) {
            Assert.assertEquals("bar" + i, results2.get("foo" + i));
        }
        for (int i = 4000; i < 6000; i++) {
            Assert.assertEquals("bar" + i, results3.get("foo" + i));
        }
        for (int i = 6000; i < 8000; i++) {
            Assert.assertEquals("bar" + i, results4.get("foo" + i));
        }
        for (int i = 8000; i < 10000; i++) {
            Assert.assertEquals("bar" + i, results5.get("foo" + i));
        }
    }

    private MemCacheDaemon createDaemon( final InetSocketAddress address ) throws IOException {
        final MemCacheDaemon daemon = new MemCacheDaemon();
        final LRUCacheStorageDelegate cacheStorage = new LRUCacheStorageDelegate(40000, 1024*1024*1024, 1024000);
        daemon.setCache(new Cache(cacheStorage));
        daemon.setAddr( address );
        daemon.setVerbose(false);
        return daemon;
    }

}
