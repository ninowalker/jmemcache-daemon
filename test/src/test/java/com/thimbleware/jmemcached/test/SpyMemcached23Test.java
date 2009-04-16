package com.thimbleware.jmemcached.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;

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
    public void testConnectDaemon() throws IOException, InterruptedException {
        _client.set( "foo", 5000, "bar" );
        Assert.assertEquals( "bar", _client.get( "foo" ) );
    }

    private MemCacheDaemon createDaemon( final InetSocketAddress address ) throws IOException {
        final MemCacheDaemon daemon = new MemCacheDaemon();
        final LRUCacheStorageDelegate cacheStorage = new LRUCacheStorageDelegate(1000, 1024*1024, 1024000);
        daemon.setCache(new Cache(cacheStorage));
        daemon.setAddr( address );
        daemon.setVerbose(true);
        return daemon;
    }

}
