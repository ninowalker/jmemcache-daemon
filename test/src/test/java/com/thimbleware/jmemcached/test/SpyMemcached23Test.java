package com.thimbleware.jmemcached.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;

import net.spy.memcached.*;
import net.spy.memcached.transcoders.IntegerTranscoder;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
        _client.shutdown();
        _daemon.stop();
    }

    @Test
    public void testBinaryCompressed(){
        _client.add("foo", 86400, "foobarshoe");
        assertEquals("wrong value returned from cache", "foobarshoe",  _client.get("foo"));
        StringBuilder sb = new StringBuilder();
        sb.append("hello world");
        for(int i=0; i<20; i++){
            sb.append(sb);
        }
        System.out.println("length is: " + sb.length());
        _client.add("sb", 86400, sb.toString());
        assertNotNull("null get when sb.length()="+sb.length(), _client.get("sb"));
        assertEquals("wrong length for sb",sb.length(), _client.get("sb").toString().length());
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
    public void testIncrDecr() {
        _client.set( "foo", 0, "1");
        Assert.assertEquals( "1", _client.get( "foo" ) );
        _client.incr( "foo", 5 );
        Assert.assertEquals( "6", _client.get( "foo" ) );
        _client.decr( "foo", 10 );
        Assert.assertEquals( "0", _client.get( "foo" ) );
    }

    @Test
    public void testCAS() throws Exception {
        _client.set("foo", 32000, 123);
        CASValue<Object> casValue = _client.gets("foo");
        Assert.assertEquals( 123, casValue.getValue());

        CASResponse cr = _client.cas("foo", casValue.getCas(), 456);

        Assert.assertEquals(CASResponse.OK, cr);

        Future<Object> rf = _client.asyncGet("foo");

        Assert.assertEquals(456, rf.get());
    }

    @Test
    public void testAppendPrepend() throws Exception {
        _client.set( "foo", 0, "foo" );
        _client.append(0, "foo", "bar");
        Assert.assertEquals( "foobar", _client.get( "foo" ));
        _client.prepend(0, "foo", "baz");
        Assert.assertEquals( "bazfoobar", _client.get( "foo" ));
    }

    @Test
    public void testBulkGet() throws IOException, InterruptedException, ExecutionException {
        ArrayList<String> allStrings = new ArrayList<String>();
        for (int i = 0; i < 10000; i++) {
            _client.set("foo" + i, 360000, "bar" + i);
            allStrings.add("foo" + i);
        }
        // doing a regular get, we are just too slow for spymemcached's tolerances... for now
        Future<Map<String, Object>> future = _client.asyncGetBulk(allStrings);
        Map<String, Object> results = future.get();

        for (int i = 0; i < 10000; i++) {
            Assert.assertEquals("bar" + i, results.get("foo" + i));
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
