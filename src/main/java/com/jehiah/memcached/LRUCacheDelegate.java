/**
 *
 * Java Memcached Server
 *
 * http://jehiah.com/projects/j-memcached
 *
 * Distributed under GPL
 * @author Jehiah Czebotar
 */
package com.jehiah.memcached;

import java.util.Iterator;

/**
 * A delegate around the internal thread-safe LRUCache implementation.
 */
public final class LRUCacheDelegate implements Cache {

    private LRUCache<String, MCElement> cache;

    public LRUCacheDelegate(int maxSize, long maxBytes, long ceilingSize) {
        //Create a Cache specifying its configuration.
        cache = new LRUCache<String, MCElement>(maxSize, maxBytes, ceilingSize);
    }

    public MCElement get(String keystring) {
        return cache.get(keystring);
    }

    public void put(String keystring, MCElement el) {
        cache.put(keystring, el, el.data_length);
    }

    public void remove(String keystring) {
        cache.remove(keystring);
    }

    public Iterator<String> keys() {
        return cache.keys().iterator();
    }

    public long size() {
        return cache.getSize();
    }

    public void flushAll() {
        cache.clear();
    }

    public long count() {
        return cache.count();
    }

    public long maxSize() {
        return cache.getMaximumSize();
    }
}
