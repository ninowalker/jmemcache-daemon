/**
 *  Copyright 2008 ThimbleWare Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.thimbleware.jmemcached.storage.hash;

import com.thimbleware.jmemcached.MCElement;
import com.thimbleware.jmemcached.Cache;
import com.thimbleware.jmemcached.storage.CacheStorage;

import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A delegate around the internal thread-safe LRUCache implementation.
 */
public final class LRUCacheStorageDelegate implements CacheStorage {

    private LRUCache<String, MCElement> cache;

    /**
     * Read-write lock allows maximal concurrency, since readers can share access;
     * only writers need sole access.
     */
    private final ReadWriteLock cacheReadWriteLock = new ReentrantReadWriteLock();
    
    public LRUCacheStorageDelegate(int maxSize, long maxBytes, long ceilingSize) {
        //Create a CacheStorage specifying its configuration.
        cache = new LRUCache<String, MCElement>(maxSize, maxBytes, ceilingSize);
    }

    public MCElement get(String keystring) {
        return cache.get(keystring);
    }

    public void put(String keystring, MCElement el, int data_length) {
        cache.put(keystring, el, el.dataLength);
    }

    public void remove(String keystring) {
        cache.remove(keystring);
    }

    public Set<String> keys() {
        return cache.keys();
    }

    public long getCurrentSizeBytes() {
        return cache.getSize();
    }

    public void clear() {
        cache.clear();
    }

    public void close() {
        cache.clear();
    }

    public long getCurrentItemCount() {
        return cache.count();
    }

    public int getMaximumItems() {
        return cache.getMaximumItems();
    }

    public long getMaximumSizeBytes() {
        return cache.getMaximumSizeBytes();
    }

    /**
     * Blocks of code in which the contents of the cache
     * are examined in any way must be surrounded by calls to <code>startRead</code>
     * and <code>finishRead</code>. See documentation for ReadWriteLock.
     * @param cache
     */
    public void finishCacheRead(Cache cache) {
        cacheReadWriteLock.readLock().unlock();
    }

    /**
     * Blocks of code in which the contents of the cache
     * are examined in any way must be surrounded by calls to <code>startRead</code>
     * and <code>finishRead</code>. See documentation for ReadWriteLock.
     * @param cache
     */
    public void startCacheRead(Cache cache) {
        cacheReadWriteLock.readLock().lock();

    }

    /**
     * Blocks of code in which the contents of the cache
     * are changed in any way must be surrounded by calls to <code>startWrite</code> and
     * <code>finishWrite</code>. See documentation for ReadWriteLock.
     * protect the higher layers from implementation details.
     * @param cache
     */
    public void startCacheWrite(Cache cache) {
        cacheReadWriteLock.writeLock().lock();

    }

    /**
     * Blocks of code in which the contents of the cache
     * are changed in any way must be surrounded by calls to <code>startWrite</code> and
     * <code>finishWrite</code>. See documentation for ReadWriteLock.
     * @param cache
     */
    public void finishCacheWrite(Cache cache) {
        cacheReadWriteLock.writeLock().unlock();
    }
}
