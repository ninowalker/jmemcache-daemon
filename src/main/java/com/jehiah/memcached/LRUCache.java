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
package com.jehiah.memcached;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Simple, thread-safe, generic cache to hold Objects in a table, which uses
 * read-write locks for maximum liveness.
 * <ul>
 * <li> the cache is thread-safe
 * <li> the cache has read-write locks, which exploit the fact that
 * most access are reads
 * <li> cache items are expelled when the total maximum size of the cache is
 * reached
 * <li> unusual: not built to handle the case of user input affecting the
 * data; only handles case of changes to the datastore eventually refreshing
 * the cache. Thus, this cache is not suitable for the case in which user
 * input must be reflected.
 * </ul>
 * <p/>
 * <p/>
 * Source: http://www.javapractices.com/Topic118.cjp
 */
public final class LRUCache<ID_TYPE, ITEM_TYPE> {

    private final Map<ID_TYPE, CacheEntry<ITEM_TYPE>> fItems;

    /**
     * Read-write lock allows maximal concurrency, since readers can share access;
     * only writers need sole access.
     */
    private final ReadWriteLock readWriteLock;

    private long size = 0; // current size in bytes

    private final long maximumSize; // in bytes
    private long ceilingSize;
    private int maximumItems;
    private static final int INITIAL_TABLE_SIZE = 2048;

    final class CacheEntry<ITEM_TYPE> {
        long size;
        ITEM_TYPE item;

        CacheEntry(long size, ITEM_TYPE item) {
            this.size = size;
            this.item = item;
        }
    }

    /**
     * Caches are created as empty, and populated through use.
     *
     * @param maximumItems maximum number of items allowed in the cache
     * @param maximumSize maximum size in bytes of the cache
     * @param ceilingSize number of bytes to attempt to leave as ceiling room
     */
    public LRUCache(final int maximumItems, final long maximumSize, final long ceilingSize) {
        this.maximumItems = maximumItems;
        this.maximumSize = maximumSize;
        this.ceilingSize = ceilingSize;
        fItems = new LinkedHashMap<ID_TYPE, CacheEntry<ITEM_TYPE>>(INITIAL_TABLE_SIZE) {
            protected boolean removeEldestEntry(Map.Entry<ID_TYPE, CacheEntry<ITEM_TYPE>> eldest) {
                if (size + ceilingSize > maximumSize || size() > maximumItems) {
                    size -= eldest.getValue().size;
                    return true;
                } else return false;
            }
        };
        readWriteLock = new ReentrantReadWriteLock();

    }

    /**
     * Return true only if the corresponding item is in the cache, and has been
     * in it for no more that fRefreshInterval milliseconds; if caching is
     * disabled, then always return false.
     *
     * @param aId is non-null.
     * @return if the corresponding item is in the cache
     * @throws IllegalArgumentException if a param does not comply.
     */
    public boolean has(ID_TYPE aId) {
        if (aId == null) throw new IllegalArgumentException("Id must not be null.");

        startRead();
        try {
            return fItems.containsKey(aId);
        }
        finally {
            finishRead();
        }
    }

    /**
     * Retrieve an existing item from the cache.
     *
     * @param aId is non-null, and corresponds to an existing item in the cache.
     * @return a non-null Object
     * @throws IllegalArgumentException if aId is null, or if the item is not in the cache
     * @throws IllegalStateException    if the item in the cache is null or the cache is disabled
     */
    public ITEM_TYPE get(ID_TYPE aId) {
        if (aId == null) throw new IllegalArgumentException("Id must not be null.");

        startRead();
        ITEM_TYPE result;
        try {
            if (fItems.containsKey(aId)) {
                result = fItems.get(aId).item;
                if (result == null) {
                    throw new IllegalStateException("Stored item should not be null. Id:" + aId);
                }
            } else {
                return null;
            }
            return result;
        }
        finally {
            finishRead();
        }
    }

    /**
     * If the item is already present, then replace it; otherwise, add it.
     * <p/>
     * If the cache is disabled, do nothing.
     *
     * @param aId   is non-null
     * @param aItem is non-null
     * @param item_size is the size of aItem in bytes
     * @throws IllegalArgumentException if param does not comply
     */
    public void put(ID_TYPE aId, ITEM_TYPE aItem, long item_size) {
        if (aId == null) throw new IllegalArgumentException("Id must not be null.");
        if (aItem == null) throw new IllegalArgumentException("Item must not be null.");

        startWrite();
        try {
            fItems.put(aId, new CacheEntry<ITEM_TYPE>(item_size, aItem));
            size += item_size;
        }
        finally {
            finishWrite();
        }
    }

    /**
     * Start from beginning, and remove all items from the cache; if cache is
     * disabled, do nothing.
     * <p/>
     * Forces a re-population of all items into the cache.
     */
    public void clear() {
        startWrite();
        try {
            fItems.clear();
            size = 0;
        }
        finally {
            finishWrite();
        }
    }

    public Set<ID_TYPE> keys() {
        startRead();
        try {
            return fItems.keySet();
        } finally {
            finishRead();
        }
    }

    public void remove(ID_TYPE keystring) {
        startWrite();
        try {
            CacheEntry<ITEM_TYPE> item = fItems.get(keystring);
            if (item != null) {
                fItems.remove(keystring);
                size -= item.size;
            }
        } finally {
            finishWrite();
        }
    }



    /**
     * Blocks of code in which the contents of fItems and fTimePlacedIntoCache
     * are examined in any way must be surrounded by calls to <code>startRead</code>
     * and <code>finishRead</code>. See documentation for ReadWriteLock.
     * <p/>
     * Translates the InterruptedException into a generic DataAccessException, to
     * protect the higher layers from implementation details.
     */
    private void startRead() {
        readWriteLock.readLock().lock();

    }

    private void finishRead() {
        readWriteLock.readLock().unlock();
    }


    /**
     * Blocks of code in which the contents of fItems and fTimePlacedIntoCache
     * are changed in any way must be surrounded by calls to <code>startWrite</code> and
     * <code>finishWrite</code>. See documentation for ReadWriteLock.
     * <p/>
     * Translates the InterruptedException into a generic DataAccessException, to
     * protect the higher layers from implementation details.
     */
    private void startWrite() {
        readWriteLock.writeLock().lock();

    }

    private void finishWrite() {
        readWriteLock.writeLock().unlock();
    }

    //
    public long count() {
        startRead();
        try {
            return fItems.size();
        } finally {
            finishRead();
        }
    }

    public long getSize() {
        return size;
    }

    public long getMaximumSize() {
        return maximumSize;
    }

    public long getCeilingSize() {
        return ceilingSize;
    }

    public int getMaximumItems() {
        return maximumItems;
    }

}