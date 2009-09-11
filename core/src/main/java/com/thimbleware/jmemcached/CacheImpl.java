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
package com.thimbleware.jmemcached;

import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;
import com.thimbleware.jmemcached.storage.ConcurrentSizedMap;

import static java.lang.Integer.parseInt;
import static java.lang.String.valueOf;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 */
public final class CacheImpl extends AbstractCache implements Cache {

    final ConcurrentSizedMap<String, MCElement> cache;
    final DelayQueue<DelayedMCElement> deleteQueue;
    final ReadWriteLock deleteQueueReadWriteLock;

    /**
     * Construct the server session handler
     */
    public CacheImpl(ConcurrentSizedMap<String, MCElement> storage) {
        super();
        this.cache = storage;
        deleteQueue = new DelayQueue<DelayedMCElement>();
        deleteQueueReadWriteLock = new ReentrantReadWriteLock();

        initStats();
    }

    /**
     * Handle the deletion of an item from the cache.
     *
     * @param key the key for the item
     * @param time an amount of time to block this entry in the cache for further writes
     * @return the message response
     */
    public DeleteResponse delete(String key, int time) {
        boolean removed = false;

        // delayed remove
        if (time != 0) {
            // block the element and schedule a delete; replace its entry with a blocked element
            MCElement placeHolder = new MCElement(key, 0, 0, 0);
            placeHolder.data = new byte[]{};
            placeHolder.blocked = true;
            placeHolder.blocked_until = Now() + time;

            cache.replace(key, placeHolder);

            // this must go on a queue for processing later...
            try {
                deleteQueueReadWriteLock.writeLock().lock();
                deleteQueue.add(new DelayedMCElement(placeHolder));
            } finally {
                deleteQueueReadWriteLock.writeLock().unlock();
            }
        } else
            removed = cache.remove(key) != null;

        if (removed) return DeleteResponse.DELETED;
        else return DeleteResponse.NOT_FOUND;

    }

    /**
     * Add an element to the cache
     *
     * @param e the element to add
     * @return the store response code
     */
    public StoreResponse add(MCElement e) {
        return cache.putIfAbsent(e.keystring, e) == null ? StoreResponse.STORED : StoreResponse.NOT_STORED;
    }

    /**
     * Replace an element in the cache
     *
     * @param e the element to replace
     * @return the store response code
     */
    public StoreResponse replace(MCElement e) {
        return cache.replace(e.keystring, e) != null ? StoreResponse.STORED : StoreResponse.NOT_STORED;
    }

    /**
     * Append bytes to the end of an element in the cache
     *
     * @param element the element to append
     * @return the store response code
     */
    public StoreResponse append(MCElement element) {
        MCElement old = cache.get(element.keystring);
        if (old == null || isBlocked(old) || isExpired(old)) {
            getMisses.incrementAndGet();
            return StoreResponse.NOT_FOUND;
        }
        else {
            MCElement replace = new MCElement(old.keystring, old.flags, old.expire, old.dataLength + element.dataLength);
            ByteBuffer b = ByteBuffer.allocate(replace.dataLength);
            b.put(old.data);
            b.put(element.data);
            replace.data = new byte[replace.dataLength];
            b.flip();
            b.get(replace.data);
            replace.cas_unique++;
            return cache.replace(old.keystring, old, replace) ? StoreResponse.STORED : StoreResponse.NOT_STORED;
        }
    }

    /**
     * Prepend bytes to the end of an element in the cache
     *
     * @param element the element to append
     * @return the store response code
     */
    public StoreResponse prepend(MCElement element) {
        MCElement old = cache.get(element.keystring);
        if (old == null || isBlocked(old) || isExpired(old)) {
            getMisses.incrementAndGet();
            return StoreResponse.NOT_FOUND;
        }
        else {
            MCElement replace = new MCElement(old.keystring, old.flags, old.expire, old.dataLength + element.dataLength);
            ByteBuffer b = ByteBuffer.allocate(replace.dataLength);
            b.put(element.data);
            b.put(old.data);
            replace.data = new byte[replace.dataLength];
            b.flip();
            b.get(replace.data);
            replace.cas_unique++;
            return cache.replace(old.keystring, old, replace) ? StoreResponse.STORED : StoreResponse.NOT_STORED;
        }
    }

    /**
     * Set an element in the cache
     *
     * @param e the element to set
     * @return the store response code
     */
    public StoreResponse set(MCElement e) {
        setCmds.incrementAndGet();//update stats

        e.cas_unique = casCounter.getAndIncrement();

        cache.put(e.keystring, e);

        return StoreResponse.STORED;
    }

    /**
     * Set an element in the cache but only if the element has not been touched
     * since the last 'gets'
     * @param cas_key the cas key returned by the last gets
     * @param e the element to set
     * @return the store response code
     */
    public StoreResponse cas(Long cas_key, MCElement e) {
        // have to get the element
        MCElement element = cache.get(e.keystring);
        if (element == null || isBlocked(element)) {
            getMisses.incrementAndGet();
            return StoreResponse.NOT_FOUND;
        }

        if (element.cas_unique == cas_key) {
            // cas_unique matches, now set the element
            if (cache.replace(e.keystring, element, e)) return StoreResponse.STORED;
            else {
                getMisses.incrementAndGet();
                return StoreResponse.NOT_FOUND;
            }
        } else {
            // cas didn't match; someone else beat us to it
            return StoreResponse.EXISTS;
        }
    }

    /**
     * Increment/decremen t an (integer) element in the cache
     * @param key the key to increment
     * @param mod the amount to add to the value
     * @return the message response
     */
    public Integer get_add(String key, int mod) {
        MCElement old = cache.get(key);
        if (old == null || isBlocked(old) || isExpired(old)) {
            getMisses.incrementAndGet();
            return null;
        } else {
            // TODO handle parse failure!
            int old_val = parseInt(new String(old.data)) + mod; // change value
            if (old_val < 0) {
                old_val = 0;

            } // check for underflow

            byte[] newData = valueOf(old_val).getBytes();
            int newDataLength = newData.length;

            MCElement replace = new MCElement(old.keystring, old.flags, old.expire, newDataLength);
            replace.data = newData;
            replace.cas_unique++;
            return cache.replace(old.keystring, old, replace) ? old_val : null;
        }
    }


    protected boolean isBlocked(MCElement e) {
        return e.blocked && e.blocked_until > Now();
    }

    protected boolean isExpired(MCElement e) {
        return e.expire != 0 && e.expire < Now();
    }

    /**
     * Get an element from the cache
     * @param keys the key for the element to lookup
     * @return the element, or 'null' in case of cache miss.
     */
    public MCElement[] get(String ... keys) {
        getCmds.incrementAndGet();//updates stats

        MCElement[] elements = new MCElement[keys.length];
        int x = 0;
        int hits = 0;
        int misses = 0;
        for (String key : keys) {
            MCElement e = cache.get(key);
            if (e == null || isExpired(e) || e.blocked) {
                misses++;

                elements[x] = null;
            } else {
                hits++;

                elements[x] = e;
            }
            x++;

        }
        getMisses.addAndGet(misses);
        getHits.addAndGet(hits);

        return elements;

    }

    /**
     * Flush all cache entries
     * @return command response
     */
    public boolean flush_all() {
        return flush_all(0);
    }

    /**
     * Flush all cache entries with a timestamp after a given expiration time
     * @param expire the flush time in seconds
     * @return command response
     */
    public boolean flush_all(int expire) {
        // TODO implement this, it isn't right... but how to handle efficiently? (don't want to linear scan entire cacheStorage)
        cache.clear();
        return true;
    }

    public void close() {
        cache.clear();
    }

    @Override
    public Set<String> keys() {
        return cache.keySet();
    }

    @Override
    public long getCurrentItems() {
        return cache.size();
    }

    @Override
    public long getLimitMaxBytes() {
        return cache.getMemoryCapacity();
    }

    @Override
    public long getCurrentBytes() {
        return cache.getMemoryUsed();
    }

    /**
     * Executed periodically to clean from the cache those entries that are just blocking
     * the insertion of new ones.
     */
    public void processDeleteQueue() {
        try {
            deleteQueueReadWriteLock.writeLock().lock();
            DelayedMCElement toDelete = deleteQueue.poll();
            if (toDelete != null) {
                cache.remove(toDelete.element.keystring);
            }

        } finally {
            deleteQueueReadWriteLock.writeLock().unlock();
        }
    }


    /**
     * Delayed key blocks get processed occasionally.
     */
    protected class DelayedMCElement implements Delayed {
        private MCElement element;

        public DelayedMCElement(MCElement element) {
            this.element = element;
        }

        public long getDelay(TimeUnit timeUnit) {
            return timeUnit.convert(element.blocked_until - Now(), TimeUnit.MILLISECONDS);
        }

        public int compareTo(Delayed delayed) {
            if (!(delayed instanceof CacheImpl.DelayedMCElement))
                return -1;
            else
                return element.keystring.compareTo(((CacheImpl.DelayedMCElement)delayed).element.keystring);
        }
    }
}
