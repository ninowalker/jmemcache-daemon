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

import com.thimbleware.jmemcached.storage.CacheStorage;

import java.io.IOException;
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
public final class CacheImpl extends AbstractCache<LocalCacheElement> implements Cache<LocalCacheElement> {

    final CacheStorage<String, LocalCacheElement> storage;
    final DelayQueue<DelayedMCElement> deleteQueue;
    final ReadWriteLock deleteQueueReadWriteLock;

    /**
     * Construct the server session handler
     * @param storage
     */
    public CacheImpl(CacheStorage<String, LocalCacheElement> storage) {
        super();
        this.storage = storage;
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
            LocalCacheElement placeHolder = new LocalCacheElement(key, 0, 0, 0);
            placeHolder.setData(new byte[]{});
            placeHolder.setBlocked(true);
            placeHolder.setBlockedUntil(Now() + (long)time);

            storage.replace(key, placeHolder);

            // this must go on a queue for processing later...
            try {
                deleteQueueReadWriteLock.writeLock().lock();
                deleteQueue.add(new DelayedMCElement(placeHolder));
            } finally {
                deleteQueueReadWriteLock.writeLock().unlock();
            }
        } else
            removed = storage.remove(key) != null;

        if (removed) return DeleteResponse.DELETED;
        else return DeleteResponse.NOT_FOUND;

    }

    /**
     * Add an element to the cache
     *
     * @param e the element to add
     * @return the store response code
     */
    public StoreResponse add(LocalCacheElement e) {
        return storage.putIfAbsent(e.getKeystring(), e) == null ? StoreResponse.STORED : StoreResponse.NOT_STORED;
    }

    /**
     * Replace an element in the cache
     *
     * @param e the element to replace
     * @return the store response code
     */
    public StoreResponse replace(LocalCacheElement e) {
        return storage.replace(e.getKeystring(), e) != null ? StoreResponse.STORED : StoreResponse.NOT_STORED;
    }

    /**
     * Append bytes to the end of an element in the cache
     *
     * @param element the element to append
     * @return the store response code
     */
    public StoreResponse append(LocalCacheElement element) {
        LocalCacheElement old = storage.get(element.getKeystring());
        if (old == null || isBlocked(old) || isExpired(old)) {
            getMisses.incrementAndGet();
            return StoreResponse.NOT_FOUND;
        }
        else {
            LocalCacheElement replace = new LocalCacheElement(old.getKeystring(), old.getFlags(), old.getExpire(), old.getDataLength() + element.getDataLength());
            ByteBuffer b = ByteBuffer.allocate(replace.getDataLength());
            b.put(old.getData());
            b.put(element.getData());
            replace.setData(new byte[replace.getDataLength()]);
            b.flip();
            b.get(replace.getData());
            replace.setCasUnique(replace.getCasUnique() + 1);
            return storage.replace(old.getKeystring(), old, replace) ? StoreResponse.STORED : StoreResponse.NOT_STORED;
        }
    }

    /**
     * Prepend bytes to the end of an element in the cache
     *
     * @param element the element to append
     * @return the store response code
     */
    public StoreResponse prepend(LocalCacheElement element) {
        LocalCacheElement old = storage.get(element.getKeystring());
        if (old == null || isBlocked(old) || isExpired(old)) {
            getMisses.incrementAndGet();
            return StoreResponse.NOT_FOUND;
        }
        else {
            LocalCacheElement replace = new LocalCacheElement(old.getKeystring(), old.getFlags(), old.getExpire(), old.getDataLength() + element.getDataLength());
            ByteBuffer b = ByteBuffer.allocate(replace.getDataLength());
            b.put(element.getData());
            b.put(old.getData());
            replace.setData(new byte[replace.getDataLength()]);
            b.flip();
            b.get(replace.getData());
            replace.setCasUnique(replace.getCasUnique() + 1);
            return storage.replace(old.getKeystring(), old, replace) ? StoreResponse.STORED : StoreResponse.NOT_STORED;
        }
    }

    /**
     * Set an element in the cache
     *
     * @param e the element to set
     * @return the store response code
     */
    public StoreResponse set(LocalCacheElement e) {
        setCmds.incrementAndGet();//update stats

        e.setCasUnique(casCounter.getAndIncrement());

        storage.put(e.getKeystring(), e);

        return StoreResponse.STORED;
    }

    /**
     * Set an element in the cache but only if the element has not been touched
     * since the last 'gets'
     * @param cas_key the cas key returned by the last gets
     * @param e the element to set
     * @return the store response code
     */
    public StoreResponse cas(Long cas_key, LocalCacheElement e) {
        // have to get the element
        LocalCacheElement element = storage.get(e.getKeystring());
        if (element == null || isBlocked(element)) {
            getMisses.incrementAndGet();
            return StoreResponse.NOT_FOUND;
        }

        if (element.getCasUnique().equals(cas_key)) {
            // casUnique matches, now set the element
            if (storage.replace(e.getKeystring(), element, e)) return StoreResponse.STORED;
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
        LocalCacheElement old = storage.get(key);
        if (old == null || isBlocked(old) || isExpired(old)) {
            getMisses.incrementAndGet();
            return null;
        } else {
            // TODO handle parse failure!
            int old_val = parseInt(new String(old.getData())) + mod; // change value
            if (old_val < 0) {
                old_val = 0;

            } // check for underflow

            byte[] newData = valueOf(old_val).getBytes();
            int newDataLength = newData.length;

            LocalCacheElement replace = new LocalCacheElement(old.getKeystring(), old.getFlags(), old.getExpire(), newDataLength);
            replace.setData(newData);
            replace.setCasUnique(replace.getCasUnique() + 1);
            return storage.replace(old.getKeystring(), old, replace) ? old_val : null;
        }
    }


    protected boolean isBlocked(CacheElement e) {
        return e.isBlocked() && e.getBlockedUntil() > Now();
    }

    protected boolean isExpired(CacheElement e) {
        return e.getExpire() != 0 && e.getExpire() < Now();
    }

    /**
     * Get an element from the cache
     * @param keys the key for the element to lookup
     * @return the element, or 'null' in case of cache miss.
     */
    public LocalCacheElement[] get(String ... keys) {
        getCmds.incrementAndGet();//updates stats

        LocalCacheElement[] elements = new LocalCacheElement[keys.length];
        int x = 0;
        int hits = 0;
        int misses = 0;
        for (String key : keys) {
            LocalCacheElement e = storage.get(key);
            if (e == null || isExpired(e) || e.isBlocked()) {
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
        storage.clear();
        return true;
    }

    public void close() throws IOException {
        storage.close();
    }

    @Override
    public Set<String> keys() {
        return storage.keySet();
    }

    @Override
    public long getCurrentItems() {
        return storage.size();
    }

    @Override
    public long getLimitMaxBytes() {
        return storage.getMemoryCapacity();
    }

    @Override
    public long getCurrentBytes() {
        return storage.getMemoryUsed();
    }

    /**
     * Executed periodically to clean from the cache those entries that are just blocking
     * the insertion of new ones.
     */
    @Override
    public void processDeleteQueue() {
        try {
            deleteQueueReadWriteLock.writeLock().lock();
            DelayedMCElement toDelete = deleteQueue.poll();
            if (toDelete != null) {
                storage.remove(toDelete.element.getKeystring());
            }

        } finally {
            deleteQueueReadWriteLock.writeLock().unlock();
        }
    }


    /**
     * Delayed key blocks get processed occasionally.
     */
    protected static class DelayedMCElement implements Delayed {
        private CacheElement element;

        public DelayedMCElement(CacheElement element) {
            this.element = element;
        }

        public long getDelay(TimeUnit timeUnit) {
            return timeUnit.convert(element.getBlockedUntil() - Now(), TimeUnit.MILLISECONDS);
        }

        public int compareTo(Delayed delayed) {
            if (!(delayed instanceof CacheImpl.DelayedMCElement))
                return -1;
            else
                return element.getKeystring().compareTo(((DelayedMCElement)delayed).element.getKeystring());
        }
    }
}
