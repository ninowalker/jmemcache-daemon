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

import static java.lang.Integer.parseInt;
import static java.lang.String.valueOf;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 */
public final class Cache {

    /**
     * The following state variables are universal for the entire daemon. These are used for statistics gathering.
     * In order for these values to work properly, the handler _must_ be declared with a ChannelPipelineCoverage
     * of "all".
     */
    public final AtomicInteger curr_conns = new AtomicInteger();
    public final AtomicInteger total_conns = new AtomicInteger();
    public final AtomicInteger started = new AtomicInteger();          /* when the process was started */
    public final AtomicLong bytes_read = new AtomicLong();
    public final AtomicLong bytes_written = new AtomicLong();
    public final AtomicLong curr_bytes = new AtomicLong();

    private AtomicInteger currentItems = new AtomicInteger();
    private AtomicInteger totalItems = new AtomicInteger();
    private AtomicInteger getCmds = new AtomicInteger();
    private AtomicInteger setCmds = new AtomicInteger();
    private AtomicInteger getHits = new AtomicInteger();
    private AtomicInteger getMisses = new AtomicInteger();

    private AtomicLong casCounter = new AtomicLong(1);

    protected CacheStorage cacheStorage;

    private DelayQueue<DelayedMCElement> deleteQueue;

    private final ReadWriteLock deleteQueueReadWriteLock;

/**
 * Initialize base values for status.
 */
    {
        curr_bytes.set(0);
        curr_conns.set(0);
        total_conns.set(0);
        bytes_read.set(0);
        bytes_written.set(0);
        started.set(Now());
    }


    public enum StoreResponse {
        STORED, NOT_STORED, EXISTS, NOT_FOUND
    }

    public enum DeleteResponse {
        DELETED, NOT_FOUND
    }

    /**
     * Delayed key blocks get processed occasionally.
     */
    private class DelayedMCElement implements Delayed {
        private MCElement element;

        public DelayedMCElement(MCElement element) {
            this.element = element;
        }

        public long getDelay(TimeUnit timeUnit) {
            return timeUnit.convert(element.blocked_until - Now(), TimeUnit.MILLISECONDS);
        }

        public int compareTo(Delayed delayed) {
            if (!(delayed instanceof DelayedMCElement))
                return -1;
            else
                return element.keystring.compareTo(((DelayedMCElement)delayed).element.keystring);
        }
    }

    /**
     * Construct the server session handler
     *
     * @param cacheStorage the cache to use
     */
    public Cache(CacheStorage cacheStorage) {
        initStats();
        this.cacheStorage = cacheStorage;
        this.deleteQueue = new DelayQueue<DelayedMCElement>();

        deleteQueueReadWriteLock = new ReentrantReadWriteLock();
    }

    /**
     * Handle the deletion of an item from the cache.
     *
     * @param key the key for the item
     * @param time an amount of time to block this entry in the cache for further writes
     * @return the message response
     */
    public DeleteResponse delete(String key, int time) {
        try {
            cacheStorage.startCacheWrite(this);

            if (isThere(key)) {
                if (time != 0) {
                    // mark it as blocked
                    MCElement el = this.cacheStorage.get(key);
                    el.blocked = true;
                    el.blocked_until = Now() + time;

                    // actually clear the data since we don't need to keep it
                    el.dataLength = 0;
                    el.data = new byte[0];
                    this.cacheStorage.put(key, el, el.dataLength);

                    // this must go on a queue for processing later...
                    try {
                        deleteQueueReadWriteLock.writeLock().lock();
                        deleteQueue.add(new DelayedMCElement(el));
                    } finally {
                        deleteQueueReadWriteLock.writeLock().unlock();
                    }
                } else {
                    this.cacheStorage.remove(key); // just remove it
                }
                return DeleteResponse.DELETED;
            } else {
                return DeleteResponse.NOT_FOUND;
            }
        } finally {
            cacheStorage.finishCacheWrite(this);
        }
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
                try {
                    cacheStorage.startCacheWrite(this);
                    if (this.cacheStorage.get(toDelete.element.keystring) != null) {
                        this.cacheStorage.remove(toDelete.element.keystring);
                    }
                } finally {
                    cacheStorage.finishCacheWrite(this);
                }
            }

        } finally {
            deleteQueueReadWriteLock.writeLock().unlock();
        }
    }

    /**
     * Add an element to the cache
     *
     * @param e the element to add
     * @return the store response code
     */
    public StoreResponse add(MCElement e) {
        try {
            cacheStorage.startCacheWrite(this);
            if (!isThere(e.keystring)) return set(e);
            else return StoreResponse.NOT_STORED;
        } finally {
            cacheStorage.finishCacheWrite(this);
        }
    }

    /**
     * Replace an element in the cache
     *
     * @param e the element to replace
     * @return the store response code
     */
    public StoreResponse replace(MCElement e) {
        try {
            cacheStorage.startCacheWrite(this);
            if (isThere(e.keystring)) return set(e);
            else return StoreResponse.NOT_STORED;
        } finally {
            cacheStorage.finishCacheWrite(this);
        }
    }

    /**
     * Append bytes to the end of an element in the cache
     *
     * @param element the element to append
     * @return the store response code
     */
    public StoreResponse append(MCElement element) {
        try {
            cacheStorage.startCacheWrite(this);
            MCElement ret = get(element.keystring)[0];
            if (ret == null || isBlocked(ret) || isExpired(ret))
                return StoreResponse.NOT_FOUND;
            else {
                ret.dataLength += element.dataLength;
                ByteBuffer b = ByteBuffer.allocate(ret.dataLength);
                b.put(ret.data);
                b.put(element.data);
                ret.data = new byte[ret.dataLength];
                b.flip();
                b.get(ret.data);
                ret.cas_unique++;
                this.cacheStorage.put(ret.keystring, ret, ret.dataLength);

                return StoreResponse.STORED;
            }
        } finally {
            cacheStorage.finishCacheWrite(this);
        }
    }

    /**
     * Prepend bytes to the end of an element in the cache
     *
     * @param element the element to append
     * @return the store response code
     */
    public StoreResponse prepend(MCElement element) {
        try {
            cacheStorage.startCacheWrite(this);
            MCElement ret = get(element.keystring)[0];
            if (ret == null || isBlocked(ret) || isExpired(ret))
                return StoreResponse.NOT_FOUND;
            else {
                ret.dataLength += element.dataLength;
                ByteBuffer b = ByteBuffer.allocate(ret.dataLength);
                b.put(element.data);
                b.put(ret.data);
                ret.data = new byte[ret.dataLength];
                b.flip();
                b.get(ret.data);
                ret.cas_unique++;
                this.cacheStorage.put(ret.keystring, ret, ret.dataLength);

                return StoreResponse.STORED;
            }
        } finally {
            cacheStorage.finishCacheWrite(this);
        }
    }
    /**
     * Set an element in the cache
     *
     * @param e the element to set
     * @return the store response code
     */
    public StoreResponse set(MCElement e) {
        try {
            cacheStorage.startCacheWrite(this);
            setCmds.incrementAndGet();//update stats

            // increment the CAS counter; put in the new CAS
            e.cas_unique = casCounter.getAndIncrement();

            this.cacheStorage.put(e.keystring, e, e.dataLength);

            return StoreResponse.STORED;
        } finally {
            cacheStorage.finishCacheWrite(this);
        }
    }

    /**
     * Set an element in the cache but only if the element has not been touched
     * since the last 'gets'
     * @param cas_key the cas key returned by the last gets
     * @param e the element to set
     * @return the store response code
     */
    public StoreResponse cas(Long cas_key, MCElement e) {
        try {
            cacheStorage.startCacheWrite(this);
            // have to get the element
            MCElement element = get(e.keystring)[0];
            if (element == null || isBlocked(element))
                return StoreResponse.NOT_FOUND;

            if (element.cas_unique == cas_key) {
                // cas_unique matches, now set the element
                return set(e);
            } else {
                // cas didn't match; someone else beat us to it
                return StoreResponse.EXISTS;
            }

        } finally {
            cacheStorage.finishCacheWrite(this);
        }
    }

    /**
     * Increment/decremen t an (integer) element in the cache
     * @param key the key to increment
     * @param mod the amount to add to the value
     * @return the message response
     */
    public Integer get_add(String key, int mod) {
        try {
            cacheStorage.startCacheWrite(this);
            MCElement e = this.cacheStorage.get(key);
            if (e == null) {
                getMisses.incrementAndGet();//update stats
                return null;
            }
            if (isExpired(e) || e.blocked) {
                //logger.info("FOUND BUT EXPIRED");
                getMisses.incrementAndGet();//update stats
                return null;
            }
            // TODO handle parse failure!
            int old_val = parseInt(new String(e.data)) + mod; // change value
            if (old_val < 0) {
                old_val = 0;
            } // check for underflow
            e.data = valueOf(old_val).getBytes(); // toString
            e.dataLength = e.data.length;

            // assign new cas id
            e.cas_unique = casCounter.getAndIncrement();

            this.cacheStorage.put(e.keystring, e, e.dataLength); // save new value
            return old_val;
        } finally {
            cacheStorage.finishCacheWrite(this);
        }
    }


    /**
     * Check whether an element is in the cache and non-expired and the slot is non-blocked
     * @param key the key for the element to lookup
     * @return whether the element is in the cache and is live
     */
    public boolean isThere(String key) {
        try {
            cacheStorage.startCacheRead(this);
            MCElement e = this.cacheStorage.get(key);
            return e != null && !isExpired(e) && !isBlocked(e);
        } finally {
            cacheStorage.finishCacheRead(this);
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
            cacheStorage.startCacheRead(this);

            try {
                MCElement e = this.cacheStorage.get(key);

                if (e == null || isExpired(e) || e.blocked) {
                    misses++;

                    elements[x] = null;
                } else {
                    hits++;

                    elements[x] = e;
                }
                x++;
            } finally {
                cacheStorage.finishCacheRead(this);
            }

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
        try {
            cacheStorage.startCacheWrite(this);
            this.cacheStorage.clear();
        } finally {
            cacheStorage.finishCacheWrite(this);
        }
        return true;
    }

    /**
     * @return the current time in seconds (from epoch), used for expiries, etc.
     */
    protected final int Now() {
        return (int) (System.currentTimeMillis() / 1000);
    }

    /**
     * Initialize all statistic counters
     */
    protected void initStats() {
        currentItems.set(0);
        totalItems.set(0);
        getCmds.set(0);
        setCmds.set(0);
        getHits.set(0);
        getMisses.set(0);
    }

    public void close() {
        cacheStorage.close();
    }

    public Set<String> keys() {
        try { cacheStorage.startCacheRead(this);
            return cacheStorage.keys();
        } finally {
            cacheStorage.finishCacheRead(this);
        }
    }

    public long getCurrentItems() {
        try {
            cacheStorage.startCacheRead(this);
            return  this.cacheStorage.getCurrentItemCount();
        } finally {
            cacheStorage.finishCacheRead(this);
        }
    }

    public long getLimitMaxBytes() {
        try {
            cacheStorage.startCacheRead(this);
            return this.cacheStorage.getMaximumSizeBytes();
        } finally {
            cacheStorage.finishCacheRead(this);
        }
    }

    public long getCurrentBytes() {
        try {
            cacheStorage.startCacheRead(this);
            return this.cacheStorage.getCurrentSizeBytes();
        } finally {
            cacheStorage.finishCacheRead(this);
        }
    }


    public int getTotalItems() {
        return totalItems.get();
    }

    public int getGetCmds() {
        return getCmds.get();
    }

    public int getSetCmds() {
        return setCmds.get();
    }

    public int getGetHits() {
        return getHits.get();
    }

    public int getGetMisses() {
        return getMisses.get();
    }

    /**
     * Return runtime statistics
     *
     * @param arg additional arguments to the stats command
     * @return the full command response
     */
    public Map<String, Set<String>> stat(String arg) {
        Map<String, Set<String>> result = new HashMap<String, Set<String>>();

        if ("keys".equals(arg)) {
            for (String key : this.keys()) {
                multiSet(result, "key", key);
            }

            return result;
        }

        // stats we know
        multiSet(result, "version", MemCacheDaemon.memcachedVersion);
        multiSet(result, "cmd_gets", valueOf(getGetCmds()));
        multiSet(result, "cmd_sets", valueOf(getSetCmds()));
        multiSet(result, "get_hits", valueOf(getGetHits()));
        multiSet(result, "get_misses", valueOf(getGetMisses()));
        multiSet(result, "curr_connections", valueOf(curr_conns));
        multiSet(result, "total_connections", valueOf(total_conns));
        multiSet(result, "time", valueOf(valueOf(Now())));
        multiSet(result, "uptime", valueOf(Now() - this.started.intValue()));
        multiSet(result, "cur_items", valueOf(this.getCurrentItems()));
        multiSet(result, "limit_maxbytes", valueOf(this.getLimitMaxBytes()));
        multiSet(result, "current_bytes", valueOf(this.getCurrentBytes()));
        multiSet(result, "free_bytes", valueOf(Runtime.getRuntime().freeMemory()));

        // Not really the same thing precisely, but meaningful nonetheless. potentially this should be renamed
        multiSet(result, "pid", valueOf(Thread.currentThread().getId()));

        // stuff we know nothing about; gets faked only because some clients expect this
        multiSet(result, "rusage_user", "0:0");
        multiSet(result, "rusage_system", "0:0");
        multiSet(result, "connection_structures", "0");

        // TODO we could collect these stats
        multiSet(result, "bytes_read", "0");
        multiSet(result, "bytes_written", "0");

        return result;
    }

    private void multiSet(Map<String, Set<String>> map, String key, String val) {
        Set<String> cur = map.get(key);
        if (cur == null) {
            cur = new HashSet<String>();
        }
        cur.add(val);
        map.put(key, cur);
    }
}
