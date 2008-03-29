package com.jehiah.memcached;

import java.util.Iterator;

/**
 * Interface for a cache usable by the daemon.
 *
 * All read and write operations _must_ be thread safe at the cache level since it is almost
 * guaranteed that concurrent writes will be made to the cache, and the caller does not
 * manage locking on the cache.
 */
public interface Cache {
    /**
     * Retrieve an element from the cache.  The retriever is responsible for counting hits,
     * managing expirations, etc.
     *
     * @param keystring the key identifying the entry
     * @return a cache element
     */
    MCElement get(String keystring);

    /**
     * Put an entry into the cache or replace an existing entry.
     *
     * @param keystring the key identifying the entry
     * @param el the element to place in the cache
     */
    void put(String keystring, MCElement el);

    /**
     * Remove an entry from the cache
     *
     * @param keystring the key to lookup
     */
    void remove(String keystring);

    /**
     * @return the list of keys currently managed in the cache
     */
    Iterator<String> keys();

    /**
     * Flush all entries from the cache
     */
    void flushAll();

    /**
     * @return the total count (in bytes) of all the elements in the cache
     */
    long size();

    /**
     * @return the maximum capacity (in bytes) of the cache
     */
    long maxSize();

    /**
     * @return how many entries are in the cache
     */
    long count();

}
