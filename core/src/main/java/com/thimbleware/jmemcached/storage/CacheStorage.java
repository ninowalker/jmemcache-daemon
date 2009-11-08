package com.thimbleware.jmemcached.storage;

import com.thimbleware.jmemcached.storage.hash.SizedItem;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

/**
 * The interface for cache storage. Essentially a concurrent map but with methods for investigating the heap
 * state of the storage unit and with additional support for explicit resource-cleanup (close()).
 */
public interface CacheStorage<K, V extends SizedItem> extends ConcurrentMap<K, V> {
    /**
     * @return the capacity (in bytes) of the storage
     */
    long getMemoryCapacity();

    /**
     * @return the current usage (in bytes) of the storage
     */
    long getMemoryUsed();

    /**
     * @return the capacity (in # of items) of the storage
     */
    int capacity();

    /**
     * Close the storage unit, deallocating any resources it might be currently holding.
     * @throws java.io.IOException thrown if IO faults occur anywhere during close.
     */
    void close() throws IOException;
}
