package com.thimbleware.jmemcached.storage;

import com.thimbleware.jmemcached.storage.hash.SizedItem;

import java.util.concurrent.ConcurrentMap;

/**
 */
public interface ConcurrentSizedMap<K, V extends SizedItem> extends ConcurrentMap<K, V> {
    long getMemoryCapacity();

    long getMemoryUsed();

    int capacity();
}
