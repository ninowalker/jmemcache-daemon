package com.thimbleware.jmemcached;

import java.util.Set;
import java.util.Map;

/**
 */
public interface Cache<CACHE_ELEMENT extends CacheElement> {
    DeleteResponse delete(String key, int time);

    StoreResponse add(CACHE_ELEMENT e);

    StoreResponse replace(CACHE_ELEMENT e);

    StoreResponse append(CACHE_ELEMENT element);

    StoreResponse prepend(CACHE_ELEMENT element);

    StoreResponse set(CACHE_ELEMENT e);

    StoreResponse cas(Long cas_key, CACHE_ELEMENT e);

    Integer get_add(String key, int mod);

    CACHE_ELEMENT[] get(String ... keys);

    boolean flush_all();

    boolean flush_all(int expire);

    void close();

    Set<String> keys();

    long getCurrentItems();

    long getLimitMaxBytes();

    long getCurrentBytes();

    int getGetCmds();

    int getSetCmds();

    int getGetHits();

    int getGetMisses();

    Map<String, Set<String>> stat(String arg);

    void processDeleteQueue();

    public enum StoreResponse {
        STORED, NOT_STORED, EXISTS, NOT_FOUND
    }

    public enum DeleteResponse {
        DELETED, NOT_FOUND
    }
}
