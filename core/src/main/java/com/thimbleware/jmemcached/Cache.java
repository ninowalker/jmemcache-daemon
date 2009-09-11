package com.thimbleware.jmemcached;

import java.util.Set;
import java.util.Map;

/**
 */
public interface Cache {
    DeleteResponse delete(String key, int time);

    StoreResponse add(MCElement e);

    StoreResponse replace(MCElement e);

    StoreResponse append(MCElement element);

    StoreResponse prepend(MCElement element);

    StoreResponse set(MCElement e);

    StoreResponse cas(Long cas_key, MCElement e);

    Integer get_add(String key, int mod);

    MCElement[] get(String ... keys);

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

    public enum StoreResponse {
        STORED, NOT_STORED, EXISTS, NOT_FOUND
    }

    public enum DeleteResponse {
        DELETED, NOT_FOUND
    }
}
