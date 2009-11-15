package com.thimbleware.jmemcached;

import com.thimbleware.jmemcached.storage.hash.SizedItem;

import java.io.Serializable;

/**
 */
public interface CacheElement extends Serializable, SizedItem {
    int THIRTY_DAYS = 60 * 60 * 24 * 30;

    int size();

    int hashCode();

    int getExpire();

    int getFlags();

    byte[] getData();

    void setData(byte[] data);

    String getKeystring();

    long getCasUnique();

    void setCasUnique(long casUnique);

    boolean isBlocked();

    void setBlocked(boolean blocked);

    long getBlockedUntil();

    void setBlockedUntil(long blockedUntil);
}
