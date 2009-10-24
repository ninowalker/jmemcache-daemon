package com.thimbleware.jmemcached;

import com.thimbleware.jmemcached.storage.hash.SizedItem;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 * User: ryan
 * Date: Oct 3, 2009
 * Time: 10:55:54 PM
 * To change this template use File | Settings | File Templates.
 */
public interface CacheElement extends Serializable, SizedItem {
    int THIRTY_DAYS = 60 * 60 * 24 * 30;

    int size();

    @Override
    boolean equals(Object o);

    @Override
    int hashCode();

    Integer getExpire();

    void setExpire(Integer expire);

    Integer getFlags();

    void setFlags(Integer flags);

    Integer getDataLength();

    void setDataLength(Integer dataLength);

    byte[] getData();

    void setData(byte[] data);

    String getKeystring();

    void setKeystring(String keystring);

    Long getCasUnique();

    void setCasUnique(Long casUnique);

    Boolean isBlocked();

    void setBlocked(Boolean blocked);

    Long getBlockedUntil();

    void setBlockedUntil(Long blockedUntil);
}
