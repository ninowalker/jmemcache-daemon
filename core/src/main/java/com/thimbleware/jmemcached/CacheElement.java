package com.thimbleware.jmemcached;

import com.thimbleware.jmemcached.storage.hash.SizedItem;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 */
public interface CacheElement extends Serializable, SizedItem {
    public final static int THIRTY_DAYS = 2592000;

    int size();

    int hashCode();

    int getExpire();

    int getFlags();

    ChannelBuffer getData();

    void setData(ChannelBuffer data);

    Key getKey();

    long getCasUnique();

    void setCasUnique(long casUnique);

    boolean isBlocked();

    void block(long blockedUntil);

    long getBlockedUntil();

    CacheElement append(LocalCacheElement element);

    CacheElement prepend(LocalCacheElement element);

    LocalCacheElement.IncrDecrResult add(int mod);
}
