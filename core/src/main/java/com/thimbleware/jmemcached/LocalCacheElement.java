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

import java.util.Arrays;

/**
 * Represents information about a cache entry.
 */
public final class LocalCacheElement implements CacheElement {
    private Integer expire ;
    private Integer flags;
    private Integer dataLength;
    private byte[] data;
    private String keystring;
    private Long casUnique = 0L;
    private Boolean blocked = false;
    private Long blockedUntil;

    public LocalCacheElement() {
    }

    private LocalCacheElement(String keystring) {
        this.setKeystring(keystring);
    }

    public LocalCacheElement(String keystring, int flags, int expire, int dataLength) {
        this.setKeystring(keystring);
        this.setFlags(flags);
        this.setExpire(expire);
        this.setDataLength(dataLength);
    }

    /**
     * @return the current time in seconds
     */
    public static int Now() {
        return (int) (System.currentTimeMillis() / 1000);
    }

    public int size() {
        return getDataLength();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CacheElement cacheElement = (CacheElement) o;

        if (isBlocked() != cacheElement.isBlocked()) return false;
        if (getBlockedUntil() != cacheElement.getBlockedUntil()) return false;
        if (getCasUnique() != cacheElement.getCasUnique()) return false;
        if (getDataLength() != cacheElement.getDataLength()) return false;
        if (getExpire() != cacheElement.getExpire()) return false;
        if (getFlags() != cacheElement.getFlags()) return false;
        if (!Arrays.equals(getData(), cacheElement.getData())) return false;
        if (!getKeystring().equals(cacheElement.getKeystring())) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = getExpire();
        result = 31 * result + getFlags();
        result = 31 * result + getDataLength();
        result = 31 * result + Arrays.hashCode(getData());
        result = 31 * result + getKeystring().hashCode();
        result = 31 * result + (int) (getCasUnique() ^ (getCasUnique() >>> 32));
        result = 31 * result + (isBlocked() ? 1 : 0);
        result = 31 * result + (int) (getBlockedUntil() ^ (getBlockedUntil() >>> 32));
        return result;
    }

    public static LocalCacheElement key(String key) {
        return new LocalCacheElement(key);
    }

    public Integer getExpire() {
        return expire;
    }

    public void setExpire(Integer expire) {
        this.expire = expire;
    }

    public Integer getFlags() {
        return flags;
    }

    public void setFlags(Integer flags) {
        this.flags = flags;
    }

    public Integer getDataLength() {
        return dataLength;
    }

    public void setDataLength(Integer dataLength) {
        this.dataLength = dataLength;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public String getKeystring() {
        return keystring;
    }

    public void setKeystring(String keystring) {
        this.keystring = keystring;
    }

    public Long getCasUnique() {
        return casUnique;
    }

    public void setCasUnique(Long casUnique) {
        this.casUnique = casUnique;
    }

    public Boolean isBlocked() {
        return blocked;
    }

    public void setBlocked(Boolean blocked) {
        this.blocked = blocked;
    }

    public Long getBlockedUntil() {
        return blockedUntil;
    }

    public void setBlockedUntil(Long blockedUntil) {
        this.blockedUntil = blockedUntil;
    }
}