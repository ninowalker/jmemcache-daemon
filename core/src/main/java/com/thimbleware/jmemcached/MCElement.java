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

import com.thimbleware.jmemcached.storage.hash.SizedItem;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Represents information about a cache entry.
 */
public final class MCElement implements Serializable, SizedItem {
    public int expire = 0;
    public int flags;
    public int dataLength = 0;
    public byte[] data;
    public String keystring;
    public long cas_unique;
    public boolean blocked = false;
    public long blocked_until = 0;
    public final static int THIRTY_DAYS = 60 * 60 * 24 * 30;

    public MCElement() {
    }

    public MCElement(String keystring, int flags, int expire, int dataLength) {
        this.keystring = keystring;
        this.flags = flags;
        this.expire = expire;
        this.dataLength = dataLength;
    }

    /**
     * @return the current time in seconds
     */
    public static final int Now() {
        return (int) (System.currentTimeMillis() / 1000);
    }

    public int size() {
        return dataLength;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MCElement mcElement = (MCElement) o;

        if (blocked != mcElement.blocked) return false;
        if (blocked_until != mcElement.blocked_until) return false;
        if (cas_unique != mcElement.cas_unique) return false;
        if (dataLength != mcElement.dataLength) return false;
        if (expire != mcElement.expire) return false;
        if (flags != mcElement.flags) return false;
        if (!Arrays.equals(data, mcElement.data)) return false;
        if (!keystring.equals(mcElement.keystring)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = expire;
        result = 31 * result + flags;
        result = 31 * result + dataLength;
        result = 31 * result + Arrays.hashCode(data);
        result = 31 * result + keystring.hashCode();
        result = 31 * result + (int) (cas_unique ^ (cas_unique >>> 32));
        result = 31 * result + (blocked ? 1 : 0);
        result = 31 * result + (int) (blocked_until ^ (blocked_until >>> 32));
        return result;
    }
}