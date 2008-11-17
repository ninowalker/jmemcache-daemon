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

import java.io.Serializable;

/**
 * Represents information about a cache entry.
 */
public final class MCElement implements Serializable {
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
}