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

import com.thimbleware.jmemcached.util.BufferUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;


/**
 * Represents information about a cache entry.
 */
public final class LocalCacheElement implements CacheElement, Externalizable {
    private int expire ;
    private int flags;
    private ByteBuffer data;
    private Key key;
    private long casUnique = 0L;
    private boolean blocked = false;
    private long blockedUntil;

    public LocalCacheElement() {
    }

    public LocalCacheElement(Key key) {
        this.key = key;
    }

    public LocalCacheElement(Key key, int flags, int expire, long casUnique) {
        this.key = key;
        this.flags = flags;
        this.expire = expire;
        this.casUnique = casUnique;
    }

    /**
     * @return the current time in seconds
     */
    public static int Now() {
        return (int) (System.currentTimeMillis() / 1000);
    }

    public int size() {
        return getData().limit();
    }

    public LocalCacheElement append(LocalCacheElement appendElement) {
        int newLength = size() + appendElement.size();
        LocalCacheElement appendedElement = new LocalCacheElement(getKey(), getFlags(), getExpire(), 0L);
        ByteBuffer appended = ByteBuffer.allocateDirect(newLength);
        ByteBuffer existing = getData();
        ByteBuffer append = appendElement.getData();

        appended.put(existing);
        appended.put(append);

        appended.flip();
        appended.rewind();

        existing.rewind();
        append.rewind();

        appendedElement.setData(appended);
        appendedElement.setCasUnique(appendedElement.getCasUnique() + 1);

        return appendedElement;
    }

    public LocalCacheElement prepend(LocalCacheElement prependElement) {
        int newLength = size() + prependElement.size();

        LocalCacheElement prependedElement = new LocalCacheElement(getKey(), getFlags(), getExpire(), 0L);
        ByteBuffer prepended = ByteBuffer.allocateDirect(newLength);
        ByteBuffer prepend = prependElement.getData();
        ByteBuffer existing = getData();

        prepended.put(prepend);
        prepended.put(existing);

        existing.rewind();
        prepend.rewind();

        prepended.flip();
        prepended.rewind();

        prependedElement.setData(prepended);
        prependedElement.setCasUnique(prependedElement.getCasUnique() + 1);

        return prependedElement;
    }

    public static class IncrDecrResult {
        int oldValue;
        LocalCacheElement replace;

        public IncrDecrResult(int oldValue, LocalCacheElement replace) {
            this.oldValue = oldValue;
            this.replace = replace;
        }
    }

    public IncrDecrResult add(int mod) {
        // TODO handle parse failure!
        int modVal = BufferUtils.atoi(getData().array()) + mod; // change value
        if (modVal < 0) {
            modVal = 0;

        } // check for underflow

        byte[] newData = BufferUtils.itoa(modVal);

        LocalCacheElement replace = new LocalCacheElement(getKey(), getFlags(), getExpire(), 0L);
        ByteBuffer buf = ByteBuffer.wrap(newData);
        replace.setData(buf);
        replace.setCasUnique(replace.getCasUnique() + 1);

        return new IncrDecrResult(modVal, replace);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocalCacheElement that = (LocalCacheElement) o;

        if (blocked != that.blocked) return false;
        if (blockedUntil != that.blockedUntil) return false;
        if (casUnique != that.casUnique) return false;
        if (expire != that.expire) return false;
        if (flags != that.flags) return false;
        if (!data.equals(that.data)) return false;
        if (!key.equals(that.key)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = expire;
        result = 31 * result + flags;
        result = 31 * result + (data != null ? data.hashCode() : 0);
        result = 31 * result + key.hashCode();
        result = 31 * result + (int) (casUnique ^ (casUnique >>> 32));
        result = 31 * result + (blocked ? 1 : 0);
        result = 31 * result + (int) (blockedUntil ^ (blockedUntil >>> 32));
        return result;
    }

    public static LocalCacheElement key(Key key) {
        return new LocalCacheElement(key);
    }

    public int getExpire() {
        return expire;
    }

    public int getFlags() {
        return flags;
    }

    public ByteBuffer getData() {
        data.rewind();
        return data;
    }

    public Key getKey() {
        return key;
    }

    public long getCasUnique() {
        return casUnique;
    }

    public boolean isBlocked() {
        return blocked;
    }

    public long getBlockedUntil() {
        return blockedUntil;
    }

    public void setCasUnique(long casUnique) {
        this.casUnique = casUnique;
    }

    public void block(long blockedUntil) {
        this.blocked = true;
        this.blockedUntil = blockedUntil;
    }


    public void setData(ByteBuffer data) {
        data.rewind();
        this.data = data;
    }

    public void readExternal(ObjectInput in) throws IOException{
        expire = in.readInt() ;
        flags = in.readInt();

        final int length = in.readInt();
        int readSize = 0;
        byte[] dataArrary = new byte[length];
        while( readSize < length)
            readSize += in.read(dataArrary, readSize, length - readSize);
        data = ByteBuffer.wrap(dataArrary);

        key = new Key(new byte[in.readInt()]);
        in.read(key.bytes);
        casUnique = in.readLong();
        blocked = in.readBoolean();
        blockedUntil = in.readLong();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(expire) ;
        out.writeInt(flags);
        byte[] dataArray = data.array();
        out.writeInt(dataArray.length);
        out.write(dataArray);
        out.write(key.bytes.length);
        out.write(key.bytes);
        out.writeLong(casUnique);
        out.writeBoolean(blocked);
        out.writeLong(blockedUntil);
    }
}