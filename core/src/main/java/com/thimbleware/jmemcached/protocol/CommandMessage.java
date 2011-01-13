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
package com.thimbleware.jmemcached.protocol;

import com.thimbleware.jmemcached.CacheElement;
import com.thimbleware.jmemcached.Key;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * The payload object holding the parsed message.
 */
public final class CommandMessage<CACHE_ELEMENT extends CacheElement> implements Serializable {


    public Op op;
    public CACHE_ELEMENT element;
    public List<Key> keys;
    public boolean noreply;
    public long cas_key;
    public int time = 0;
    public int opaque;
    public boolean addKeyToResponse = false;

    public int incrExpiry;
    public int incrAmount;

    private CommandMessage(Op op) {
        this.op = op;
        element = null;
    }

    public void setKey(ChannelBuffer key) {
        this.keys = new ArrayList<Key>();
        this.keys.add(new Key(key));
    }

    public void setKeys(List<ChannelBuffer> keys) {
        this.keys = new ArrayList<Key>(keys.size());
        for (ChannelBuffer key : keys) {
            this.keys.add(new Key(key));
        }
    }

    public static CommandMessage command(Op operation) {
        return new CommandMessage(operation);
    }
}