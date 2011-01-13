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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 */
public enum Op {
    GET, GETS, APPEND, PREPEND, DELETE, DECR,
    INCR, REPLACE, ADD, SET, CAS, STATS, VERSION,
    QUIT, FLUSH_ALL, VERBOSITY;

    private static Map<ChannelBuffer, Op> opsbf = new HashMap<ChannelBuffer, Op>();

    static {
        for (int x = 0 ; x < Op.values().length; x++) {
            byte[] bytes = Op.values()[x].toString().toLowerCase().getBytes();
            opsbf.put(ChannelBuffers.wrappedBuffer(bytes), Op.values()[x]);
        }
    }


    public static Op FindOp(ChannelBuffer cmd) {
        cmd.readerIndex(0);
        return opsbf.get(cmd);
    }

}
