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

/**
 */
public enum Op {
    GET, GETS, APPEND, PREPEND, DELETE, DECR,
    INCR, REPLACE, ADD, SET, CAS, STATS, VERSION,
    QUIT, FLUSH_ALL, VERBOSITY;

    private static byte[][] ops = new byte[Op.values().length][];
    private static ChannelBuffer[] opsbf = new ChannelBuffer[Op.values().length];

    static {
        for (int x = 0 ; x < Op.values().length; x++) {
            byte[] bytes = Op.values()[x].toString().toLowerCase().getBytes();
            ops[x] = bytes;
            opsbf[x] = ChannelBuffers.wrappedBuffer(bytes);
        }
    }

    public static Op FindOp(byte[] cmd) {
        for (int x = 0 ; x < ops.length; x++) {
            if (Arrays.equals(cmd, ops[x])) return Op.values()[x];
        }
        return null;
    }

    public static Op FindOp(ChannelBuffer cmd) {
        for (int x = 0 ; x < ops.length; x++) {
            opsbf[x].readerIndex(0);
            cmd.readerIndex(0);
            if (opsbf[x].equals(cmd)) return Op.values()[x];
        }
        return null;
    }

}
