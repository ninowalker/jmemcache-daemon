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

import java.util.Arrays;

/**
 */
public enum Command {
    GET, GETS, APPEND, PREPEND, DELETE, DECR,
    INCR, REPLACE, ADD, SET, CAS, STATS, VERSION,
    QUIT, FLUSH_ALL;

    private static byte[][] commands = new byte[Command.values().length][];
    static {
        for (int x = 0 ; x < Command.values().length; x++)
            commands[x] = Command.values()[x].toString().toLowerCase().getBytes();
    }

    public static Command getCommand(byte[] cmd) {
        for (int x = 0 ; x < commands.length; x++) {
            if (Arrays.equals(cmd, commands[x])) return Command.values()[x];
        }
        return null;
    }
}
