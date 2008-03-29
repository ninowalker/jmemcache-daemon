/**
 *
 * Java Memcached Server
 *
 * http://jehiah.com/projects/j-memcached
 *
 * Distributed under GPL
 * @author Jehiah Czebotar
 */
package com.jehiah.memcached;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * The payload object holding the parsed message.
 */
public final class CommandMessage implements Serializable {
    public String cmd;
    public MCElement element;
    public ArrayList<String> keys;

    public CommandMessage(String cmd) {
        this.cmd = cmd;
        element = null;
        keys = new ArrayList<String>();
    }
}