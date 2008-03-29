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

public class CommandMessage implements Serializable {
    // should be byte buffers?
    public String cmd;
    public MCElement element;
    public ArrayList<String> keys;

    public CommandMessage(String cmd) {
        this.cmd = cmd;
        element = null;
        keys = new ArrayList<String>();
    }
}