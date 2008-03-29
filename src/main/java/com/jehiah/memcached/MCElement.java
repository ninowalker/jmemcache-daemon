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

public final class MCElement implements Serializable {
    public int expire = 0;
    public String flags;
    public int data_length = 0;
    public byte[] data;
    public String keystring;

    /*
      * Each item sent by the server looks like this:
      VALUE <key> <flags> <bytes>\r\n
      <data block>\r\n
      */
    public String toString() {
        return new StringBuffer().append("VALUE ").append(this.keystring).append(" ").append(this.flags).append(" ").append(this.data_length).append("\r\n").append(this.data).append("\r\n").toString();
//		return "VALUE " + this.keystring + " " + this.flags + " " +String.valueOf(this.data_length)+ "\r\n"+ this.data + "\r\n";
    }

}