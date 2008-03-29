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

/**
 * Represents information about a cache entry.
 */
public final class MCElement implements Serializable {
    public int expire = 0;
    public String flags;
    public int data_length = 0;
    public byte[] data;
    public String keystring;
}