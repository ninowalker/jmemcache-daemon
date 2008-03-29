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

import org.apache.mina.common.ByteBuffer;

import java.io.Serializable;

public class ResponseMessage implements Serializable {
    public ByteBuffer out;

    public ResponseMessage() {
        out = ByteBuffer.allocate(1024);
        out.setAutoExpand(true);
    }
}
