/**
 *
 * Java Memcached Server
 *
 * http://jehiah.com/projects/j-memcached
 *
 * Distributed under GPL
 * @author Jehiah Czebotar
 */
package com.thimbleware.jmemcached;

import org.apache.mina.common.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;
import org.apache.mina.filter.codec.demux.MessageEncoder;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * MINA MessageEncoder responsible for writing a ResponseMessage into the network stream.
 */
public class ResponseEncoder implements MessageEncoder {

    private static final Set<Class<ResponseMessage>> TYPES;

    static {
        Set<Class<ResponseMessage>> types = new HashSet<Class<ResponseMessage>>();
        types.add(ResponseMessage.class);
        TYPES = Collections.unmodifiableSet(types);
    }

    public void encode(IoSession session, Object message, ProtocolEncoderOutput out) throws Exception {
        ResponseMessage m = (ResponseMessage) message;
        m.out.flip();
        out.write(m.out);
    }

    @SuppressWarnings("unchecked")
    public Set getMessageTypes() {
        return TYPES;
    }
}
