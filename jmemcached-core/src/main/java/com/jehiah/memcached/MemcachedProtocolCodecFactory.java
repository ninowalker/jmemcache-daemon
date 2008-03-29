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

import org.apache.mina.filter.codec.demux.DemuxingProtocolCodecFactory;

public class MemcachedProtocolCodecFactory extends DemuxingProtocolCodecFactory
{
    public MemcachedProtocolCodecFactory()
    {
        super.register( CommandDecoder.class );
        super.register( ResponseEncoder.class );
    }
}
