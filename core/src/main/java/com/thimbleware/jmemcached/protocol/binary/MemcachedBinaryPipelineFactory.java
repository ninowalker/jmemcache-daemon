package com.thimbleware.jmemcached.protocol.binary;

import com.thimbleware.jmemcached.Cache;
import com.thimbleware.jmemcached.protocol.MemcachedCommandHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.DefaultChannelGroup;


public class MemcachedBinaryPipelineFactory implements ChannelPipelineFactory {

    private final MemcachedBinaryCommandDecoder decoder =  new MemcachedBinaryCommandDecoder();
    private final MemcachedCommandHandler memcachedCommandHandler;
    private final MemcachedBinaryResponseEncoder memcachedBinaryResponseEncoder = new MemcachedBinaryResponseEncoder();

    public MemcachedBinaryPipelineFactory(Cache cache, String version, boolean verbose, int idleTime, DefaultChannelGroup channelGroup) {
        memcachedCommandHandler = new MemcachedCommandHandler(cache, version, verbose, idleTime, channelGroup);
    }

    public ChannelPipeline getPipeline() throws Exception {
        return Channels.pipeline(
                decoder,
                memcachedCommandHandler,
                memcachedBinaryResponseEncoder
        );
    }
}
