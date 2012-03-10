package com.thimbleware.jmemcached.protocol.binary;

import com.thimbleware.jmemcached.Cache;
import com.thimbleware.jmemcached.CacheElementFactory;
import com.thimbleware.jmemcached.protocol.MemcachedCommandHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPipelineFactory;
import io.netty.channel.Channels;
import io.netty.channel.group.DefaultChannelGroup;


public class MemcachedBinaryPipelineFactory implements ChannelPipelineFactory {

    private final MemcachedBinaryCommandDecoder decoder;
    private final MemcachedCommandHandler memcachedCommandHandler;
    private final MemcachedBinaryResponseEncoder memcachedBinaryResponseEncoder = new MemcachedBinaryResponseEncoder();

    public MemcachedBinaryPipelineFactory(CacheElementFactory factory, Cache cache, String version, boolean verbose, int idleTime, DefaultChannelGroup channelGroup) {
        memcachedCommandHandler = new MemcachedCommandHandler(cache, version, verbose, idleTime, channelGroup);
        decoder = new MemcachedBinaryCommandDecoder(factory);
    }

    public ChannelPipeline getPipeline() throws Exception {
        return Channels.pipeline(
                decoder,
                memcachedCommandHandler,
                memcachedBinaryResponseEncoder
        );
    }
}
