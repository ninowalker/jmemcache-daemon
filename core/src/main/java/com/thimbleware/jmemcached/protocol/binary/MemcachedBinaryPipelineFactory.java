package com.thimbleware.jmemcached.protocol.binary;

import com.thimbleware.jmemcached.Cache;
import com.thimbleware.jmemcached.protocol.MemcachedCommandHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.handler.codec.string.StringEncoder;


public class MemcachedBinaryPipelineFactory implements ChannelPipelineFactory {

    private Cache cache;
    private String version;
    private boolean verbose;
    private int idleTime;

    private DefaultChannelGroup channelGroup;

    public MemcachedBinaryPipelineFactory(Cache cache, String version, boolean verbose, int idleTime, DefaultChannelGroup channelGroup) {
        this.cache = cache;
        this.version = version;
        this.verbose = verbose;
        this.idleTime = idleTime;
        this.channelGroup = channelGroup;
    }

    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("commandDecoder", new MemcachedBinaryCommandDecoder());
        pipeline.addAfter("commandDecoder", "commandHandler", new MemcachedCommandHandler(cache, version, verbose, idleTime, channelGroup));
        pipeline.addAfter("commandHandler", "responseEncoder", new MemcachedBinaryResponseEncoder());

        return pipeline;
    }
}
