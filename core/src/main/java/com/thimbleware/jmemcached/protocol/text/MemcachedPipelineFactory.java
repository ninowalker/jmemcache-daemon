package com.thimbleware.jmemcached.protocol.text;

import com.thimbleware.jmemcached.Cache;
import com.thimbleware.jmemcached.protocol.MemcachedCommandHandler;
import com.thimbleware.jmemcached.protocol.SessionStatus;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.handler.codec.string.StringEncoder;

/**
 */
public class MemcachedPipelineFactory implements ChannelPipelineFactory {

    private Cache cache;
    private String version;
    private boolean verbose;
    private int idleTime;

    private int frameSize;
    private DefaultChannelGroup channelGroup;

    public MemcachedPipelineFactory(Cache cache, String version, boolean verbose, int idleTime, int frameSize, DefaultChannelGroup channelGroup) {
        this.cache = cache;
        this.version = version;
        this.verbose = verbose;
        this.idleTime = idleTime;
        this.frameSize = frameSize;
        this.channelGroup = channelGroup;
    }

    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        SessionStatus status = new SessionStatus().ready();
        pipeline.addLast("frameHandler", new MemcachedFrameDecoder(status, frameSize));
        pipeline.addAfter("frameHandler", "commandDecoder", createMemcachedCommandDecoder(status));
        pipeline.addAfter("commandDecoder", "commandHandler", createMemcachedCommandHandler(cache, version, verbose, idleTime, channelGroup));
        pipeline.addAfter("commandHandler", "responseEncoder", createMemcachedResponseEncoder());
        pipeline.addAfter("responseEncoder", "responseHandler", createResponseEncoder());

        return pipeline;
    }

    private StringEncoder createResponseEncoder() {
        return new StringEncoder();
    }

    private MemcachedResponseEncoder createMemcachedResponseEncoder() {
        return new MemcachedResponseEncoder();
    }

    private MemcachedCommandDecoder createMemcachedCommandDecoder(SessionStatus status) {
        return new MemcachedCommandDecoder(status);
    }

    protected MemcachedCommandHandler createMemcachedCommandHandler(Cache cache, String version, boolean verbose, int idleTime, DefaultChannelGroup channelGroup) {
        return new MemcachedCommandHandler(cache, version, verbose, idleTime, channelGroup);
    }
}
