package com.thimbleware.jmemcached.protocol;

import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import com.thimbleware.jmemcached.Cache;

/**
 */
public class MemcachedPipelineFactory implements ChannelPipelineFactory {

    private Cache cache;
    private String version;
    private boolean verbose;
    private int idleTime;

    private int frameSize;

    public MemcachedPipelineFactory(Cache cache, String version, boolean verbose, int idleTime, int frameSize) {
        this.cache = cache;
        this.version = version;
        this.verbose = verbose;
        this.idleTime = idleTime;
        this.frameSize = frameSize;
    }

    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        SessionStatus status = new SessionStatus().ready();
        pipeline.addLast("frameHandler", new MemcachedFrameHandler(status, frameSize));
        pipeline.addAfter("frameHandler", "stringDecoder", new StringDecoder());
        pipeline.addAfter("stringDecoder", "commandDecoder", new MemcachedCommandDecoder(status));
        pipeline.addAfter("commandDecoder", "commandHandler", new MemcachedCommandHandler(cache, version, verbose, idleTime));
        pipeline.addAfter("commandHandler", "responseHandler", new StringEncoder());

        return pipeline;
    }
}
