/**
 *  Copyright 2008 ThimbleWare Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.thimbleware.jmemcached.protocol;


import com.thimbleware.jmemcached.Cache;
import com.thimbleware.jmemcached.MCElement;
import com.thimbleware.jmemcached.protocol.exceptions.UnknownCommandException;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Integer.parseInt;
import static java.lang.String.valueOf;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

// TODO implement flush_all delay

/**
 * The actual command handler, which is responsible for processing the CommandMessage instances
 * that are inbound from the protocol decoders.
 * <p/>
 * One instance is shared among the entire pipeline, since this handler is stateless, apart from some globals
 * for the entire daemon.
 * <p/>
 * The command handler produces ResponseMessages which are destined for the response encoder.
 */
@ChannelPipelineCoverage("all")
public final class MemcachedCommandHandler extends SimpleChannelUpstreamHandler {

    final Logger logger = LoggerFactory.getLogger(MemcachedCommandHandler.class);

    /**
     * The following state variables are universal for the entire daemon. These are used for statistics gathering.
     * In order for these values to work properly, the handler _must_ be declared with a ChannelPipelineCoverage
     * of "all".
     */
    public final String version;
    public final AtomicInteger curr_conns = new AtomicInteger();
    public final AtomicInteger total_conns = new AtomicInteger();
    public final AtomicInteger started = new AtomicInteger();          /* when the process was started */
    public final AtomicLong bytes_read = new AtomicLong();
    public final AtomicLong bytes_written = new AtomicLong();
    public final AtomicLong curr_bytes = new AtomicLong();

    public final int idle_limit;
    public final boolean verbose;


    /**
     * Initialize base values for status.
     */
    {
        curr_bytes.set(0);
        curr_conns.set(0);
        total_conns.set(0);
        bytes_read.set(0);
        bytes_written.set(0);
        started.set(Now());
    }

    /**
     * The actual physical data storage.
     */
    private Cache cache;

    /**
     * The channel group for the entire daemon, used for handling global cleanup on shutdown.
     */
    private DefaultChannelGroup channelGroup;

    /**
     * Construct the server session handler
     *
     * @param cache            the cache to use
     * @param memcachedVersion the version string to return to clients
     * @param verbosity        verbosity level for debugging
     * @param idle             how long sessions can be idle for
     * @param channelGroup
     */
    public MemcachedCommandHandler(Cache cache, String memcachedVersion, boolean verbosity, int idle, DefaultChannelGroup channelGroup) {
        this.cache = cache;

        version = memcachedVersion;
        verbose = verbosity;
        idle_limit = idle;
        this.channelGroup = channelGroup;
    }


    /**
     * On open we manage some statistics, and add this connection to the channel group.
     *
     * @param channelHandlerContext
     * @param channelStateEvent
     * @throws Exception
     */
    @Override
    public void channelOpen(ChannelHandlerContext channelHandlerContext, ChannelStateEvent channelStateEvent) throws Exception {
        total_conns.incrementAndGet();
        curr_conns.incrementAndGet();
        channelGroup.add(channelHandlerContext.getChannel());
    }

    /**
     * On close we manage some statistics, and remove this connection from the channel group.
     *
     * @param channelHandlerContext
     * @param channelStateEvent
     * @throws Exception
     */
    @Override
    public void channelClosed(ChannelHandlerContext channelHandlerContext, ChannelStateEvent channelStateEvent) throws Exception {
        curr_conns.decrementAndGet();
        channelGroup.remove(channelHandlerContext.getChannel());
    }


    /**
     * The actual meat of the matter.  Turn CommandMessages into executions against the physical cache, and then
     * pass on the downstream messages.
     *
     * @param channelHandlerContext
     * @param messageEvent
     * @throws Exception
     */

    @Override
    public void messageReceived(ChannelHandlerContext channelHandlerContext, MessageEvent messageEvent) throws Exception {
        if (!(messageEvent.getMessage() instanceof CommandMessage)) {
            // Ignore what this encoder can't encode.
            channelHandlerContext.sendUpstream(messageEvent);
            return;
        }

        CommandMessage command = (CommandMessage) messageEvent.getMessage();
        Command cmd = command.cmd;
        int cmdKeysSize = command.keys.size();

        // first process any messages in the delete queue
        cache.processDeleteQueue();

        // now do the real work
        if (this.verbose) {
            StringBuilder log = new StringBuilder();
            log.append(cmd);
            if (command.element != null) {
                log.append(" ").append(command.element.keystring);
            }
            for (int i = 0; i < cmdKeysSize; i++) {
                log.append(" ").append(command.keys.get(i));
            }
            logger.info(log.toString());
        }

        Channel channel = messageEvent.getChannel();
        Cache.StoreResponse ret;
        if (cmd == Command.GET || cmd == Command.GETS) {
            MCElement[] results = get(command.keys.toArray(new String[command.keys.size()]));
            ResponseMessage resp = new ResponseMessage(command).withElements(results);
            Channels.fireMessageReceived(channelHandlerContext, resp, channel.getRemoteAddress());
        } else if (cmd == Command.SET) {
            ret = cache.set(command.element);
            Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
        } else if (cmd == Command.CAS) {
            ret = cache.cas(command.cas_key, command.element);
            Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
        } else if (cmd == Command.ADD) {
            ret = cache.add(command.element);
            Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
        } else if (cmd == Command.REPLACE) {
            ret = cache.replace(command.element);
            Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
        } else if (cmd == Command.APPEND) {
            ret = cache.append(command.element);
            Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
        } else if (cmd == Command.PREPEND) {
            ret = cache.prepend(command.element);
            Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
        } else if (cmd == Command.INCR) {
            Integer incrDecrResp = cache.get_add(command.keys.get(0), command.incrAmount); // TODO support default value and expiry!!
            Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withIncrDecrResponse(incrDecrResp), channel.getRemoteAddress());
        } else if (cmd == Command.DECR) {
            Integer incrDecrResp = cache.get_add(command.keys.get(0), -1 * command.incrAmount);
            Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withIncrDecrResponse(incrDecrResp), channel.getRemoteAddress());
        } else if (cmd == Command.DELETE) {
            Cache.DeleteResponse dr = cache.delete(command.keys.get(0), command.time);
            Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withDeleteResponse(dr), channel.getRemoteAddress());
        } else if (cmd == Command.STATS) {

            // TODO stats has no equiv in binary protocol -- so we're sorta breaking encapsulation here by outputing the text protocol response here
            String option = "";
            if (cmdKeysSize > 0) {
                option = command.keys.get(0);
            }
            Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withStatResponse(stat(option)), channel.getRemoteAddress());

        } else if (cmd == Command.VERSION) {
            ResponseMessage responseMessage = new ResponseMessage(command);
            responseMessage.version = version;
            Channels.fireMessageReceived(channelHandlerContext, responseMessage, channel.getRemoteAddress());
        } else if (cmd == Command.QUIT) {
            channel.disconnect();
        } else if (cmd == Command.FLUSH_ALL) {
            Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withFlushResponse(cache.flush_all(command.time)), channel.getRemoteAddress());
        } else {
            throw new UnknownCommandException("unknown command:" + cmd);

        }

    }

    /**
     * Get an element from the cache
     *
     * @param keys the key for the element to lookup
     * @return the element, or 'null' in case of cache miss.
     */
    private MCElement[] get(String... keys) {
        return cache.get(keys);
    }


    /**
     * @return the current time in seconds (from epoch), used for expiries, etc.
     */
    private static int Now() {
        return (int) (System.currentTimeMillis() / 1000);
    }


    /**
     * Return runtime statistics
     *
     * @param arg additional arguments to the stats command
     * @return the full command response
     */
    private String stat(String arg) {

        StringBuilder builder = new StringBuilder();

        if (arg.equals("keys")) {
            for (String key : this.cache.keys()) {
                builder.append("STAT key ").append(key).append("\r\n");
            }
            builder.append("END\r\n");
            return builder.toString();
        }

        // stats we know
        builder.append("STAT version ").append(version).append("\r\n");
        builder.append("STAT cmd_gets ").append(valueOf(cache.getGetCmds())).append("\r\n");
        builder.append("STAT cmd_sets ").append(valueOf(cache.getSetCmds())).append("\r\n");
        builder.append("STAT get_hits ").append(valueOf(cache.getGetHits())).append("\r\n");
        builder.append("STAT get_misses ").append(valueOf(cache.getGetMisses())).append("\r\n");
        builder.append("STAT curr_connections ").append(valueOf(curr_conns)).append("\r\n");
        builder.append("STAT total_connections ").append(valueOf(total_conns)).append("\r\n");
        builder.append("STAT time ").append(valueOf(Now())).append("\r\n");
        builder.append("STAT uptime ").append(valueOf(Now() - this.started.intValue())).append("\r\n");

        builder.append("STAT cur_items ").append(valueOf(this.cache.getCurrentItems())).append("\r\n");
        builder.append("STAT limit_maxbytes ").append(valueOf(this.cache.getLimitMaxBytes())).append("\r\n");
        builder.append("STAT current_bytes ").append(valueOf(this.cache.getCurrentBytes())).append("\r\n");
        builder.append("STAT free_bytes ").append(valueOf(Runtime.getRuntime().freeMemory())).append("\r\n");

        // Not really the same thing precisely, but meaningful nonetheless. potentially this should be renamed
        builder.append("STAT pid " + Thread.currentThread().getId() + "\r\n");

        // stuff we know nothing about; gets faked only because some clients expect this

        builder.append("STAT rusage_user 0:0\r\n");
        builder.append("STAT rusage_system 0:0\r\n");
        builder.append("STAT connection_structures 0\r\n");

        // TODO we could collect these
        builder.append("STAT bytes_read 0\r\n");
        builder.append("STAT bytes_written 0\r\n");
        builder.append("END\r\n");

        return builder.toString();
    }


}