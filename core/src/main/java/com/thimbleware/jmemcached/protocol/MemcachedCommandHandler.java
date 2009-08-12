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
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;

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
            handleGets(channelHandlerContext, command, channel);
        } else if (cmd == Command.SET) {
            handleSet(channelHandlerContext, command, channel);
        } else if (cmd == Command.CAS) {
            handleCas(channelHandlerContext, command, channel);
        } else if (cmd == Command.ADD) {
            handleAdd(channelHandlerContext, command, channel);
        } else if (cmd == Command.REPLACE) {
            handleReplace(channelHandlerContext, command, channel);
        } else if (cmd == Command.APPEND) {
            handleAppend(channelHandlerContext, command, channel);
        } else if (cmd == Command.PREPEND) {
            handlePrepend(channelHandlerContext, command, channel);
        } else if (cmd == Command.INCR) {
            handleIncr(channelHandlerContext, command, channel);
        } else if (cmd == Command.DECR) {
            handleDecr(channelHandlerContext, command, channel);
        } else if (cmd == Command.DELETE) {
            handleDelete(channelHandlerContext, command, channel);
        } else if (cmd == Command.STATS) {
            handleStats(channelHandlerContext, command, cmdKeysSize, channel);
        } else if (cmd == Command.VERSION) {
            handleVersion(channelHandlerContext, command, channel);
        } else if (cmd == Command.QUIT) {
            handleQuit(channel);
        } else if (cmd == Command.FLUSH_ALL) {
            handleFlush(channelHandlerContext, command, channel);
        } else if (cmd == null) {
            // NOOP
            handleNoOp(channelHandlerContext, command);
        } else {
            throw new UnknownCommandException("unknown command:" + cmd);

        }

    }

    protected void handleNoOp(ChannelHandlerContext channelHandlerContext, CommandMessage command) {
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command));
    }

    protected void handleFlush(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withFlushResponse(cache.flush_all(command.time)), channel.getRemoteAddress());
    }

    protected void handleQuit(Channel channel) {
        channel.disconnect();
    }

    protected void handleVersion(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        ResponseMessage responseMessage = new ResponseMessage(command);
        responseMessage.version = version;
        Channels.fireMessageReceived(channelHandlerContext, responseMessage, channel.getRemoteAddress());
    }

    protected void handleStats(ChannelHandlerContext channelHandlerContext, CommandMessage command, int cmdKeysSize, Channel channel) {
        String option = "";
        if (cmdKeysSize > 0) {
            option = command.keys.get(0);
        }
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withStatResponse(stat(option)), channel.getRemoteAddress());
    }

    protected void handleDelete(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        Cache.DeleteResponse dr = cache.delete(command.keys.get(0), command.time);
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withDeleteResponse(dr), channel.getRemoteAddress());
    }

    protected void handleDecr(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        Integer incrDecrResp = cache.get_add(command.keys.get(0), -1 * command.incrAmount);
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withIncrDecrResponse(incrDecrResp), channel.getRemoteAddress());
    }

    protected void handleIncr(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        Integer incrDecrResp = cache.get_add(command.keys.get(0), command.incrAmount); // TODO support default value and expiry!!
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withIncrDecrResponse(incrDecrResp), channel.getRemoteAddress());
    }

    protected void handlePrepend(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        Cache.StoreResponse ret;
        ret = cache.prepend(command.element);
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
    }

    protected void handleAppend(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        Cache.StoreResponse ret;
        ret = cache.append(command.element);
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
    }

    protected void handleReplace(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        Cache.StoreResponse ret;
        ret = cache.replace(command.element);
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
    }

    protected void handleAdd(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        Cache.StoreResponse ret;
        ret = cache.add(command.element);
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
    }

    protected void handleCas(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        Cache.StoreResponse ret;
        ret = cache.cas(command.cas_key, command.element);
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
    }

    protected void handleSet(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        Cache.StoreResponse ret;
        ret = cache.set(command.element);
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
    }

    protected void handleGets(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        MCElement[] results = get(command.keys.toArray(new String[command.keys.size()]));
        ResponseMessage resp = new ResponseMessage(command).withElements(results);
        Channels.fireMessageReceived(channelHandlerContext, resp, channel.getRemoteAddress());
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
    private Map<String, Set<String>> stat(String arg) {
        Map<String, Set<String>> result = new HashMap<String, Set<String>>();

        if (arg.equals("keys")) {
            for (String key : this.cache.keys()) {
                multiSet(result, "key", key);
            }

            return result;
        }

        // stats we know
        multiSet(result, "version", version);
        multiSet(result, "cmd_gets", valueOf(cache.getGetCmds()));
        multiSet(result, "cmd_sets", valueOf(cache.getSetCmds()));
        multiSet(result, "get_hits", valueOf(cache.getGetHits()));
        multiSet(result, "get_misses", valueOf(cache.getGetMisses()));
        multiSet(result, "curr_connections", valueOf(curr_conns));
        multiSet(result, "total_connections", valueOf(total_conns));
        multiSet(result, "time", valueOf(valueOf(Now())));
        multiSet(result, "uptime", valueOf(Now() - this.started.intValue()));
        multiSet(result, "cur_items", valueOf(this.cache.getCurrentItems()));
        multiSet(result, "limit_maxbytes", valueOf(this.cache.getLimitMaxBytes()));
        multiSet(result, "current_bytes", valueOf(this.cache.getCurrentBytes()));
        multiSet(result, "free_bytes", valueOf(Runtime.getRuntime().freeMemory()));

        // Not really the same thing precisely, but meaningful nonetheless. potentially this should be renamed
        multiSet(result, "pid", valueOf(Thread.currentThread().getId()));

        // stuff we know nothing about; gets faked only because some clients expect this
        multiSet(result, "rusage_user", "0:0");
        multiSet(result, "rusage_system", "0:0");
        multiSet(result, "connection_structures", "0");

        // TODO we could collect these stats
        multiSet(result, "bytes_read", "0");
        multiSet(result, "bytes_written", "0");

        return result;
    }

    private void multiSet(Map<String, Set<String>> map, String key, String val) {
        Set<String> cur = map.get(key);
        if (cur == null) {
            cur = new HashSet<String>();
        }
        cur.add(val);
        map.put(key, cur);
    }

}