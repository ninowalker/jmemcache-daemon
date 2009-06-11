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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.channel.*;

import static java.lang.Integer.parseInt;
import static java.lang.String.valueOf;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.thimbleware.jmemcached.Cache;
import com.thimbleware.jmemcached.MCElement;
import com.thimbleware.jmemcached.protocol.exceptions.ClientException;

// TODO implement flush_all delay

/**
 * The heart of the daemon, responsible for handling the creation and destruction of network
 * sessions, keeping cache statistics, and (most importantly) processing inbound (parsed) commands and then passing on
 * a response message for output.
 */
@ChannelPipelineCoverage("all")
public final class MemcachedCommandHandler extends SimpleChannelUpstreamHandler {

    final Logger logger = LoggerFactory.getLogger(MemcachedCommandHandler.class);

    public final String version;
    public static final AtomicInteger curr_conns = new AtomicInteger();
    public static final AtomicInteger total_conns = new AtomicInteger();
    public static final AtomicInteger started = new AtomicInteger();          /* when the process was started */

    public static final AtomicLong bytes_read = new AtomicLong();
    public static final AtomicLong bytes_written = new AtomicLong();
    public static final AtomicLong curr_bytes = new AtomicLong();

    public final int idle_limit;
    public final boolean verbose;

    public static final String VALUE = "VALUE ";

    static {
        curr_bytes.set(0);
        curr_conns.set(0);
        total_conns.set(0);
        bytes_read.set(0);
        bytes_written.set(0);
        started.set(Now());
    }

    /**
     */
    private Cache cache;

    /**
     * Construct the server session handler
     *
     * @param cache            the cache to use
     * @param memcachedVersion the version string to return to clients
     * @param verbosity        verbosity level for debugging
     * @param idle             how long sessions can be idle for
     * @param status
     */
    public MemcachedCommandHandler(Cache cache, String memcachedVersion, boolean verbosity, int idle) {

        this.cache = cache;

        version = memcachedVersion;
        verbose = verbosity;
        idle_limit = idle;
    }

    @Override
    public void channelOpen(ChannelHandlerContext channelHandlerContext, ChannelStateEvent channelStateEvent) throws Exception {
        total_conns.incrementAndGet();
        curr_conns.incrementAndGet();
    }

    @Override
    public void channelClosed(ChannelHandlerContext channelHandlerContext, ChannelStateEvent channelStateEvent) throws Exception {
        curr_conns.decrementAndGet();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
            throws Exception
    {
        if (e.getCause() instanceof ClientException) {
            logger.debug("client error", e.getCause().getMessage());

            ctx.getChannel().write("CLIENT_ERROR\r\n");
        } else {
            logger.error("error", e.getCause());

            ctx.getChannel().write("ERROR\r\n");
        }
    }


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
        if (cmd == Command.GET || cmd == Command.GETS) {
            MCElement[] results = get(command.keys.toArray(new String[command.keys.size()]));
            for (MCElement result : results) {
                if (result != null) {
                    channel.write(VALUE);
                    channel.write(result.keystring + " " + result.flags + " " + result.dataLength + (cmd == Command.GETS ? " " + result.cas_unique : "") + "\r\n");
                    channel.write(new String(result.data));
                    channel.write("\r\n");
                    channelHandlerContext.sendUpstream(messageEvent);
                }
            }
            channel.write("END\r\n");
        } else if (cmd == Command.SET) {
            String ret = set(command.element);
            if (!command.noreply)
                channel.write(ret);
        } else if (cmd == Command.CAS) {
            String ret = cas(command.cas_key, command.element);
            if (!command.noreply) channel.write(ret);
        } else if (cmd == Command.ADD) {
            String ret = add(command.element);
            if (!command.noreply) channel.write(ret);
        } else if (cmd == Command.REPLACE) {
            String ret = replace(command.element);
            if (!command.noreply) channel.write(ret);
        } else if (cmd == Command.APPEND) {
            String ret = append(command.element);
            if (!command.noreply) channel.write(ret);
        } else if (cmd == Command.PREPEND) {
            String ret = prepend(command.element);
            if (!command.noreply) channel.write(ret);
        } else if (cmd == Command.INCR) {
            String ret = get_add(command.keys.get(0), parseInt(command.keys.get(1)));
            if (!command.noreply) channel.write(ret);
        } else if (cmd == Command.DECR) {
            String ret = get_add(command.keys.get(0), -1 * parseInt(command.keys.get(1)));
            if (!command.noreply) channel.write(ret);
        } else if (cmd == Command.DELETE) {
            String ret = delete(command.keys.get(0), command.time);
            if (!command.noreply) channel.write(ret);
        } else if (cmd == Command.STATS) {
            String option = "";
            if (cmdKeysSize > 0) {
                option = command.keys.get(0);
            }
            channel.write(stat(option));
        } else if (cmd == Command.VERSION) {
            channel.write("VERSION " + version + "\r\n");
        } else if (cmd == Command.QUIT) {
            channel.close();
        } else if (cmd == Command.FLUSH_ALL) {

            String ret = flush_all(command.time);
            if (!command.noreply)
                channel.write(ret);
        } else {
            channel.write("ERROR\r\n");
            logger.error("error; unrecognized command: " + cmd);
        }

    }


    /**
     * Handle the deletion of an item from the cache.
     *
     * @param key  the key for the item
     * @param time only delete the element if time (time in seconds)
     * @return the message response
     */
    private  String delete(String key, int time) {
        return getDeleteResponseString(cache.delete(key, time));
    }

    private String getDeleteResponseString(Cache.DeleteResponse deleteResponse) {
        if (deleteResponse == Cache.DeleteResponse.DELETED) return "DELETED\r\n";
        else return "NOT_FOUND\r\n";
    }

    /**
     * Add an element to the cache
     *
     * @param e the element to add
     * @return the message response string
     */
    private String add(MCElement e) {
        return getStoreResponseString(cache.add(e));
    }

    /**
     * Find the string response message which is equivalent to a response to a set/add/replace message
     * in the cache
     *
     * @param storeResponse the response code
     * @return the string to output on the network
     */
    private  String getStoreResponseString(Cache.StoreResponse storeResponse) {
        switch (storeResponse) {
            case EXISTS:
                return "EXISTS\r\n";
            case NOT_FOUND:
                return "NOT_FOUND\r\n";
            case NOT_STORED:
                return "NOT_STORED\r\n";
            case STORED:
                return "STORED\r\n";
        }
        throw new RuntimeException("unknown store response from cache: " + storeResponse);
    }

    /**
     * Replace an element in the cache
     *
     * @param e the element to replace
     * @return the message response string
     */
    private  String replace(MCElement e) {
        return getStoreResponseString(cache.replace(e));
    }

    /**
     * Append bytes to an element in the cache
     * @param element the element to append to
     * @return the message response string
     */
    private  String append(MCElement element) {
        return getStoreResponseString(cache.append(element));
    }

    /**
     * Prepend bytes to an element in the cache
     * @param element the element to append to
     * @return the message response string
     */
    private  String prepend(MCElement element) {
        return getStoreResponseString(cache.prepend(element));
    }

    /**
     * Set an element in the cache
     *
     * @param e the element to set
     * @return the message response string
     */
    private  String set(MCElement e) {
        return getStoreResponseString(cache.set(e));
    }

    /**
     * Check and set an element in the cache
     *
     * @param cas_key the unique cas id for the element, to match against
     * @param e       the element to set @return the message response string
     * @return the message response string
     */
    private  String cas(Long cas_key, MCElement e) {
        return getStoreResponseString(cache.cas(cas_key, e));
    }

    /**
     * Increment an (integer) element in the cache
     *
     * @param key the key to increment
     * @param mod the amount to add to the value
     * @return the message response
     */
    private  String get_add(String key, int mod) {
        Integer ret = cache.get_add(key, mod);
        if (ret == null)
            return "NOT_FOUND\r\n";
        else
            return valueOf(ret)  + "\r\n";
    }

    /**
     * Get an element from the cache
     *
     * @param key the key for the element to lookup
     * @return the element, or 'null' in case of cache miss.
     */
    private  MCElement get(String key) {
        return cache.get(key)[0];
    }

    /**
     * Get an element from the cache
     *
     * @param keys the key for the element to lookup
     * @return the element, or 'null' in case of cache miss.
     */
    private MCElement[] get(String ... keys) {
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
    private  String stat(String arg) {

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

    /**
     * Flush all cache entries
     *
     * @return command response
     */
    private  boolean flush_all() {
        return cache.flush_all();
    }

    /**
     * Flush all cache entries with a timestamp after a given expiration time
     *
     * @param expire the flush time in seconds
     * @return command response
     */
    private  String flush_all(int expire) {
        return cache.flush_all(expire) ? "OK\r\n" : "ERROR\r\n";
    }


}