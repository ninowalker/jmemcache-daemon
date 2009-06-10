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
package com.thimbleware.jmemcached;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import com.thimbleware.jmemcached.protocol.MemcachedPipelineFactory;

/**
 * The actual daemon - responsible for the binding and configuration of the network configuration.
 */
public class MemCacheDaemon {

    final Logger logger = LoggerFactory.getLogger(MemCacheDaemon.class);

    public static String memcachedVersion = "0.3";

    private int receiveBufferSize = 32768;
    private int sendBufferSize = 32768;

    private boolean verbose;
    private int idleTime;
    private InetSocketAddress addr;
    private Cache cache;

    private boolean running = false;
    private Channel serverChannel;

    public MemCacheDaemon() {
    }

    public MemCacheDaemon(Cache cache) {
        this.cache = cache;
    }

    /**
     * Bind the network connection and start the network processing threads.
     *
     * @throws IOException
     */
    public void start() throws IOException {
        ChannelFactory factory =
            new NioServerSocketChannelFactory(
                    Executors.newCachedThreadPool(),
                    Executors.newCachedThreadPool());

        ServerBootstrap bootstrap = new ServerBootstrap(factory);

        bootstrap.setPipelineFactory(new MemcachedPipelineFactory(cache, memcachedVersion, verbose, idleTime, receiveBufferSize));

        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);

        serverChannel = bootstrap.bind(addr);

        logger.info("Listening on " + String.valueOf(addr.getHostName()) + ":" + addr.getPort());
        running = true;
    }

    public void stop() {
        serverChannel.close().awaitUninterruptibly();
    }
    
    public static void setMemcachedVersion(String memcachedVersion) {
        MemCacheDaemon.memcachedVersion = memcachedVersion;
    }

    public void setReceiveBufferSize(int receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
    }

    public void setSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public void setIdleTime(int idleTime) {
        this.idleTime = idleTime;
    }

    public void setAddr(InetSocketAddress addr) {
        this.addr = addr;
    }


    public Cache getCache() {
        return cache;
    }

    public void setCache(Cache cache) {
        this.cache = cache;
    }

    public boolean isRunning() {
        return running;
    }
}
