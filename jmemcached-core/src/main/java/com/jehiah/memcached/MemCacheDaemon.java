package com.jehiah.memcached;

import org.apache.mina.common.ExecutorThreadModel;
import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.transport.socket.nio.SocketAcceptor;
import org.apache.mina.transport.socket.nio.SocketAcceptorConfig;
import org.apache.mina.transport.socket.nio.SocketSessionConfig;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 * The actual daemon - responsible for the binding and configuration of the network configuration.
 */
public class MemCacheDaemon {

    final Logger logger = LoggerFactory.getLogger(MemCacheDaemon.class);

    public static String memcachedVersion = "0.2-SNAPSHOT";

    private int receiveBufferSize = 1024000;
    private int sendBufferSize = 1024000;

    private boolean verbose;
    private int idleTime;
    private InetSocketAddress addr;
    private int port;
    private LRUCacheDelegate cacheDelegate;

    public MemCacheDaemon() {
    }

    /**
     * Bind the network connection and start the network processing threads.
     *
     * @throws IOException
     */
    public void start() throws IOException {
        SocketAcceptor acceptor = new SocketAcceptor(16, Executors.newCachedThreadPool() );
        SocketAcceptorConfig defaultConfig = acceptor.getDefaultConfig();
        SocketSessionConfig sessionConfig = defaultConfig.getSessionConfig();
        sessionConfig.setSendBufferSize(sendBufferSize);
        sessionConfig.setReceiveBufferSize(receiveBufferSize);
        sessionConfig.setTcpNoDelay(true);
        defaultConfig.setThreadModel(ExecutorThreadModel.getInstance("jmemcached"));

        acceptor.bind(this.addr, new ServerSessionHandler(cacheDelegate, memcachedVersion, verbose, idleTime));

        ProtocolCodecFactory codec = new MemcachedProtocolCodecFactory();
        acceptor.getFilterChain().addFirst("protocolFilter", new ProtocolCodecFilter(codec));

        logger.info("Listening on " + String.valueOf(addr.getHostName()) + ":" + this.port);
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

    public void setPort(int port) {
        this.port = port;
    }

    public LRUCacheDelegate getCacheDelegate() {
        return cacheDelegate;
    }

    public void setCacheDelegate(LRUCacheDelegate cacheDelegate) {
        this.cacheDelegate = cacheDelegate;
    }


}
