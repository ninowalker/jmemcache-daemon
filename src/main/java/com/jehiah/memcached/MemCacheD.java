package com.jehiah.memcached;

import org.apache.mina.common.ExecutorThreadModel;
import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.transport.socket.nio.SocketAcceptor;
import org.apache.mina.transport.socket.nio.SocketAcceptorConfig;
import org.apache.mina.transport.socket.nio.SocketSessionConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 */
public class MemCacheD {

    public static String memcachedVersion = "0.2-SNAPSHOT";

    private int receiveBufferSize = 1024000;
    private int sendBufferSize = 1024000;
    private int ceilingSize = 1024000;
    private int maxCacheSize;
    private long maxCacheBytes;
    private boolean verbose;
    private int idleTime;
    private InetSocketAddress addr;
    private int port;

    public MemCacheD() {
    }

    public MemCacheD(InetSocketAddress addr, int port, int maxCacheSize, long maxCacheBytes, int idleTime, boolean verbose) {
        this.addr = addr;
        this.port = port;
        this.maxCacheSize = maxCacheSize;
        this.maxCacheBytes = maxCacheBytes;
        this.idleTime = idleTime;
        this.verbose = verbose;

    }
    public void start() throws IOException {
        SocketAcceptor acceptor = new SocketAcceptor(16, Executors.newCachedThreadPool() );
        SocketAcceptorConfig defaultConfig = acceptor.getDefaultConfig();
        SocketSessionConfig sessionConfig = defaultConfig.getSessionConfig();
        sessionConfig.setSendBufferSize(sendBufferSize);
        sessionConfig.setReceiveBufferSize(receiveBufferSize);
        sessionConfig.setTcpNoDelay(true);
        defaultConfig.setThreadModel(ExecutorThreadModel.getInstance("jmemcached"));

        acceptor.bind(this.addr, new ServerSessionHandler(new MCCache(maxCacheSize, maxCacheBytes, ceilingSize), memcachedVersion, this.verbose, this.idleTime));

        ProtocolCodecFactory codec = new MemcachedProtocolCodecFactory();
        acceptor.getFilterChain().addFirst("protocolFilter", new ProtocolCodecFilter(codec));

        System.err.println("Listening on " + String.valueOf(addr.getHostName()) + ":" + this.port);
    }

    public static void setMemcachedVersion(String memcachedVersion) {
        MemCacheD.memcachedVersion = memcachedVersion;
    }

    public void setReceiveBufferSize(int receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
    }

    public void setSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
    }

    public void setCeilingSize(int ceilingSize) {
        this.ceilingSize = ceilingSize;
    }

    public void setMaxCacheSize(int maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
    }

    public void setMaxCacheBytes(long maxCacheBytes) {
        this.maxCacheBytes = maxCacheBytes;
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
}
