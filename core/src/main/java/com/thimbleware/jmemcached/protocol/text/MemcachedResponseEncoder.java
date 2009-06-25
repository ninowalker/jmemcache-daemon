package com.thimbleware.jmemcached.protocol.text;

import com.thimbleware.jmemcached.Cache;
import com.thimbleware.jmemcached.MCElement;
import com.thimbleware.jmemcached.protocol.Command;
import com.thimbleware.jmemcached.protocol.ResponseMessage;
import com.thimbleware.jmemcached.protocol.exceptions.ClientException;
import org.jboss.netty.channel.*;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.valueOf;
import java.util.Set;
import java.util.Map;

/**
 * Response encoder for the memcached text protocol. Produces strings destined for the StringEncoder
 */
@ChannelPipelineCoverage("all")
public class MemcachedResponseEncoder extends SimpleChannelUpstreamHandler {

    final Logger logger = LoggerFactory.getLogger(MemcachedResponseEncoder.class);

    public static final String VALUE = "VALUE ";

    /**
     * Handle exceptions in protocol processing. Exceptions are either client or internal errors.  Report accordingly.
     *
     * @param ctx
     * @param e
     * @throws Exception
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        try {
            throw e.getCause();
        } catch (ClientException ce) {
            if (ctx.getChannel().isOpen())
                ctx.getChannel().write("CLIENT_ERROR\r\n");
        } catch (Throwable tr) {
            logger.error("error", tr);
            if (ctx.getChannel().isOpen())
                ctx.getChannel().write("ERROR\r\n");
        }
    }

    @Override
    public void messageReceived(ChannelHandlerContext channelHandlerContext, MessageEvent messageEvent) throws Exception {
        ResponseMessage command = (ResponseMessage) messageEvent.getMessage();

        Command cmd = command.cmd.cmd;

        Channel channel = messageEvent.getChannel();
        if (cmd == Command.GET || cmd == Command.GETS) {
            MCElement[] results = command.elements;
            for (MCElement result : results) {
                if (result != null) {
                    writeString(channel, VALUE);
                    writeString(channel, result.keystring + " " + result.flags + " " + result.dataLength + (cmd == Command.GETS ? " " + result.cas_unique : "") + "\r\n");
                    ChannelBuffer outputbuffer = ChannelBuffers.buffer(result.dataLength);
                    outputbuffer.writeBytes(result.data);
                    channel.write(outputbuffer);
                    writeString(channel, "\r\n");

                    // send response immediately
                    channelHandlerContext.sendUpstream(messageEvent);
                }
            }
            writeString(channel, "END\r\n");
        } else if (cmd == Command.SET || cmd == Command.CAS || cmd == Command.ADD || cmd == Command.REPLACE || cmd == Command.APPEND  || cmd == Command.PREPEND) {
            String ret = storeResponseString(command.response);
            if (!command.cmd.noreply)
                writeString(channel, ret);
        } else if (cmd == Command.INCR || cmd == Command.DECR) {
            String ret = incrDecrResponseString(command.incrDecrResponse);
            if (!command.cmd.noreply) writeString(channel, ret);
        } else if (cmd == Command.DELETE) {
            String ret = deleteResponseString(command.deleteResponse);
            if (!command.cmd.noreply) writeString(channel, ret);
        } else if (cmd == Command.STATS) {
            for (Map.Entry<String, Set<String>> stat : command.stats.entrySet()) {
                for (String statVal : stat.getValue()) {
                    writeString(channel, "STAT " + stat.getKey() + " " + statVal + "\r\n");
                }
            }
            writeString(channel, "END\r\n");
        } else if (cmd == Command.VERSION) {
            writeString(channel, "VERSION " + command.version + "\r\n");
        } else if (cmd == Command.QUIT) {
            channel.disconnect();
        } else if (cmd == Command.FLUSH_ALL) {
            if (!command.cmd.noreply) {
                String ret = command.flushSuccess ? "OK\r\n" : "ERROR\r\n";

                writeString(channel, ret);
            }
        } else {
            writeString(channel, "ERROR\r\n");
            logger.error("error; unrecognized command: " + cmd);
        }
        
    }

    private String deleteResponseString(Cache.DeleteResponse deleteResponse) {
        if (deleteResponse == Cache.DeleteResponse.DELETED) return "DELETED\r\n";
        else return "NOT_FOUND\r\n";
    }


    private String incrDecrResponseString(Integer ret) {
        if (ret == null)
            return "NOT_FOUND\r\n";
        else
            return valueOf(ret) + "\r\n";
    }

    /**
     * Find the string response message which is equivalent to a response to a set/add/replace message
     * in the cache
     *
     * @param storeResponse the response code
     * @return the string to output on the network
     */
    private String storeResponseString(Cache.StoreResponse storeResponse) {
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

    private void writeString(Channel out, String str) {
        ChannelBuffer outbuf = ChannelBuffers.buffer(str.length());
        outbuf.writeBytes(str.getBytes());
        out.write(outbuf);
    }
}
