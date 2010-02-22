package com.thimbleware.jmemcached.protocol.text;

import com.thimbleware.jmemcached.Cache;
import com.thimbleware.jmemcached.CacheElement;
import com.thimbleware.jmemcached.protocol.Op;
import com.thimbleware.jmemcached.protocol.ResponseMessage;
import com.thimbleware.jmemcached.protocol.exceptions.ClientException;
import com.thimbleware.jmemcached.util.BufferUtils;
import org.jboss.netty.channel.*;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.thimbleware.jmemcached.protocol.text.MemcachedPipelineFactory.*;
import static java.lang.String.valueOf;

import java.util.Set;
import java.util.Map;

/**
 * Response encoder for the memcached text protocol. Produces strings destined for the StringEncoder
 */
public final class MemcachedResponseEncoder<CACHE_ELEMENT extends CacheElement> extends SimpleChannelUpstreamHandler {

    final Logger logger = LoggerFactory.getLogger(MemcachedResponseEncoder.class);


    public static final ChannelBuffer CRLF = ChannelBuffers.copiedBuffer("\r\n", USASCII);
    private static final ChannelBuffer VALUE = ChannelBuffers.copiedBuffer("VALUE ", USASCII);
    private static final ChannelBuffer EXISTS = ChannelBuffers.copiedBuffer("EXISTS\r\n", USASCII);
    private static final ChannelBuffer NOT_FOUND = ChannelBuffers.copiedBuffer("NOT_FOUND\r\n", USASCII);
    private static final ChannelBuffer NOT_STORED = ChannelBuffers.copiedBuffer("NOT_STORED\r\n", USASCII);
    private static final ChannelBuffer STORED = ChannelBuffers.copiedBuffer("STORED\r\n", USASCII);
    private static final ChannelBuffer DELETED = ChannelBuffers.copiedBuffer("DELETED\r\n", USASCII);
    private static final ChannelBuffer END = ChannelBuffers.copiedBuffer("END\r\n", USASCII);
    private static final ChannelBuffer OK = ChannelBuffers.copiedBuffer("OK\r\n", USASCII);
    private static final ChannelBuffer ERROR = ChannelBuffers.copiedBuffer("ERROR\r\n", USASCII);
    private static final ChannelBuffer CLIENT_ERROR = ChannelBuffers.copiedBuffer("CLIENT_ERROR\r\n", USASCII);

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
                ctx.getChannel().write(CLIENT_ERROR);
        } catch (Throwable tr) {
            logger.error("error", tr);
            if (ctx.getChannel().isOpen())
                ctx.getChannel().write(ERROR);
        }
    }



    @Override
    public void messageReceived(ChannelHandlerContext channelHandlerContext, MessageEvent messageEvent) throws Exception {
        ResponseMessage<CACHE_ELEMENT> command = (ResponseMessage<CACHE_ELEMENT>) messageEvent.getMessage();

        Op cmd = command.cmd.op;

        Channel channel = messageEvent.getChannel();

        if (cmd == Op.GET || cmd == Op.GETS) {
            CacheElement[] results = command.elements;
            int totalBytes = 0;
            for (CacheElement result : results) {
                if (result != null) {
                    totalBytes += result.size() + 512;
                }
            }
            ChannelBuffer writeBuffer = ChannelBuffers.dynamicBuffer(totalBytes);

            for (CacheElement result : results) {
                if (result != null) {
                    writeBuffer.writeBytes(VALUE.duplicate());
                    writeBuffer.writeBytes(result.getKey().bytes);
                    writeBuffer.writeByte((byte)' ');
                    writeBuffer.writeBytes(BufferUtils.itoa(result.getFlags()));
                    writeBuffer.writeByte((byte)' ');
                    writeBuffer.writeBytes(BufferUtils.itoa(result.getData().length));
                    if (cmd == Op.GETS) {
                        writeBuffer.writeByte((byte)' ');
                        writeBuffer.writeBytes(BufferUtils.ltoa(result.getCasUnique()));
                    }
                    writeBuffer.writeByte( (byte)'\r');
                    writeBuffer.writeByte( (byte)'\n');
                    writeBuffer.writeBytes(result.getData());
                    writeBuffer.writeByte( (byte)'\r');
                    writeBuffer.writeByte( (byte)'\n');
                }
            }
            writeBuffer.writeBytes(END.duplicate());

            Channels.write(channel, writeBuffer);
        } else if (cmd == Op.SET || cmd == Op.CAS || cmd == Op.ADD || cmd == Op.REPLACE || cmd == Op.APPEND  || cmd == Op.PREPEND) {

            if (!command.cmd.noreply)
                Channels.write(channel, storeResponse(command.response));
        } else if (cmd == Op.INCR || cmd == Op.DECR) {
            if (!command.cmd.noreply)
                Channels.write(channel, incrDecrResponseString(command.incrDecrResponse));

        } else if (cmd == Op.DELETE) {
            if (!command.cmd.noreply)
                Channels.write(channel, deleteResponseString(command.deleteResponse));

        } else if (cmd == Op.STATS) {
            for (Map.Entry<String, Set<String>> stat : command.stats.entrySet()) {
                for (String statVal : stat.getValue()) {
                    StringBuilder builder = new StringBuilder();
                    builder.append("STAT ");
                    builder.append(stat.getKey());
                    builder.append(" ");
                    builder.append(String.valueOf(statVal));
                    builder.append("\r\n");
                    Channels.write(channel, ChannelBuffers.copiedBuffer(builder.toString(), USASCII));
                }
            }
            Channels.write(channel, END.duplicate());

        } else if (cmd == Op.VERSION) {
            Channels.write(channel, ChannelBuffers.copiedBuffer("VERSION " + command.version + "\r\n", USASCII));
        } else if (cmd == Op.QUIT) {
            Channels.disconnect(channel);
        } else if (cmd == Op.FLUSH_ALL) {
            if (!command.cmd.noreply) {
                ChannelBuffer ret = command.flushSuccess ? OK.duplicate() : ERROR.duplicate();

                Channels.write(channel, ret);
            }
        } else {
            Channels.write(channel, ERROR.duplicate());
            logger.error("error; unrecognized command: " + cmd);
        }

    }

    private ChannelBuffer deleteResponseString(Cache.DeleteResponse deleteResponse) {
        if (deleteResponse == Cache.DeleteResponse.DELETED) return DELETED.duplicate();
        else return NOT_FOUND.duplicate();
    }


    private ChannelBuffer incrDecrResponseString(Integer ret) {
        if (ret == null)
            return NOT_FOUND.duplicate();
        else
            return ChannelBuffers.copiedBuffer(valueOf(ret) + "\r\n", USASCII);
    }

    /**
     * Find the string response message which is equivalent to a response to a set/add/replace message
     * in the cache
     *
     * @param storeResponse the response code
     * @return the string to output on the network
     */
    private ChannelBuffer storeResponse(Cache.StoreResponse storeResponse) {
        switch (storeResponse) {
            case EXISTS:
                return EXISTS.duplicate();
            case NOT_FOUND:
                return NOT_FOUND.duplicate();
            case NOT_STORED:
                return NOT_STORED.duplicate();
            case STORED:
                return STORED.duplicate();
        }
        throw new RuntimeException("unknown store response from cache: " + storeResponse);
    }
}
