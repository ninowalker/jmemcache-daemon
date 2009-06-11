package com.thimbleware.jmemcached.protocol.binary;

import com.thimbleware.jmemcached.protocol.Command;
import com.thimbleware.jmemcached.protocol.ResponseMessage;
import com.thimbleware.jmemcached.MCElement;
import com.thimbleware.jmemcached.Cache;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;

import java.nio.ByteOrder;

/**
 */
@ChannelPipelineCoverage("all")
public class MemcachedBinaryResponseEncoder extends SimpleChannelUpstreamHandler {

    public static enum ResponseCode {
        /**
         *       <t hangText="0x0000">No error</t>
         <t hangText="0x0001">Key not found</t>
         <t hangText="0x0002">Key exists</t>
         <t hangText="0x0003">Value too large</t>
         <t hangText="0x0004">Invalid arguments</t>
         <t hangText="0x0005">Item not stored</t>
         <t hangText="0x0081">Unknown command</t>
         <t hangText="0x0082">Out of memory</t>
         */
        OK(0x0000),
        KEYNF(0x0001),
        KEYEXISTS(0x0002),
        TOOLARGE(0x0003),
        INVARG(0x0004),
        NOT_STORED(0x0005),
        UNKNOWN(0x0081),
        OOM(0x00082);

        public short code;

        ResponseCode(int code) {
            this.code = (short)code;
        }
    }

    public ResponseCode getStatusCode(ResponseMessage command) {
        Command cmd = command.cmd.cmd;
        if (cmd == Command.GET || cmd == Command.GETS) {
            return ResponseCode.OK;
        } else if (cmd == Command.SET || cmd == Command.CAS || cmd == Command.ADD || cmd == Command.REPLACE || cmd == Command.APPEND  || cmd == Command.PREPEND) {
            switch (command.response) {

                case EXISTS:
                    return ResponseCode.KEYEXISTS;
                case NOT_FOUND:
                    return ResponseCode.KEYNF;
                case NOT_STORED:
                    return ResponseCode.NOT_STORED;
                case STORED:
                    return ResponseCode.OK;
            }
        } else if (cmd == Command.INCR || cmd == Command.DECR) {
            return command.incrDecrResponse == null ? ResponseCode.KEYNF : ResponseCode.OK;
        } else if (cmd == Command.DELETE) {
            switch (command.deleteResponse) {
                case DELETED:
                    return ResponseCode.OK;
                case NOT_FOUND:
                    return ResponseCode.KEYNF;
            }
        } else if (cmd == Command.STATS) {
            return ResponseCode.OK;
        } else if (cmd == Command.VERSION) {
            return ResponseCode.OK;
        } else if (cmd == Command.FLUSH_ALL) {
            return ResponseCode.OK;
        }
        return ResponseCode.UNKNOWN;
    }

    @Override
    public void messageReceived(ChannelHandlerContext channelHandlerContext, MessageEvent messageEvent) throws Exception {
        ResponseMessage command = (ResponseMessage) messageEvent.getMessage();
        MemcachedBinaryCommandDecoder.BinaryCommand bcmd = MemcachedBinaryCommandDecoder.BinaryCommand.forCommandMessage(command.cmd);

        // take the ResponseMessage and turn it into a binary payload.
        ChannelBuffer header = ChannelBuffers.buffer(ByteOrder.BIG_ENDIAN, 24);

        header.writeByte((byte)0x81);  // magic
        header.writeByte(bcmd.code); // opcode
        short keyLength = (short) (bcmd.addKeyToResponse && command.cmd != null  && command.cmd.keys.size() != 0 ? command.cmd.keys.get(0).length() : 0); // keylength
        header.writeShort(keyLength);
        header.writeByte((byte)4); // extra length = flags + expiry
        header.writeByte((byte)0); // data type unused
        header.writeShort(getStatusCode(command).code); // status code

        boolean hasData = bcmd.correspondingCommand == Command.GET || bcmd.correspondingCommand == Command.GETS;
        int dataLength = hasData && command.elements != null ? command.elements[0].dataLength : 0;
        header.writeInt(dataLength + keyLength + 4); // data length
        header.writeInt(command.cmd.opaque); // opaque
        header.writeLong(command.cmd.cas_key);

        // write the header
        messageEvent.getChannel().write(header);

        // write extras == flags & expiry
        ChannelBuffer extrasBuffer = ChannelBuffers.buffer(ByteOrder.BIG_ENDIAN, 4);

        extrasBuffer.writeShort((short) (command.cmd.element != null ? command.cmd.element.flags : 0));
        extrasBuffer.writeShort((short) (command.cmd.element != null ? command.cmd.element.expire : 0));
        messageEvent.getChannel().write(extrasBuffer);


        // write key if there is one
        if (bcmd.addKeyToResponse && command.cmd.keys != null && command.cmd.keys.size() != 0) {
            ChannelBuffer keyBuffer = ChannelBuffers.buffer(ByteOrder.BIG_ENDIAN, command.cmd.keys.get(0).length());
            keyBuffer.writeBytes(command.cmd.keys.get(0).getBytes());
            messageEvent.getChannel().write(keyBuffer);
        }

        // write value if there is one
        if (command.elements != null && (command.cmd.cmd == Command.GET || command.cmd.cmd == Command.GETS)) {
            ChannelBuffer valueBuffer = ChannelBuffers.buffer(ByteOrder.BIG_ENDIAN, command.elements[0].dataLength);
            valueBuffer.writeBytes(command.elements[0].data);
            messageEvent.getChannel().write(valueBuffer);
        }
    }
}
