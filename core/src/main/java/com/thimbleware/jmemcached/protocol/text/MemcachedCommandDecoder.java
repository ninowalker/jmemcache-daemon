package com.thimbleware.jmemcached.protocol.text;

import com.thimbleware.jmemcached.MCElement;
import com.thimbleware.jmemcached.protocol.Command;
import com.thimbleware.jmemcached.protocol.CommandMessage;
import com.thimbleware.jmemcached.protocol.SessionStatus;
import com.thimbleware.jmemcached.protocol.exceptions.InvalidProtocolStateException;
import com.thimbleware.jmemcached.protocol.exceptions.MalformedCommandException;
import com.thimbleware.jmemcached.protocol.exceptions.UnknownCommandException;
import org.jboss.netty.channel.*;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * The MemcachedCommandDecoder is responsible for taking lines from the MemcachedFrameDecoder and parsing them
 * into CommandMessage instances for handling by the MemcachedCommandHandler
 * <p/>
 * Protocol status is held in the SessionStatus instance which is shared between each of the decoders in the pipeline.
 */
@ChannelPipelineCoverage("one")
public class MemcachedCommandDecoder extends SimpleChannelUpstreamHandler {

    final Logger logger = LoggerFactory.getLogger(MemcachedCommandDecoder.class);

    private SessionStatus status;

    private static final String NOREPLY = "noreply";

    public MemcachedCommandDecoder(SessionStatus status) {
        this.status = status;
    }                                     

    /**
     * Process an inbound string from the pipeline's downstream, and depending on the state (waiting for data or
     * processing commands), turn them into the correct type of command.
     *
     * @param channelHandlerContext
     * @param messageEvent
     * @throws Exception
     */
    @Override
    public void messageReceived(ChannelHandlerContext channelHandlerContext, MessageEvent messageEvent) throws Exception {
        ChannelBuffer in = (ChannelBuffer) messageEvent.getMessage();

        try {
            // Because of the frame handler, we are assured that we are receiving only complete lines or payloads.
            // Verify that we are in 'processing()' mode
            if (status.state == SessionStatus.State.PROCESSING) {
                // split into pieces
                String[] commandPieces = in.toString("US-ASCII").split(" ");

                processLine(commandPieces, messageEvent.getChannel(), channelHandlerContext);
            } else if (status.state == SessionStatus.State.PROCESSING_MULTILINE) {
                byte[] payload = new byte[in.capacity()];
                in.readBytes(payload);
                continueSet(messageEvent.getChannel(), status, payload, channelHandlerContext);
            } else {
                throw new InvalidProtocolStateException("invalid protocol state");
            }

        } finally {
            // Now indicate that we need more for this command by changing the session status's state.
            // This instructs the frame decoder to start collecting data for us.
            // Note, we don't do this if we're waiting for data.
            if (status.state != SessionStatus.State.WAITING_FOR_DATA) status.ready();
        }
    }

    /**
     * Process an individual complete protocol line and either passes the command for processing by the
     * session handler, or (in the case of SET-type commands) partially parses the command and sets the session into
     * a state to wait for additional data.
     *
     * @param parts                 the (originally space separated) parts of the command
     * @param channel
     * @param channelHandlerContext
     * @return the session status we want to set the session to
     */
    private void processLine(String[] parts, Channel channel, ChannelHandlerContext channelHandlerContext) throws UnknownCommandException, MalformedCommandException {
        final int numParts = parts.length;

        // Turn the command into an enum for matching on
        Command cmdType;
        try {
            cmdType = Command.valueOf(parts[0].toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new UnknownCommandException("unknown command: " + parts[0].toLowerCase());
        }

        // Produce the initial command message, for filling in later
        CommandMessage cmd = CommandMessage.command(cmdType);

        // TODO there is a certain amount of fudgery here related to common things like 'noreply', etc. that could be refactored nicely

        // Dispatch on the type of command
        if (cmdType == Command.ADD ||
                cmdType == Command.SET ||
                cmdType == Command.REPLACE ||
                cmdType == Command.CAS ||
                cmdType == Command.APPEND ||
                cmdType == Command.PREPEND) {

            // if we don't have all the parts, it's malformed
            if (numParts < 5) {
                throw new MalformedCommandException("invalid command length");
            }

            // Fill in all the elements of the command
            int size = Integer.parseInt(parts[4]);
            int expire = Integer.parseInt(parts[3]);
            int flags = Integer.parseInt(parts[2]);
            cmd.element = new MCElement(parts[1], flags, expire != 0 && expire < MCElement.THIRTY_DAYS ? MCElement.Now() + expire : expire, size);

            // look for cas and "noreply" elements
            if (numParts > 5) {
                int noreply = cmdType == Command.CAS ? 6 : 5;
                if (cmdType == Command.CAS) {
                    cmd.cas_key = Long.valueOf(parts[5]);
                }

                if (numParts == noreply + 1 && parts[noreply].equalsIgnoreCase(NOREPLY))
                    cmd.noreply = true;
            }

            // Now indicate that we need more for this command by changing the session status's state.
            // This instructs the frame decoder to start collecting data for us.
            status.needMore(size, cmd);
        } else if (cmdType == Command.GET ||
                cmdType == Command.GETS ||
                cmdType == Command.STATS ||
                cmdType == Command.QUIT ||
                cmdType == Command.VERSION) {

            // Get all the keys
            cmd.keys.addAll(Arrays.asList(parts).subList(1, numParts));

            // Pass it on.
            Channels.fireMessageReceived(channelHandlerContext, cmd, channel.getRemoteAddress());
        } else if (cmdType == Command.INCR ||
                cmdType == Command.DECR) {

            // Malformed
            if (numParts < 2 || numParts > 3)
                throw new MalformedCommandException("invalid increment command");

            cmd.keys.add(parts[1]);
            cmd.incrAmount = Integer.valueOf(parts[2]);
            
            if (numParts == 3 && parts[2].equalsIgnoreCase(NOREPLY)) {
                cmd.noreply = true;
            }

            Channels.fireMessageReceived(channelHandlerContext, cmd, channel.getRemoteAddress());
        } else if (cmdType == Command.DELETE) {
            cmd.keys.add(parts[1]);

            if (numParts >= 2) {
                if (parts[numParts - 1].equalsIgnoreCase(NOREPLY)) {
                    cmd.noreply = true;
                    if (numParts == 4)
                        cmd.time = Integer.valueOf(parts[2]);
                } else if (numParts == 3)
                    cmd.time = Integer.valueOf(parts[2]);
            }
            Channels.fireMessageReceived(channelHandlerContext, cmd, channel.getRemoteAddress());
        } else if (cmdType == Command.FLUSH_ALL) {
            if (numParts >= 1) {
                if (parts[numParts - 1].equalsIgnoreCase(NOREPLY)) {
                    cmd.noreply = true;
                    if (numParts == 3)
                        cmd.time = Integer.valueOf(parts[1]);
                } else if (numParts == 2)
                    cmd.time = Integer.valueOf(parts[1]);
            }
            Channels.fireMessageReceived(channelHandlerContext, cmd, channel.getRemoteAddress());
        } else {
            throw new UnknownCommandException("unknown command: " + cmdType);
        }

    }

    /**
     * Handles the continuation of a SET/ADD/REPLACE command with the data it was waiting for.
     *
     * @param state                 the current session status (unused)
     * @param remainder             the bytes picked up
     * @param channelHandlerContext
     * @return the new status to set the session to
     */
    private void continueSet(Channel channel, SessionStatus state, byte[] remainder, ChannelHandlerContext channelHandlerContext) {
        state.cmd.element.data = remainder;
        Channels.fireMessageReceived(channelHandlerContext, state.cmd, channelHandlerContext.getChannel().getRemoteAddress());
    }
}
