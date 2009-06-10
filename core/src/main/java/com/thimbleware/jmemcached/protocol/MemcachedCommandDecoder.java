package com.thimbleware.jmemcached.protocol;

import org.jboss.netty.channel.*;

import java.util.List;
import java.util.Arrays;

import com.thimbleware.jmemcached.MCElement;

/**
 */
@ChannelPipelineCoverage("one")
public class MemcachedCommandDecoder extends SimpleChannelUpstreamHandler {

    private SessionStatus status;

    private static final String NOREPLY = "noreply";

    public MemcachedCommandDecoder(SessionStatus status) {
        this.status = status;
    }

    @Override
    public void messageReceived(ChannelHandlerContext channelHandlerContext, MessageEvent messageEvent) throws Exception {
        String in = (String) messageEvent.getMessage();

        // Because of the frame handler, we are assured that we are receiving only complete lines or payloads.
        // Verify that we are in 'processing()' mode
        if (status.state == SessionStatus.State.PROCESSING) {
            // split into pieces
            String[] commandPieces = in.split(" ");

            processLine(commandPieces, messageEvent.getChannel(), channelHandlerContext);
        } else if (status.state == SessionStatus.State.PROCESSING_MULTILINE) {
            continueSet(messageEvent.getChannel(), status, in.getBytes(), channelHandlerContext);
        } else {
            status.ready();

            throw new RuntimeException("invalid protocol state");
        }


    }

    /**
     * Process an individual complete protocol line and either passes the command for processing by the
     * session handler, or (in the case of SET-type commands) partially parses the command and sets the session into
     * a state to wait for additional data.
     * @param parts the (originally space separated) parts of the command
     * @param channel
     * @param channelHandlerContext
     * @return the session status we want to set the session to
     */
    private void processLine(String[] parts, Channel channel, ChannelHandlerContext channelHandlerContext) {
        final int numParts = parts.length;
        final String cmdType = parts[0].toUpperCase().intern();
        CommandMessage cmd = new CommandMessage(cmdType);

        if (cmdType == Commands.ADD ||
                cmdType == Commands.SET ||
                cmdType == Commands.REPLACE ||
                cmdType == Commands.CAS ||
                cmdType == Commands.APPEND ||
                cmdType == Commands.PREPEND) {

            // if we don't have all the parts, it's malformed
            if (numParts < 5) {
                throw new RuntimeException("invalid command length");
            }

            int size = Integer.parseInt(parts[4]);
            int expire = Integer.parseInt(parts[3]);
            int flags = Integer.parseInt(parts[2]);
            cmd.element = new MCElement(parts[1], flags, expire != 0 && expire < MCElement.THIRTY_DAYS ? MCElement.Now() + expire : expire, size);

            // look for cas and "noreply" elements
            if (numParts > 5) {
                int noreply = cmdType == Commands.CAS ? 6 : 5;
                if (cmdType == Commands.CAS) {
                    cmd.cas_key = Long.valueOf(parts[5]);
                }

                if (numParts == noreply + 1 && parts[noreply].equalsIgnoreCase(NOREPLY))
                    cmd.noreply = true;
            }

            status.needMore(size, cmd);
        } else if (cmdType == Commands.GET ||
                cmdType == Commands.GETS ||
                cmdType == Commands.STATS ||
                cmdType == Commands.QUIT ||
                cmdType == Commands.VERSION) {

            // CMD <options>*
            cmd.keys.addAll(Arrays.asList(parts).subList(1, numParts));

            Channels.fireMessageReceived(channelHandlerContext, cmd, channel.getRemoteAddress());

            status.ready();
        } else if (cmdType == Commands.INCR ||
                cmdType == Commands.DECR) {

            if (numParts < 2 || numParts > 3)
                throw new RuntimeException("invalid increment command");

            cmd.keys.add(parts[1]);
            cmd.keys.add(parts[2]);
            if (numParts == 3 && parts[2].equalsIgnoreCase(NOREPLY)) {
                cmd.noreply = true;
            }

            Channels.fireMessageReceived(channelHandlerContext, cmd, channel.getRemoteAddress());

            status.ready();
        } else if (cmdType == Commands.DELETE) {
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

            status.ready();
        } else if (cmdType == Commands.FLUSH_ALL) {
            if (numParts >= 1) {
                if (parts[numParts - 1].equalsIgnoreCase(NOREPLY)) {
                    cmd.noreply = true;
                    if (numParts == 3)
                        cmd.time = Integer.valueOf(parts[1]);
                } else if (numParts == 2)
                    cmd.time = Integer.valueOf(parts[1]);
            }
            Channels.fireMessageReceived(channelHandlerContext, cmd, channel.getRemoteAddress());

            status.ready();
        } else {
            status.ready();
            throw new RuntimeException("unknown command: " + cmdType);
        }
    }

    /**
     * Handles the continuation of a SET/ADD/REPLACE command with the data it was waiting for.
     *
     * @param state the current session status (unused)
     * @param remainder the bytes picked up
     * @param channelHandlerContext
     * @return the new status to set the session to
     */
    private void continueSet(Channel channel, SessionStatus state, byte[] remainder, ChannelHandlerContext channelHandlerContext) {
        state.cmd.element.data = remainder;
        Channels.fireMessageReceived(channelHandlerContext, state.cmd, channelHandlerContext.getChannel().getRemoteAddress());
    }
}
