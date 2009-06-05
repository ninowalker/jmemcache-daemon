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

import static com.thimbleware.jmemcached.protocol.SessionStatus.State.*;
import com.thimbleware.jmemcached.MCElement;

import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.core.buffer.IoBuffer;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

/**
 * MINA MessageDecoderAdapter responsible for parsing inbound lines from the memcached protocol session.
 */
public final class CommandDecoder extends CumulativeProtocolDecoder {

    public static CharsetDecoder DECODER  = Charset.forName("US-ASCII").newDecoder();

    private static final String SESSION_STATUS = "sessionStatus";
    private static final String NOREPLY = "noreply";

    protected boolean doDecode(IoSession session, IoBuffer in, ProtocolDecoderOutput out)
            throws Exception {

        SessionStatus returnedSessionStatus;

        SessionStatus sessionStatus = (SessionStatus) session.getAttribute(SESSION_STATUS);

        if (sessionStatus != null && sessionStatus.state == WAITING_FOR_DATA) {
            if (in.remaining() < sessionStatus.bytesNeeded + 2) {
                return false;
            }
            // get the bytes we want, and that's it
            byte[] buffer = new byte[sessionStatus.bytesNeeded];
            in.get(buffer);

            // eat crlf at end
            String crlf = in.getString(2, DECODER);
            if (crlf.equals("\r\n")) {
                returnedSessionStatus = continueSet(session, out, sessionStatus, buffer);
                session.setAttribute(SESSION_STATUS, SessionStatus.ready());

                return true;
            } else {
                session.setAttribute(SESSION_STATUS, SessionStatus.ready());
                return true;
            }
        }


        // Remember the initial position.
        int start = in.position();

        List<String> words = new ArrayList<String>(8);

        // Now find the first CRLF in the buffer.
        byte previous = 0;
        while (in.hasRemaining()) {
            byte current = in.get();

            if (previous == '\r' && current == '\n') {
                // Remember the current position and limit.
                int position = in.position();
                int limit = in.limit();
                try {
                    in.position(start);
                    in.limit(position);
                    // The bytes between in.position() and in.limit()
                    // now contain a full CRLF terminated line.
                    words = collectCommand(in.slice());

                    if (words != null) {
                        // build the segmented parts into an array list and pass it on for processing
                        final int numParts = words.size();

                        returnedSessionStatus = processLine(words, out, numParts);

                        if (returnedSessionStatus.state != ERROR) {
                            session.setAttribute(SESSION_STATUS, returnedSessionStatus);
                            return true;
                        } else {
                            out.write(CommandMessage.clientError("bad data chunk"));
                            
                            return true;
                        }
                    }
                } finally {
                    // Set the position to point right after the
                    // detected line and set the limit to the old
                    // one.
                    in.position(position);
                    in.limit(limit);
                }
                // Decoded one line; CumulativeProtocolDecoder will
                // call me again until I return false. So just
                // return true until there are no more lines in the
                // buffer.
                return true;
            }

            previous = current;
        }

        // Could not find CRLF in the buffer. Reset the initial
        // position to the one we recorded above.
        in.position(start);

        return false;
    }


    // collect the parts of the stream for the command
    private List<String> collectCommand(IoBuffer in) {
        // get the bytes we want, and that's it
        byte[] buffer = new byte[in.remaining() - 2];
        in.get(buffer);

        return Arrays.asList(new String(buffer).split(" "));
    }


    /**
     * Process an individual complete protocol line and either passes the command for processing by the
     * session handler, or (in the case of SET-type commands) partially parses the command and sets the session into
     * a state to wait for additional data.
     * @param parts the (originally space separated) parts of the command
     * @param out the MINA protocol decoder output to pass our command on to
     * @param numParts number of arguments
     * @return the session status we want to set the session to
     */
    private SessionStatus processLine(List<String> parts, ProtocolDecoderOutput out, int numParts) {
        final String cmdType = parts.get(0).toUpperCase().intern();
        CommandMessage cmd = CommandMessage.command(cmdType);

        if (cmdType == Commands.ADD ||
                cmdType == Commands.SET ||
                cmdType == Commands.REPLACE ||
                cmdType == Commands.CAS ||
                cmdType == Commands.APPEND ||
                cmdType == Commands.PREPEND) {

            // if we don't have all the parts, it's malformed
            if (numParts < 5) {
                out.write(CommandMessage.error(""));

                return SessionStatus.ready();
            }

            int size = Integer.parseInt(parts.get(4));

            int expire = Integer.parseInt(parts.get(3));

            int flags = Integer.parseInt(parts.get(2));

            cmd.element = new MCElement(parts.get(1), flags, expire != 0 && expire < MCElement.THIRTY_DAYS ? MCElement.Now() + expire : expire, size);

            // look for cas and "noreply" elements
            if (numParts > 5) {
                int noreply = cmdType == Commands.CAS ? 6 : 5;
                if (cmdType == Commands.CAS) {
                    cmd.cas_key = Long.valueOf(parts.get(5));
                }

                if (numParts == noreply + 1 && parts.get(noreply).equalsIgnoreCase(NOREPLY))
                    cmd.noreply = true;

            }

            return SessionStatus.needMoreForCommand(size, cmd);

        } else if (cmdType == Commands.GET ||
                cmdType == Commands.GETS ||
                cmdType == Commands.STATS ||
                cmdType == Commands.QUIT ||
                cmdType == Commands.VERSION) {
            // CMD <options>*
            cmd.keys = parts.subList(1, numParts);

            out.write(cmd);
        } else if (cmdType == Commands.INCR ||
                cmdType == Commands.DECR) {

            if (numParts < 2 || numParts > 3)
                return SessionStatus.error();

            cmd.keys.add(parts.get(1));
            cmd.keys.add(parts.get(2));
            if (numParts == 3 && parts.get(2).equalsIgnoreCase(NOREPLY)) {
                cmd.noreply = true;
            }

            out.write(cmd);
        } else if (cmdType == Commands.DELETE) {
            cmd.keys.add(parts.get(1));

            if (numParts >= 2) {
                if (parts.get(numParts - 1).equalsIgnoreCase(NOREPLY)) {
                    cmd.noreply = true;
                    if (numParts == 4)
                        cmd.time = Integer.valueOf(parts.get(2));
                } else if (numParts == 3)
                    cmd.time = Integer.valueOf(parts.get(2));
            }

            out.write(cmd);
        } else if (cmdType == Commands.FLUSH_ALL) {
            if (numParts >= 1) {
                if (parts.get(numParts - 1).equalsIgnoreCase(NOREPLY)) {
                    cmd.noreply = true;
                    if (numParts == 3)
                        cmd.time = Integer.valueOf(parts.get(1));
                } else if (numParts == 2)
                    cmd.time = Integer.valueOf(parts.get(1));
            }
            out.write(cmd);
        } else {
            return SessionStatus.error();
        }


        return SessionStatus.ready();

    }

    /**
     * Handles the continuation of a SET/ADD/REPLACE command with the data it was waiting for.
     *
     * @param session the MINA IoSession
     * @param out the MINA protocol decoder output which we signal with the completed command
     * @param state the current session status (unused)
     * @param remainder the bytes picked up
     * @return the new status to set the session to
     */
    private SessionStatus continueSet(IoSession session, ProtocolDecoderOutput out, SessionStatus state, byte[] remainder) {
        state.cmd.element.data = remainder;

        out.write(state.cmd);

        return SessionStatus.ready();
    }

}
