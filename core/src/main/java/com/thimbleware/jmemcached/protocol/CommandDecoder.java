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

import static com.thimbleware.jmemcached.protocol.CommandDecoder.SessionState.*;
import com.thimbleware.jmemcached.MCElement;

import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.apache.mina.filter.codec.demux.MessageDecoderAdapter;
import org.apache.mina.filter.codec.demux.MessageDecoderResult;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.core.buffer.IoBuffer;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.List;

/**
 * MINA MessageDecoderAdapter responsible for parsing inbound lines from the memcached protocol session.
 */
public final class CommandDecoder extends MessageDecoderAdapter {

    public static CharsetDecoder DECODER  = Charset.forName("US-ASCII").newDecoder();

    private static final int WORD_BUFFER_INIT_SIZE = 8;

    private static final String SESSION_STATUS = "sessionStatus";
    private ArrayList<StringBuilder> words;
    private static final String CRLF = "\r\n";
    private static final String NOREPLY = "noreply";

    /**
     * Possible states that the current session is in.
     */
    enum SessionState {
        ERROR,
        WAITING_FOR_DATA,
        READY
    }

    /**
     * Object for holding the current session status.
     */
    final class SessionStatus implements Serializable {
        // the state the session is in
        public final SessionState state;

        // if we are waiting for more data, how much?
        public int bytesNeeded;

        // the current working command
        public CommandMessage cmd;

        SessionStatus(SessionState state) {
            this.state = state;
        }

        SessionStatus(SessionState state, int bytesNeeded, CommandMessage cmd) {
            this.state = state;
            this.bytesNeeded = bytesNeeded;
            this.cmd = cmd;
        }
    }

    public CommandDecoder() {
        words = new ArrayList<StringBuilder>(8);
    }

    
    /**
     * Checks the specified buffer is decodable by this decoder.
     *
     * In our case checks the session state to see if we are waiting for data.  If we are, make sure
     * that we actually have all the data we need.
     *
     * @return {@link #OK} if this decoder can decode the specified buffer.
     *         {@link #NOT_OK} if this decoder cannot decode the specified buffer.
     *         {@link #NEED_DATA} if more data is required to determine if the
     *         specified buffer is decodable ({@link #OK}) or not decodable
     *         {@link #NOT_OK}.
     */
    public final MessageDecoderResult decodable(IoSession session, IoBuffer in) {
        // ask the session for its state,
        SessionStatus sessionStatus = (SessionStatus) session.getAttribute(SESSION_STATUS);
        if (sessionStatus != null &&  sessionStatus.state == WAITING_FOR_DATA) {
            if (in.remaining() < sessionStatus.bytesNeeded + 2)
                return MessageDecoderResult.NEED_DATA;
        }
        return MessageDecoderResult.OK;
    }

    /**
     * Actually decodes inbound data from the memcached protocol session.
     *
     * MINA invokes {@link #decode(IoSession, IoBuffer, ProtocolDecoderOutput)}
     * method with read data, and then the decoder implementation puts decoded
     * messages into {@link ProtocolDecoderOutput}.
     *
     * @return {@link #OK} if finished decoding messages successfully.
     *         {@link #NEED_DATA} if you need more data to finish decoding current message.
     *         {@link #NOT_OK} if you cannot decode current message due to protocol specification violation.
     *
     * @throws Exception if the read data violated protocol specification
     */
    public final MessageDecoderResult decode(IoSession session, IoBuffer in, ProtocolDecoderOutput out) throws Exception {
        SessionStatus sessionStatus = (SessionStatus) session.getAttribute(SESSION_STATUS);
        SessionStatus returnedSessionStatus;
        if (sessionStatus != null && sessionStatus.state == WAITING_FOR_DATA) {
            if (in.remaining() < sessionStatus.bytesNeeded + 2) {
                return MessageDecoderResult.NEED_DATA;
            }
            // get the bytes we want, and that's it

            byte[] buffer = new byte[sessionStatus.bytesNeeded];
            in.get(buffer);

            // eat crlf at end
            String crlf = in.getString(2, DECODER);
            if (crlf.equals(CRLF))
                returnedSessionStatus = continueSet(session, out, sessionStatus, buffer);
            else {
                session.setAttribute(SESSION_STATUS, new SessionStatus(READY));
                return MessageDecoderResult.NOT_OK;
            }
        } else {
            // retrieve the first line of the input, if there isn't a full one, request more
            words.clear();

            in.mark();

            boolean completed = collectCommand(in);

            if (!completed) {
                in.reset();
                return MessageDecoderResult.NEED_DATA;
            }

            final int numParts = words.size();
            List<String> parts = new ArrayList<String>(numParts);
            for (StringBuilder word : words) {
                parts.add(word.toString());
            }
            returnedSessionStatus = processLine(parts, out, numParts);
        }

        if (returnedSessionStatus.state != ERROR) {
            session.setAttribute(SESSION_STATUS, returnedSessionStatus);
            return MessageDecoderResult.OK;
        } else
            return MessageDecoderResult.NOT_OK;

    }

    private boolean collectCommand(IoBuffer in) {
        StringBuilder wordBuffer = new StringBuilder(WORD_BUFFER_INIT_SIZE);
        boolean completed = false;
        boolean appended = false;
        
        while (in.hasRemaining()) {
            char c = (char) in.get();

            if (c == ' ') {
                words.add(wordBuffer);
                appended = false;
                wordBuffer = new StringBuilder(WORD_BUFFER_INIT_SIZE);
            } else if (c == '\r' && in.hasRemaining() && in.get() == (byte) '\n') {
                if (appended)
                    words.add(wordBuffer);
                completed = true;
                break;
            } else {
                wordBuffer.append(c);
                appended = true;
            }
        }
        return completed;
    }


    /**
     * Process an individual completel protocol line and either passes the command for processing by the
     * session handler, or (in the case of SET-type commands) partially parses the command and sets the session into
     * a state to wait for additional data.
     * @param parts the (originally space separated) parts of the command
     * @param out the MINA protocol decoder output to pass our command on to
     * @param numParts number of arguments
     * @return the session status we want to set the session to
     */
    private SessionStatus processLine(List<String> parts, ProtocolDecoderOutput out, int numParts) {
        final String cmdType = parts.get(0).toUpperCase().intern();
        CommandMessage cmd = new CommandMessage(cmdType);

        if (cmdType == Commands.ADD ||
                cmdType == Commands.SET ||
                cmdType == Commands.REPLACE ||
                cmdType == Commands.CAS ||
                cmdType == Commands.APPEND ||
                cmdType == Commands.PREPEND) {

            // if we don't have all the parts, it's malformed
            if (numParts < 5) {
                return new SessionStatus(ERROR);
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

            return new SessionStatus(WAITING_FOR_DATA, size, cmd);

        } else if (cmdType == Commands.GET ||
                cmdType == Commands.GETS ||
                cmdType == Commands.STATS ||
                cmdType == Commands.QUIT ||
                cmdType == Commands.VERSION) {
            // CMD <options>*
            cmd.keys.addAll(parts.subList(1, numParts));

            out.write(cmd);
        } else if (cmdType == Commands.INCR ||
                cmdType == Commands.DECR) {

            if (numParts < 2 || numParts > 3)
                return new SessionStatus(ERROR);

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
            return new SessionStatus(ERROR);
        }


        return new SessionStatus(READY);

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

        return new SessionStatus(READY);
    }

}
