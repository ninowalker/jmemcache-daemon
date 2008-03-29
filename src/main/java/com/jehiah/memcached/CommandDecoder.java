package com.jehiah.memcached;

import static com.jehiah.memcached.CommandDecoder.SessionState.*;
import org.apache.mina.common.ByteBuffer;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.apache.mina.filter.codec.demux.MessageDecoderAdapter;
import org.apache.mina.filter.codec.demux.MessageDecoderResult;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.List;

/**
 */
public final class CommandDecoder extends MessageDecoderAdapter {

    private final static int THIRTY_DAYS = 60 * 60 * 24 * 30;

    public static CharsetDecoder DECODER  = Charset.forName("US-ASCII").newDecoder();

    private static final int WORD_BUFFER_INIT_SIZE = 16;
    private static final String SESSION_STATUS = "sessionStatus";

    enum SessionState {
        ERROR,
        SET_WAIT_LINE,
        READY
    }

    final class SessionStatus implements Serializable {
        public SessionState state;
        public int bytesNeeded;
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

    public final MessageDecoderResult decodable(IoSession session, ByteBuffer in) {
        // ask the session for its state,
        SessionStatus sessionStatus = (SessionStatus) session.getAttribute(SESSION_STATUS);
        if (sessionStatus != null &&  sessionStatus.state == SET_WAIT_LINE) {
            if (in.remaining() <= sessionStatus.bytesNeeded)
                return MessageDecoderResult.NEED_DATA;
        }
        return MessageDecoderResult.OK;
    }

    public final MessageDecoderResult decode(IoSession session, ByteBuffer in, ProtocolDecoderOutput out) throws Exception {
        SessionStatus sessionStatus = (SessionStatus) session.getAttribute(SESSION_STATUS);
        SessionStatus returnedSessionStatus;
        if (sessionStatus != null && sessionStatus.state == SET_WAIT_LINE) {
            if (in.remaining() < sessionStatus.bytesNeeded)
                return MessageDecoderResult.NEED_DATA;

            // get the bytes we want, and that's it

            byte[] buffer = new byte[sessionStatus.bytesNeeded];
            in.get(buffer);
            //String remainder = in.getString(sessionStatus.bytesNeeded, DECODER);

            returnedSessionStatus = continueSet(session, out, sessionStatus, buffer);
        } else {
            // retrieve the first line of the input, if there isn't a full one, request more
            StringBuffer wordBuffer = new StringBuffer(WORD_BUFFER_INIT_SIZE);
            ArrayList<String> words = new ArrayList<String>(8);
            int r = in.remaining();
            boolean completed = false;
            for (int i = 0; i < r;) {
                char c = (char) in.get();

                if (c == ' ') {
                    words.add(wordBuffer.toString());
                    wordBuffer = new StringBuffer(WORD_BUFFER_INIT_SIZE);
                    i++;
                } else if (c == '\r' && i + 1 < r && in.get() == (byte) '\n') {
                    if (wordBuffer.length() != 0)
                        words.add(wordBuffer.toString());
                    completed = true;
                    break;
                } else {
                    wordBuffer.append(c);
                    i++;
                }
            }
            if (!completed)
                return MessageDecoderResult.NEED_DATA;

            returnedSessionStatus = processLine(words, session, out);
        }

        session.setAttribute(SESSION_STATUS, returnedSessionStatus);
        if (returnedSessionStatus.state == ERROR)
            return MessageDecoderResult.NOT_OK;
        else
            return MessageDecoderResult.OK;
    }


    private SessionStatus processLine(List<String> parts, IoSession session, ProtocolDecoderOutput out) {
        if (parts.isEmpty())
            return new SessionStatus(READY);
        
        CommandMessage cmd = new CommandMessage(parts.get(0).toUpperCase().intern());

        if (cmd.cmd == Commands.ADD ||
                cmd.cmd == Commands.SET ||
                cmd.cmd == Commands.REPLACE) {

            // if we don't have all the parts, it's malformed
            if (parts.size() != 5) {
                return new SessionStatus(ERROR);
            }

            int size = Integer.parseInt(parts.get(4));

            cmd.element = new MCElement();
            cmd.element.keystring = parts.get(1);
            cmd.element.flags = parts.get(2);
            cmd.element.expire = Integer.parseInt(parts.get(3));
            if (cmd.element.expire != 0 && cmd.element.expire <= THIRTY_DAYS) {
                cmd.element.expire += Now();
            }
            cmd.element.data_length = size;

            return new SessionStatus(SET_WAIT_LINE, size, cmd);

        } else if (cmd.cmd == Commands.GET ||
                cmd.cmd == Commands.INCR ||
                cmd.cmd == Commands.DECR ||
                cmd.cmd == Commands.STATS ||
                cmd.cmd == Commands.FLUSH_ALL ||
                cmd.cmd == Commands.DELETE ||
                cmd.cmd == Commands.QUIT ||
                cmd.cmd == Commands.VERSION) {
            // CMD <options>*
            cmd.keys.addAll(parts.subList(1, parts.size()));

            out.write(cmd);
        }


        return new SessionStatus(READY);

    }

    private SessionStatus continueSet(IoSession session, ProtocolDecoderOutput out, SessionStatus state, byte[] remainder) {
        state.cmd.element.data = remainder;

        out.write(state.cmd);

        return new SessionStatus(READY);
    }

    public final int Now() {
        return (int) (System.currentTimeMillis() / 1000);
    }
}
