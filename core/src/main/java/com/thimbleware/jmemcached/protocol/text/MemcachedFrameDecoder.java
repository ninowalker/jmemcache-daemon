package com.thimbleware.jmemcached.protocol.text;

import com.thimbleware.jmemcached.protocol.SessionStatus;
import com.thimbleware.jmemcached.protocol.exceptions.IncorrectlyTerminatedPayloadException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The frame decoder is responsible for breaking the original stream up into a series of lines.
 * <p/>
 * The code here is heavily based on Netty's DelimiterBasedFrameDecoder, but has been modified because the
 * memcached protocol has two states: 1) processing CRLF delimited lines and 2) spooling results for SET/ADD
 */
@ChannelPipelineCoverage("one")
public class MemcachedFrameDecoder extends FrameDecoder {

    final Logger logger = LoggerFactory.getLogger(MemcachedFrameDecoder.class);

    private SessionStatus status;

    private final ChannelBuffer delimiter;
    private final int maxFrameLength;
    private volatile boolean discardingTooLongFrame;
    private volatile long tooLongFrameLength;

    /**
     * Creates a new instance.
     *
     * @param status         session status instance for holding state of the session
     * @param maxFrameLength the maximum length of the decoded frame.
     *                       A {@link org.jboss.netty.handler.codec.frame.TooLongFrameException} is thrown if frame length is exceeded
     */
    public MemcachedFrameDecoder(SessionStatus status, int maxFrameLength) {
        this.status = status;
        validateMaxFrameLength(maxFrameLength);
        this.delimiter = ChannelBuffers.wrappedBuffer(new byte[]{'\r', '\n'});
        this.maxFrameLength = maxFrameLength;
    }


    @Override
    protected Object decode(ChannelHandlerContext ctx, org.jboss.netty.channel.Channel channel, ChannelBuffer buffer) throws Exception {
        // check the state. if we're WAITING_FOR_DATA that means instead of breaking into lines, we need N bytes
        // otherwise, we're waiting for input
        if (status.state == SessionStatus.State.WAITING_FOR_DATA) {
            if (buffer.readableBytes() < status.bytesNeeded + delimiter.capacity()) return null;

            // verify delimiter matches at the right location
            ChannelBuffer dest = ChannelBuffers.buffer(delimiter.capacity());
            buffer.getBytes(status.bytesNeeded + buffer.readerIndex(), dest);

            if (!dest.equals(delimiter)) {
                // before we throw error... we're ready for the next command
                status.ready();

                // error, no delimiter at end of payload
                throw new IncorrectlyTerminatedPayloadException("payload not terminated correctly");
            } else {

                status.processingMultiline();

                // There's enough bytes in the buffer and the delimiter is at the end. Read it.

                // Successfully decoded a frame.  Return the decoded frame.
                ChannelBuffer result = ChannelBuffers.buffer(status.bytesNeeded);
                buffer.readBytes(result);

                // Consume
                buffer.skipBytes(delimiter.capacity());

                return result;
            }

        } else {
            int minFrameLength = Integer.MAX_VALUE;
            ChannelBuffer foundDelimiter = null;
            int frameLength = indexOf(buffer, delimiter);
            if (frameLength >= 0 && frameLength < minFrameLength) {
                minFrameLength = frameLength;
                foundDelimiter = delimiter;
            }

            if (foundDelimiter != null) {
                int minDelimLength = foundDelimiter.capacity();
                ChannelBuffer frame;

                if (discardingTooLongFrame) {
                    // We've just finished discarding a very large frame.
                    // Throw an exception and go back to the initial state.
                    long tooLongFrameLength = this.tooLongFrameLength;
                    this.tooLongFrameLength = 0L;
                    discardingTooLongFrame = false;
                    buffer.skipBytes(minFrameLength + minDelimLength);
                    fail(tooLongFrameLength + minFrameLength + minDelimLength);
                }

                if (minFrameLength > maxFrameLength) {
                    // Discard read frame.
                    buffer.skipBytes(minFrameLength + minDelimLength);
                    fail(minFrameLength);
                }

                frame = buffer.readBytes(minFrameLength);
                buffer.skipBytes(minDelimLength);

                status.processing();

                return frame;
            } else {
                if (buffer.readableBytes() > maxFrameLength) {
                    // Discard the content of the buffer until a delimiter is found.
                    tooLongFrameLength = buffer.readableBytes();
                    buffer.skipBytes(buffer.readableBytes());
                    discardingTooLongFrame = true;
                }

                return null;
            }
        }
    }

    private void fail(long frameLength) throws TooLongFrameException {
        throw new TooLongFrameException(
                "The frame length exceeds " + maxFrameLength + ": " + frameLength);
    }

    /**
     * Returns the number of bytes between the readerIndex of the haystack and
     * the first needle found in the haystack.  -1 is returned if no needle is
     * found in the haystack.
     */
    private static int indexOf(ChannelBuffer haystack, ChannelBuffer needle) {
        for (int i = haystack.readerIndex(); i < haystack.writerIndex(); i++) {
            int haystackIndex = i;
            int needleIndex;
            for (needleIndex = 0; needleIndex < needle.capacity(); needleIndex++) {
                if (haystack.getByte(haystackIndex) != needle.getByte(needleIndex)) {
                    break;
                } else {
                    haystackIndex++;
                    if (haystackIndex == haystack.writerIndex() &&
                            needleIndex != needle.capacity() - 1) {
                        return -1;
                    }
                }
            }

            if (needleIndex == needle.capacity()) {
                // Found the needle from the haystack!
                return i - haystack.readerIndex();
            }
        }
        return -1;
    }


    private static void validateMaxFrameLength(int maxFrameLength) {
        if (maxFrameLength <= 0) {
            throw new IllegalArgumentException(
                    "maxFrameLength must be a positive integer: " +
                            maxFrameLength);
        }
    }

}
