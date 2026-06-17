/*
 * Copyright 2016-2018 Leon Chen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.moilioncircle.redis.replicator.io;

import com.moilioncircle.redis.replicator.util.ByteArray;
import com.moilioncircle.redis.replicator.util.Strings;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

/**
 * @author Leon Chen
 * @since 2.1.0
 */
public class RedisInputStream extends InputStream {
    /** Read position in {@code buf}; the next byte to consume is {@code buf[head]}. */
    protected int head = 0;
    /** Write boundary of {@code buf}; valid data occupies {@code buf[head..tail)}. */
    protected int tail = 0;
    /** Cumulative count of bytes loaded into {@code buf} by all {@link #fill()} calls. */
    protected long total = 0;
    /** Number of bytes consumed since the last {@link #mark()} call. */
    protected long markLen = 0;
    /** Internal read-ahead buffer; valid data occupies {@code buf[head..tail)}. */
    protected final byte[] buf;
    /** Whether byte-counting mode is active; set by {@link #mark()} and cleared by {@link #unmark()}. */
    protected boolean mark = false;
    /** Underlying stream that {@link #fill()} pulls data from. */
    protected final InputStream in;
    /** Listeners notified with every byte consumed; {@code null} or empty means no-op. */
    protected List<RawByteListener> rawByteListeners;

    public RedisInputStream(ByteArray array) {
        this(new ByteArrayInputStream(array));
    }

    public RedisInputStream(final InputStream in) {
        this(in, 8192);
    }

    public RedisInputStream(final InputStream in, int len) {
        this.in = in;
        this.buf = new byte[len];
    }

    /**
     * @param rawByteListeners raw byte listeners
     * @since 2.2.0
     */
    public synchronized void setRawByteListeners(List<RawByteListener> rawByteListeners) {
        this.rawByteListeners = rawByteListeners;
    }

    protected void notify(byte... bytes) {
        if (rawByteListeners == null || rawByteListeners.isEmpty()) return;
        for (RawByteListener listener : rawByteListeners) {
            listener.handle(bytes);
        }
    }

    public int head() {
        return head;
    }

    public int tail() {
        return tail;
    }

    public int bufSize() {
        return buf.length;
    }

    public boolean isMarked() {
        return mark;
    }

    public void mark(long len) {
        mark();
        markLen = len;
    }

    public void mark() {
        if (!mark) {
            mark = true;
            return;
        }
        throw new AssertionError("already marked");
    }

    public long unmark() {
        if (mark) {
            long rs = markLen;
            markLen = 0;
            mark = false;
            return rs;
        }
        throw new AssertionError("must mark first");
    }

    public long total() {
        return total;
    }

    public ByteArray readBytes(long len) throws IOException {
        ByteArray bytes = new ByteArray(len);
        this.read(bytes, 0, len);
        if (mark) markLen += len;
        return bytes;
    }

    public int readInt(int len) throws IOException {
        return readInt(len, true);
    }

    public long readLong(int len) throws IOException {
        return readLong(len, true);
    }

    public int readInt(int length, boolean littleEndian) throws IOException {
        int r = 0;
        for (int i = 0; i < length; ++i) {
            final int v = this.read();
            if (littleEndian) {
                r |= (v << (i << 3));
            } else {
                r = (r << 8) | v;
            }
        }
        int c;
        return r << (c = (4 - length << 3)) >> c;
    }

    public long readUInt(int length) throws IOException {
        return readUInt(length, true);
    }

    public long readUInt(int length, boolean littleEndian) throws IOException {
        return readInt(length, littleEndian) & 0xFFFFFFFFL;
    }

    public int readInt(byte[] bytes) {
        return readInt(bytes, true);
    }

    public int readInt(byte[] bytes, boolean littleEndian) {
        int r = 0;
        int length = bytes.length;
        for (int i = 0; i < length; ++i) {
            final int v = bytes[i] & 0xFF;
            if (littleEndian) {
                r |= (v << (i << 3));
            } else {
                r = (r << 8) | v;
            }
        }
        int c;
        return r << (c = (4 - length << 3)) >> c;
    }

    public long readLong(int length, boolean littleEndian) throws IOException {
        long r = 0;
        for (int i = 0; i < length; ++i) {
            final long v = this.read();
            if (littleEndian) {
                r |= (v << (i << 3));
            } else {
                r = (r << 8) | v;
            }
        }
        return r;
    }

    public String readString(int len) throws IOException {
        return Strings.toString(readBytes(len).first());
    }

    public String readString(int len, Charset charset) throws IOException {
        return Strings.toString(readBytes(len).first(), charset);
    }

    @Override
    public int read() throws IOException {
        return read(true);
    }
    
    private int read(boolean notify) throws IOException {
        if (head >= tail) fill();
        if (mark) markLen += 1;
        byte b = buf[head++];
        if (notify) notify(b);
        return b & 0xff;
    }

    public long read(ByteArray bytes, long offset, long len) throws IOException {
        long total = len;
        long index = offset;
        while (total > 0) {
            int available = tail - head;
            if (available >= total) {
                ByteArray.arraycopy(new ByteArray(buf), head, bytes, index, total);
                head += total;
                break;
            } else {
                ByteArray.arraycopy(new ByteArray(buf), head, bytes, index, available);
                index += available;
                total -= available;
                fill();
            }
        }
        for (byte[] b : bytes) {
            notify(b);
        }
        return len;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return (int) read(new ByteArray(b), off, len);
    }

    @Override
    public int available() throws IOException {
        return tail - head + in.available();
    }

    public long skip(long len, boolean notify) throws IOException {
        long total = len;
        while (total > 0) {
            int available = tail - head;
            if (available >= total) {
                if (notify) notify(Arrays.copyOfRange(buf, head, head + (int) total));
                head += total;
                break;
            } else {
                if (notify) notify(Arrays.copyOfRange(buf, head, tail));
                total -= available;
                fill();
            }
        }
        return len;
    }

    @Override
    public long skip(long len) throws IOException {
        return skip(len, true);
    }

    /**
     * Reads and discards exactly {@code len} bytes, advancing {@code head} one byte
     * at a time so that it is always up-to-date even when an {@link IOException} is thrown mid-way.
     * <p>
     * Unlike {@link #skip(long, boolean)}, which does not advance {@code head} before
     * calling {@link #fill()} when the buffer is exhausted, this method advances {@code head}
     * on every byte. If an {@link IOException} is thrown mid-way, the buffer is already
     * drained up to that point, preventing stale bytes from being misread as RESP by
     * {@link com.moilioncircle.redis.replicator.cmd.ReplyParser}.
     *
     * @param len number of bytes to drain
     * @param notify notify to RawByteListener
     * @throws IOException if an I/O error occurs while draining the stream
     */
    public void drain(int len, boolean notify) throws IOException {
        for (int i = 0; i < len; i++) {
            read(notify);
        }
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    /**
     * Refills {@code buf} from the underlying stream.
     * Resets {@code head} to 0 and sets {@code tail} to the number of bytes read.
     * Throws {@link EOFException} if the stream is exhausted, or re-throws any
     * {@link IOException} (e.g. {@code SocketException}) if the connection is closed.
     */
    protected void fill() throws IOException {
        tail = in.read(buf, 0, buf.length);
        if (tail == -1) throw new EOFException("end of file or end of stream.");
        total += tail;
        head = 0;
    }
}
