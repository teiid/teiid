/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.transport;


import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;

import org.teiid.core.util.ExternalizeUtil;
import org.teiid.netty.handler.codec.serialization.CompactObjectOutputStream;
import org.teiid.netty.handler.codec.serialization.ObjectDecoderInputStream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.stream.ChunkedStream;


/**
 * An encoder which serializes a Java object into a message.
 * <p>
 * Please note that the serialized form this encoder produces is not
 * compatible with the standard {@link ObjectInputStream}.  Please use
 * {@link ObjectDecoder} or {@link ObjectDecoderInputStream} to ensure the
 * interoperability with this encoder.
 *
 * @author The Netty Project (netty-dev@lists.jboss.org)
 * @author Trustin Lee (tlee@redhat.com)
 *
 * @version $Rev:231 $, $Date:2008-06-12 16:44:50 +0900 (목, 12 6월 2008) $
 *
 */
public class ObjectEncoder extends ChannelOutboundHandlerAdapter {

    public static class FailedWriteException extends Exception {
        private static final long serialVersionUID = -998903582526732966L;
        private Object object;

        FailedWriteException(Object object, Throwable t) {
            super(t);
            this.object = object;
        }

        public Object getObject() {
            return object;
        }
    }

    private static final byte[] LENGTH_PLACEHOLDER = new byte[4];
    private static final int CHUNK_SIZE = (1 << 16) - 1;

    private final int estimatedLength;
    private final boolean preferDirect;

    /**
     * Creates a new encoder with the estimated length of 512 bytes.
     */
    public ObjectEncoder() {
        this(512, true);
    }

    /**
     * Creates a new encoder.
     *
     * @param estimatedLength
     *        the estimated byte length of the serialized form of an object.
     *        If the length of the serialized form exceeds this value, the
     *        internal buffer will be expanded automatically at the cost of
     *        memory bandwidth.  If this value is too big, it will also waste
     *        memory bandwidth.  To avoid unnecessary memory copy or allocation
     *        cost, please specify the properly estimated value.
     */
    public ObjectEncoder(int estimatedLength, boolean preferDirect) {
        if (estimatedLength < 0) {
            throw new IllegalArgumentException(
                    "estimatedLength: " + estimatedLength);
        }
        this.estimatedLength = estimatedLength;
        this.preferDirect = preferDirect;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        ByteBuf out = allocateBuffer(ctx, this.estimatedLength, this.preferDirect);
        int startIdx = out.writerIndex();
        ByteBufOutputStream bout = new ByteBufOutputStream(out);
        bout.write(LENGTH_PLACEHOLDER);
        final CompactObjectOutputStream oout = new CompactObjectOutputStream(bout);
        try {
            oout.writeObject(msg);
            ExternalizeUtil.writeCollection(oout, oout.getReferences());
            oout.flush();
            oout.close();

            int endIdx = out.writerIndex();
            out.setInt(startIdx, endIdx - startIdx - 4);

            if (out.isReadable()) {
                ctx.write(out, promise);
                for (InputStream is : oout.getStreams()) {
                    ctx.write(new AnonymousChunkedStream(new BufferedInputStream(is, CHUNK_SIZE)), promise);
                }
            } else {
                out.release();
                ctx.write(Unpooled.EMPTY_BUFFER, promise);
            }
            ctx.flush();
            out = null;
        } catch (Throwable t) {
            throw new FailedWriteException(msg, t);
        } finally {
            if (out != null) {
                out.release();
            }
        }
    }

    protected ByteBuf allocateBuffer(ChannelHandlerContext ctx,
            int estimatedSize, boolean preferDirect)
            throws Exception {
        if (preferDirect) {
            return ctx.alloc().ioBuffer(estimatedSize);
        } else {
            return ctx.alloc().heapBuffer(estimatedSize);
        }
    }

    static class AnonymousChunkedStream extends ChunkedStream {

        public AnonymousChunkedStream(InputStream in) {
            super(in, CHUNK_SIZE);
        }

        @Override
        public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
            ByteBuf cb = super.readChunk(allocator);
            int length = cb.capacity();
            ByteBuf prefix = Unpooled.wrappedBuffer(new byte[2]);
            prefix.setShort(0, (short)length);
            if (isEndOfInput()) {
                //append a 0 short
                return Unpooled.wrappedBuffer(prefix, cb, Unpooled.wrappedBuffer(new byte[2]));
            }
            return Unpooled.wrappedBuffer(prefix, cb);
        }

    }

}
