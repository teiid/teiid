/*
 * JBoss, Home of Professional Open Source
 *
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @author tags. See the COPYRIGHT.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.teiid.transport;

import static org.jboss.netty.buffer.ChannelBuffers.*;
import static org.jboss.netty.channel.Channels.*;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.channel.ChannelDownstreamHandler;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.stream.ChunkedInput;
import org.jboss.netty.handler.stream.ChunkedStream;
import org.teiid.core.util.ExternalizeUtil;
import org.teiid.netty.handler.codec.serialization.CompactObjectOutputStream;
import org.teiid.netty.handler.codec.serialization.ObjectDecoderInputStream;


/**
 * An encoder which serializes a Java object into a {@link ChannelBuffer}.
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
 * @apiviz.landmark
 * @apiviz.has org.jboss.netty.handler.codec.serialization.ObjectEncoderOutputStream - - - compatible with
 */
@ChannelPipelineCoverage("all")
public class ObjectEncoder implements ChannelDownstreamHandler {
	
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

    /**
     * Creates a new encoder with the estimated length of 512 bytes.
     */
    public ObjectEncoder() {
        this(512);
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
    public ObjectEncoder(int estimatedLength) {
        if (estimatedLength < 0) {
            throw new IllegalArgumentException(
                    "estimatedLength: " + estimatedLength);
        }
        this.estimatedLength = estimatedLength;
    }
    
    public void handleDownstream(
            final ChannelHandlerContext ctx, ChannelEvent evt) throws Exception {
        if (!(evt instanceof MessageEvent)) {
            ctx.sendDownstream(evt);
            return;
        }

        MessageEvent e = (MessageEvent) evt;
        
        if (e.getMessage() instanceof ChunkedInput) {
            ctx.sendDownstream(evt);
            return;
        }
        
        ChannelBufferOutputStream bout =
            new ChannelBufferOutputStream(dynamicBuffer(
                    estimatedLength, ctx.getChannel().getConfig().getBufferFactory()));
        bout.write(LENGTH_PLACEHOLDER);
        final CompactObjectOutputStream oout = new CompactObjectOutputStream(bout);
        try {
	        oout.writeObject(e.getMessage());
	        ExternalizeUtil.writeCollection(oout, oout.getReferences());
	        oout.flush();
	        oout.close();
        } catch (Throwable t) {
        	throw new FailedWriteException(e.getMessage(), t);
        }
        ChannelBuffer encoded = bout.buffer();
        encoded.setInt(0, encoded.writerIndex() - 4);
        write(ctx, e.getFuture(), encoded, e.getRemoteAddress());
		for (InputStream is : oout.getStreams()) {
			Channels.write(ctx.getChannel(), new AnonymousChunkedStream(new BufferedInputStream(is, CHUNK_SIZE)));
		}
    }
    
    static class AnonymousChunkedStream extends ChunkedStream {

		public AnonymousChunkedStream(InputStream in) {
			super(in, CHUNK_SIZE);
		}
    	
		@Override
		public Object nextChunk() throws Exception {
			ChannelBuffer cb = (ChannelBuffer)super.nextChunk();
			int length = cb.capacity();
			ChannelBuffer prefix = wrappedBuffer(new byte[2]);
			prefix.setShort(0, (short)length);
			if (!hasNextChunk()) {
				//append a 0 short
				return wrappedBuffer(prefix, cb, wrappedBuffer(new byte[2]));
			}
			
			return wrappedBuffer(prefix, cb);
		}
		
    }

}
