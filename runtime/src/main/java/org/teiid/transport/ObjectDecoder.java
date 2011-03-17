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

import java.io.BufferedOutputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.codec.serialization.CompatibleObjectDecoder;
import org.jboss.netty.handler.codec.serialization.CompatibleObjectEncoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.FileStoreInputStreamFactory;
import org.teiid.common.buffer.StorageManager;
import org.teiid.core.types.Streamable;
import org.teiid.core.types.InputStreamFactory.StreamFactoryReference;
import org.teiid.core.util.ExternalizeUtil;
import org.teiid.netty.handler.codec.serialization.CompactObjectInputStream;
import org.teiid.netty.handler.codec.serialization.ObjectEncoderOutputStream;


/**
 * A decoder which deserializes the received {@link ChannelBuffer}s into Java
 * objects.
 * <p>
 * Please note that the serialized form this decoder expects is not
 * compatible with the standard {@link ObjectOutputStream}.  Please use
 * {@link ObjectEncoder} or {@link ObjectEncoderOutputStream} to ensure the
 * interoperability with this decoder.
 * <p>
 * Unless there's a requirement for the interoperability with the standard
 * object streams, it is recommended to use {@link ObjectEncoder} and
 * {@link ObjectDecoder} rather than {@link CompatibleObjectEncoder} and
 * {@link CompatibleObjectDecoder}.
 *
 * @author The Netty Project (netty-dev@lists.jboss.org)
 * @author Trustin Lee (tlee@redhat.com)
 *
 * @version $Rev: 381 $, $Date: 2008-10-01 20:06:18 +0900 (Wed, 01 Oct 2008) $
 *
 * @apiviz.landmark
 */
public class ObjectDecoder extends FrameDecoder {
	
	public static final long MAX_LOB_SIZE = 1l << 32;

    private final int maxObjectSize;
    private final ClassLoader classLoader;
    
    private Object result;
    private int streamIndex;
    private OutputStream stream;
    private List<StreamFactoryReference> streams;
    private StorageManager storageManager;
    private FileStore store;

    /**
     * Creates a new decoder with the specified maximum object size.
     *
     * @param maxObjectSize  the maximum byte length of the serialized object.
     *                       if the length of the received object is greater
     *                       than this value, {@link StreamCorruptedException}
     *                       will be raised.
     * @param classLoader    the {@link ClassLoader} which will load the class
     *                       of the serialized object
     */
    public ObjectDecoder(int maxObjectSize, ClassLoader classLoader, StorageManager storageManager) {
        if (maxObjectSize <= 0) {
            throw new IllegalArgumentException("maxObjectSize: " + maxObjectSize);
        }

        this.maxObjectSize = maxObjectSize;
        this.classLoader = classLoader;
        this.storageManager = storageManager;
    }

    @Override
    protected Object decode(
            ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
    	if (result == null) {
	        if (buffer.readableBytes() < 4) {
	            return null;
	        }
	
	        int dataLen = buffer.getInt(buffer.readerIndex());
	        if (dataLen <= 0) {
	            throw new StreamCorruptedException("invalid data length: " + dataLen);
	        }
	        if (dataLen > maxObjectSize) {
	            throw new StreamCorruptedException(
	                    "data length too big: " + dataLen + " (max: " + maxObjectSize + ')');
	        }
	
	        if (buffer.readableBytes() < dataLen + 4) {
	            return null;
	        }
	
	        buffer.skipBytes(4);
	        CompactObjectInputStream cois = new CompactObjectInputStream(
	                new ChannelBufferInputStream(buffer, dataLen), classLoader);
	        result = cois.readObject();
	        streams = ExternalizeUtil.readList(cois, StreamFactoryReference.class);
	        streamIndex = 0;
    	}
    	while (streamIndex < streams.size()) {
	    	if (buffer.readableBytes() < 2) {
	            return null;
	        }
	        int dataLen = buffer.getShort(buffer.readerIndex()) & 0xffff;
	        if (buffer.readableBytes() < dataLen + 2) {
	            return null;
	        }
	        buffer.skipBytes(2);
	        
	        if (stream == null) {
	        	store = storageManager.createFileStore("temp-stream"); //$NON-NLS-1$
		        StreamFactoryReference sfr = streams.get(streamIndex);
		        sfr.setStreamFactory(new FileStoreInputStreamFactory(store, Streamable.ENCODING));
		        this.stream = new BufferedOutputStream(store.createOutputStream());
	        }
	        if (dataLen == 0) {
	        	stream.close();
	        	stream = null;
	        	streamIndex++;
		        continue;
	        }
	        if (store.getLength() + dataLen > MAX_LOB_SIZE) {
	        	throw new StreamCorruptedException(
	                    "lob too big: " + store.getLength() + dataLen + " (max: " + MAX_LOB_SIZE + ')');
	        }
	        buffer.readBytes(this.stream, dataLen);
    	}
        Object toReturn = result;
        result = null;
        streams = null;
        stream = null;
        store = null;
        return toReturn;
    }
}
