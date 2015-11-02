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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.serialization.CompatibleObjectEncoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.FileStoreInputStreamFactory;
import org.teiid.common.buffer.StorageManager;
import org.teiid.core.types.InputStreamFactory.StreamFactoryReference;
import org.teiid.core.types.Streamable;
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
 * 
 * Note this has been customized to utilize even more idomatic serialization
 * and to support out of message streaming
 * 
 */
public class ObjectDecoder extends LengthFieldBasedFrameDecoder {
	
	public static final long MAX_LOB_SIZE = 1l << 32;

    private final ClassLoader classLoader;
    
    private Object result;
    private int streamIndex;
    private OutputStream stream;
    private List<StreamFactoryReference> streams;
    private StorageManager storageManager;
    private FileStore store;
    private StreamCorruptedException error;

    private int streamDataToRead = -1;
    
	private long maxLobSize = MAX_LOB_SIZE;

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
    public ObjectDecoder(int maxObjectSize, long maxLobSize, ClassLoader classLoader, StorageManager storageManager) {
    	super(maxObjectSize, 0, 4, 0, 4);
        this.classLoader = classLoader;
        this.storageManager = storageManager;
        this.maxLobSize = maxLobSize;
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
    	if (result == null) {
    	    ByteBuf frame = (ByteBuf) super.decode(ctx, buffer);
            if (frame == null) {
                return null;
            }
	        CompactObjectInputStream cois = new CompactObjectInputStream(
	                new ByteBufInputStream(frame), classLoader);
	        result = cois.readObject();
	        streams = ExternalizeUtil.readList(cois, StreamFactoryReference.class);
	        streamIndex = 0;
    	}
    	while (streamIndex < streams.size()) {
    		//read the new chunk size
	    	if (streamDataToRead == -1) {
	    		if (buffer.readableBytes() < 2) {
		            return null;
		        }	
		        streamDataToRead = buffer.readUnsignedShort();
	    	}
	        if (stream == null) {
	        	store = storageManager.createFileStore("temp-stream"); //$NON-NLS-1$
		        StreamFactoryReference sfr = streams.get(streamIndex);
		        sfr.setStreamFactory(new FileStoreInputStreamFactory(store, Streamable.ENCODING));
		        this.stream = new BufferedOutputStream(store.createOutputStream());
	        }
	        //end of stream
	        if (streamDataToRead == 0) {
	        	stream.close();
	        	stream = null;
	        	streamIndex++;
	        	streamDataToRead = -1;
		        continue;
	        }
	        if (store.getLength() + streamDataToRead > maxLobSize) {
	        	if (error == null) {
		        	error = new StreamCorruptedException(
		                    "lob too big: " + (store.getLength() + streamDataToRead) + " (max: " + maxLobSize + ')'); //$NON-NLS-1$ //$NON-NLS-2$
	        	}
	        }
	        if (error == null) {
	        	int toRead = Math.min(buffer.readableBytes(), streamDataToRead);
	        	if (toRead == 0) {
	        		return null;
	        	}
	        	buffer.readBytes(this.stream, toRead);
	        	streamDataToRead -= toRead;
	        	if (streamDataToRead == 0) {
	        		//get the next chunk
	        		streamDataToRead = -1;
	        	}
	        } else {
	        	buffer.release();
	        	streamDataToRead = -1;
	        	break;
	        }
    	}
        Object toReturn = result;
        result = null;
        streams = null;
        stream = null;
        store = null;
        if (error != null) {
        	StreamCorruptedException sce = error;
        	error = null;
        	throw sce;
        }
        return toReturn;
    }

    /*
    @Override
    protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
        return buffer.slice(index, length);
    }
    */
}
