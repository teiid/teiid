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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.util.List;

import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.FileStoreInputStreamFactory;
import org.teiid.common.buffer.StorageManager;
import org.teiid.core.types.InputStreamFactory.StreamFactoryReference;
import org.teiid.core.types.Streamable;
import org.teiid.core.util.ExternalizeUtil;
import org.teiid.netty.handler.codec.serialization.CompactObjectInputStream;
import org.teiid.netty.handler.codec.serialization.ObjectEncoderOutputStream;
import org.teiid.runtime.RuntimePlugin;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.serialization.CompatibleObjectEncoder;


/**
 * A decoder which deserializes the received values into Java
 * objects.
 * <p>
 * Please note that the serialized form this decoder expects is not
 * compatible with the standard {@link ObjectOutputStream}.  Please use
 * {@link ObjectEncoder} or {@link ObjectEncoderOutputStream} to ensure the
 * interoperability with this decoder.
 * <p>
 * Unless there's a requirement for the interoperability with the standard
 * object streams, it is recommended to use {@link ObjectEncoder} and
 * {@link ObjectDecoder} rather than {@link CompatibleObjectEncoder}.
 *
 * @author The Netty Project (netty-dev@lists.jboss.org)
 * @author Trustin Lee (tlee@redhat.com)
 *
 * @version $Rev: 381 $, $Date: 2008-10-01 20:06:18 +0900 (Wed, 01 Oct 2008) $
 *
 * Note this has been customized to utilize even more idomatic serialization
 * and to support out of message streaming
 *
 */
public class ObjectDecoder extends LengthFieldBasedFrameDecoder {

    public static final long MAX_LOB_SIZE = 1L << 32;

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
            ByteBuf frame = null;
            try {
                frame = (ByteBuf) super.decode(ctx, buffer);
            } catch (TooLongFrameException e) {
                throw new IOException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40166), e);
            }
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
            int toRead = Math.min(buffer.readableBytes(), streamDataToRead);
            if (toRead == 0) {
                return null;
            }
            if (error == null) {
                buffer.readBytes(this.stream, toRead);
            } else {
                buffer.skipBytes(toRead);
            }
            streamDataToRead -= toRead;
            if (streamDataToRead == 0) {
                //get the next chunk
                streamDataToRead = -1;
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

    @Override
    protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
        return buffer.slice(index, length);
    }

}
