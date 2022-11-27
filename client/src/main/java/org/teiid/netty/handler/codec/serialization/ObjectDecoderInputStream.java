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
package org.teiid.netty.handler.codec.serialization;

import java.io.*;
import java.util.List;

import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.InputStreamFactory.StreamFactoryReference;
import org.teiid.core.util.AccessibleBufferedInputStream;
import org.teiid.core.util.ExternalizeUtil;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.jdbc.JDBCPlugin;


/**
 * An {@link ObjectInput} which is interoperable {@link ObjectEncoderOutputStream}.
 *
 * @author The Netty Project (netty-dev@lists.jboss.org)
 * @author Trustin Lee (tlee@redhat.com)
 *
 * @version $Rev: 628 $, $Date: 2009-01-05 20:06:00 -0600 (Mon, 05 Jan 2009) $
 *
 */
public class ObjectDecoderInputStream extends ObjectInputStream {

    private final AccessibleBufferedInputStream in;
    private final DataInput dis;
    private final ClassLoader classLoader;
    private final int maxObjectSize;

    private int remaining;
    private boolean foundLength;

    private InputStream subStream = new InputStream() {

        @Override
        public int read() throws IOException {
            if (remaining-->0) {
                return in.read();
            }
            return -1;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int read = in.read(b, off, Math.min(len, remaining));
            if (read > 0) {
                remaining -= read;
            }
            return read;
        }
    };

    private Object result;
    private int streamIndex;
    private OutputStream stream;
    private List<StreamFactoryReference> streams;

    public ObjectDecoderInputStream(AccessibleBufferedInputStream in, ClassLoader classLoader, int maxObjectSize) throws SecurityException, IOException {
        super();
        this.in = in;
        this.dis = new DataInputStream(in);
        this.classLoader = classLoader;
        this.maxObjectSize = maxObjectSize;
    }

    @Override
    protected final Object readObjectOverride() throws IOException,
            ClassNotFoundException {
        if (result == null) {
            if (!foundLength) {
                clearRemaining();
                remaining = dis.readInt();
                foundLength = true;
                if (remaining <= 0) {
                    throw new StreamCorruptedException("invalid data length: " + remaining); //$NON-NLS-1$
                }
                if (remaining > maxObjectSize) {
                    throw new StreamCorruptedException(JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20028, remaining, maxObjectSize));
                }
            }
            foundLength = false;
            CompactObjectInputStream cois = new CompactObjectInputStream(subStream, classLoader);
            result = cois.readObject();
            streams = ExternalizeUtil.readList(cois, StreamFactoryReference.class);
            streamIndex = 0;
        }
        while (streamIndex < streams.size()) {
            if (!foundLength) {
                clearRemaining();
                remaining = 0xffff & dis.readShort(); //convert to unsigned
                foundLength = true;
                if (remaining < 0) {
                    throw new StreamCorruptedException("Invalid stream chunk length"); //$NON-NLS-1$
                }
            }
            if (stream == null) {
                final File f = File.createTempFile("teiid", null); //$NON-NLS-1$
                StreamFactoryReference sfr = streams.get(streamIndex);
                sfr.setStreamFactory(new InputStreamFactory() {

                    @Override
                    public InputStream getInputStream() throws IOException {
                        return new BufferedInputStream(new FileInputStream(f));
                    }

                    @Override
                    protected void finalize() throws Throwable {
                        super.finalize();
                        f.delete();
                    }

                });
                this.stream = new FileOutputStream(f);
            }
            foundLength = false;
            if (remaining != 0) {
                int available = Math.min(remaining, in.getCount() - in.getPosition());
                if (available > 0) {
                    this.stream.write(in.getBuffer(), in.getPosition(), available);
                    in.setPosition(in.getPosition() + available);
                    remaining -= available;
                }
                if (remaining > 0) {
                    ObjectConverterUtil.write(this.stream, in, in.getBuffer(), remaining, false);
                    remaining = 0;
                }
                continue;
            }
            stream.close();
            stream = null;
            streamIndex++;
        }
        Object toReturn = result;
        result = null;
        streams = null;
        stream = null;
        return toReturn;
    }

    void clearRemaining() throws IOException {
        while (remaining > 0) {
            long skipped = in.skip(remaining);
            if (skipped == 0) {
                break;
            }
            remaining -= skipped;
        }
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

}
