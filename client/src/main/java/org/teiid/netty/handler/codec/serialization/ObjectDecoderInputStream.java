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
	        	remaining = dis.readShort();
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
						return new BufferedInputStream(new FileInputStream(f)) {
							@Override
							protected void finalize() throws Throwable {
								super.finalize();
								f.delete();
							}
						};
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
    		remaining -= in.skip(remaining);
    	}
    }

    @Override
    public void close() throws IOException {
    	in.close();
    }
    
}
