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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.util.List;

import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.InputStreamFactory.StreamFactoryReference;
import org.teiid.core.util.ExternalizeUtil;


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

    private final InputStream in;
    private final ClassLoader classLoader;
    private final int maxObjectSize;
    
    private boolean foundLength;
    private byte[] buffer;
    private int count;
    
    private Object result;
    private int streamIndex;
    private OutputStream stream;
    private List<StreamFactoryReference> streams;

    public ObjectDecoderInputStream(InputStream in, ClassLoader classLoader, int maxObjectSize) throws SecurityException, IOException {
    	super();
    	this.in = in;
        this.classLoader = classLoader;
        this.maxObjectSize = maxObjectSize;
    }
    
    @Override
    protected final Object readObjectOverride() throws IOException,
    		ClassNotFoundException {
    	if (result == null) {
	        if (!foundLength) {
	        	int dataLen = findLength(4);
	        	if (dataLen <= 0) {
	    		    throw new StreamCorruptedException("invalid data length: " + dataLen); //$NON-NLS-1$
	    		}
	    		if (dataLen > maxObjectSize) {
	    		    throw new StreamCorruptedException(
	    		            "data length too big: " + dataLen + " (max: " + maxObjectSize + ')'); //$NON-NLS-1$ //$NON-NLS-2$
	    		}
	        }
	        fillBuffer();
	        foundLength = false;
	        ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
	        buffer = null;
	        CompactObjectInputStream cois = new CompactObjectInputStream(bais, classLoader);
	        result = cois.readObject();
	        streams = ExternalizeUtil.readList(cois, StreamFactoryReference.class);
	        streamIndex = 0;
    	}
    	while (streamIndex < streams.size()) {
    		if (!foundLength) {
	        	findLength(2);
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
		        this.stream = new BufferedOutputStream(new FileOutputStream(f));
	        }
	        if (buffer.length == 0) {
	        	stream.close();
	        	stream = null;
	        	streamIndex++;
	        	foundLength = false;
	        	buffer = null;
		        continue;
	        }
	        fillBuffer();
	        foundLength = false;
	        this.stream.write(buffer);
	        buffer = null;
    	}
        Object toReturn = result;
        result = null;
        streams = null;
        stream = null;
        return toReturn;
    }

	private int findLength(int bytes) throws IOException, EOFException,
			StreamCorruptedException {
		if (buffer == null) {
			buffer = new byte[bytes];
		}
		fillBuffer();
		int dataLen = getIntFromBytes(buffer);
		buffer = new byte[dataLen];
		foundLength = true;
		return dataLen;
	}

	static int getIntFromBytes(byte[] buffer) {
		int result = 0;
		for (int i = 0; i < buffer.length; i++) {
			result += ((buffer[i] & 0xff) << (buffer.length - i - 1)*8);
		}
		return result;
	}

	private void fillBuffer() throws IOException, EOFException {
		while (count < buffer.length) {
	        int read = in.read(buffer, count, buffer.length - count);
	        if (read == -1) {
	        	throw new EOFException();
	        }
	        count += read;
        }
        count = 0;
	}
    
    @Override
    public void close() throws IOException {
    	in.close();
    }
    
}
