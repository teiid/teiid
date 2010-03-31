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

import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.LinkedList;
import java.util.List;

import com.metamatrix.common.types.BlobImpl;
import com.metamatrix.common.types.ClobImpl;
import com.metamatrix.common.types.InputStreamFactory;
import com.metamatrix.common.types.SQLXMLImpl;
import com.metamatrix.common.types.Streamable;
import com.metamatrix.common.types.InputStreamFactory.StreamFactoryReference;
import com.metamatrix.common.util.ReaderInputStream;

/**
 * @author The Netty Project (netty-dev@lists.jboss.org)
 * @author Trustin Lee (tlee@redhat.com)
 *
 * @version $Rev: 6 $, $Date: 2008-08-07 20:40:10 -0500 (Thu, 07 Aug 2008) $
 *
 */
public class CompactObjectOutputStream extends ObjectOutputStream {

    static final int TYPE_PRIMITIVE = 0;
    static final int TYPE_NON_PRIMITIVE = 1;
    
    private List<InputStream> streams = new LinkedList<InputStream>();
    private List<StreamFactoryReference> references = new LinkedList<StreamFactoryReference>();
    
    public CompactObjectOutputStream(OutputStream out) throws IOException {
        super(out);
        enableReplaceObject(true);
    }
    
    public List<InputStream> getStreams() {
		return streams;
	}
    
    public List<StreamFactoryReference> getReferences() {
		return references;
	}

    @Override
    protected void writeStreamHeader() throws IOException {
        writeByte(STREAM_VERSION);
    }

    @Override
    protected void writeClassDescriptor(ObjectStreamClass desc) throws IOException {
        if (desc.forClass().isPrimitive()) {
            write(TYPE_PRIMITIVE);
            super.writeClassDescriptor(desc);
        } else {
            write(TYPE_NON_PRIMITIVE);
            writeUTF(desc.getName());
        }
    }
        
    @Override
    protected Object replaceObject(Object obj) throws IOException {
    	if (obj instanceof Serializable) {
    		return obj;
    	}
		try {
	    	if (obj instanceof Reader) {
	    		streams.add(new ReaderInputStream((Reader)obj, Charset.forName(Streamable.ENCODING)));
	    		StreamFactoryReference sfr = new SerializableReader();
	    		references.add(sfr);
	    		return sfr;
	    	} else if (obj instanceof InputStream) {
	    		streams.add((InputStream)obj);
	    		StreamFactoryReference sfr = new SerializableInputStream();
	    		references.add(sfr);
	    		return sfr;
	    	} else if (obj instanceof SQLXML) {
				streams.add(new ReaderInputStream(((SQLXML)obj).getCharacterStream(), Charset.forName(Streamable.ENCODING)));
	    		StreamFactoryReference sfr = new SQLXMLImpl((InputStreamFactory)null);
	    		references.add(sfr);
	    		return sfr;
	    	} else if (obj instanceof Clob) {
	    		streams.add(new ReaderInputStream(((Clob)obj).getCharacterStream(), Charset.forName(Streamable.ENCODING)));
	    		StreamFactoryReference sfr = new ClobImpl(null, -1);
	    		references.add(sfr);
	    		return sfr;
	    	} else if (obj instanceof Blob) {
	    		streams.add(((Blob)obj).getBinaryStream());
	    		StreamFactoryReference sfr = new BlobImpl(null);
	    		references.add(sfr);
	    		return sfr;
	    	}
		} catch (SQLException e) {
			throw new IOException(e);
		}
    	return super.replaceObject(obj);
    }
    
    static class SerializableInputStream extends InputStream implements Externalizable, StreamFactoryReference {

		private InputStreamFactory isf;
    	private InputStream is;
    	
    	public SerializableInputStream() {
		}
    	
    	public void setStreamFactory(InputStreamFactory streamFactory) {
    		this.isf = streamFactory;
    	}
    	
		@Override
		public int read() throws IOException {
			if (is == null) {
				is = isf.getInputStream();
			}
			return is.read();
		}
		
		@Override
		public void close() throws IOException {
			isf.free();
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException,
				ClassNotFoundException {
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
		}
    }
    
    static class SerializableReader extends Reader implements Externalizable, StreamFactoryReference {

		private InputStreamFactory isf;
    	private Reader r;
    	
    	public SerializableReader() {
		}
    	
    	public void setStreamFactory(InputStreamFactory streamFactory) {
    		this.isf = streamFactory;
    	}

		@Override
		public void close() throws IOException {
			isf.free();
		}

		@Override
		public int read(char[] cbuf, int off, int len) throws IOException {
			if (r == null) {
				r = isf.getCharacterStream();
			}
			return r.read(cbuf, off, len);
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException,
				ClassNotFoundException {
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
		}
    }
    
}
