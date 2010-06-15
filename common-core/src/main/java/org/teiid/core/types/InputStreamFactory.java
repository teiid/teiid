/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.core.types;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;

import javax.xml.transform.Source;

import org.teiid.core.util.ReaderInputStream;

public abstract class InputStreamFactory implements Source {
	
	public interface StreamFactoryReference {
		
		void setStreamFactory(InputStreamFactory inputStreamFactory);
		
	}
	
	private String encoding;
	private String systemId;
	private long length = -1;
	
	public InputStreamFactory() {
	}
	
	public InputStreamFactory(String encoding) {
		this.encoding = encoding;
	}
	
    /**
     * Get a new InputStream
     * @return
     */
    public abstract InputStream getInputStream() throws IOException;
    
    public String getEncoding() {
		return encoding;
	}
    
    public void setEncoding(String encoding) {
		this.encoding = encoding;
	}
    
    @Override
    public String getSystemId() {
    	return this.systemId;
    }
    
    @Override
    public void setSystemId(String systemId) {
    	this.systemId = systemId;
    }
    
    public void free() throws IOException {
    	
    }
    
    public long getLength() {
		return length;
	}
    
    public void setLength(long length) {
		this.length = length;
	}
    
    public Reader getCharacterStream() throws IOException {
    	String enc = this.getEncoding();
    	if (enc == null) {
    		enc = Charset.defaultCharset().displayName();
    	}
		return new InputStreamReader(this.getInputStream(), enc);
    }
    
    public static class FileInputStreamFactory extends InputStreamFactory {
    	
    	private File f;
    	
    	public FileInputStreamFactory(File f) {
    		this(f, null);
    	}
    	
    	public FileInputStreamFactory(File f, String encoding) {
    		super(encoding);
    		this.f = f;
    		this.setSystemId(f.toURI().toASCIIString());
		}
    	
    	@Override
    	public long getLength() {
    		return f.length();
    	}
    	
    	@Override
    	public InputStream getInputStream() throws IOException {
    		return new BufferedInputStream(new FileInputStream(f));
    	}
    	
    }
    
    public static class ClobInputStreamFactory extends InputStreamFactory {
    	
    	private Clob clob;
    	
    	public ClobInputStreamFactory(Clob clob) {
    		super(Streamable.ENCODING);
    		this.clob = clob;
    	}
    	
    	@Override
    	public InputStream getInputStream() throws IOException {
    		try {
				return new ReaderInputStream(clob.getCharacterStream(), Charset.forName(Streamable.ENCODING));
			} catch (SQLException e) {
				throw new IOException(e);
			}
    	}
    	
    	@Override
    	public Reader getCharacterStream() throws IOException {
    		try {
				return clob.getCharacterStream();
			} catch (SQLException e) {
				throw new IOException(e);
			}
    	}
    	
    }
    
    public static class BlobInputStreamFactory extends InputStreamFactory {
    	
    	private Blob blob;
    	
    	public BlobInputStreamFactory(Blob blob) {
    		this.blob = blob;
    	}
    	
    	@Override
    	public InputStream getInputStream() throws IOException {
    		try {
				return blob.getBinaryStream();
			} catch (SQLException e) {
				throw new IOException(e);
			}
    	}
    	
    	@Override
    	public long getLength() {
    		try {
				return blob.length();
			} catch (SQLException e) {
				return -1;
			}
    	}
    	
    }
    
    public static class SQLXMLInputStreamFactory extends InputStreamFactory {
    	
    	private SQLXML sqlxml;
    	
    	public SQLXMLInputStreamFactory(SQLXML sqlxml) {
    		this.sqlxml = sqlxml;
    	}
    	
    	@Override
    	public InputStream getInputStream() throws IOException {
    		try {
				return sqlxml.getBinaryStream();
			} catch (SQLException e) {
				throw new IOException(e);
			}
    	}
    	
    	@Override
    	public Reader getCharacterStream() throws IOException {
    		try {
				return sqlxml.getCharacterStream();
			} catch (SQLException e) {
				throw new IOException(e);
			}
    	}
    	
    }
    
}
