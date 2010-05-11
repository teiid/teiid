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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;

import javax.xml.transform.Source;

public abstract class InputStreamFactory implements Source {
	
	public interface StreamFactoryReference {
		
		void setStreamFactory(InputStreamFactory inputStreamFactory);
		
	}
	
	private String encoding;
	private String systemId;
	private long length = -1;
	
	public InputStreamFactory() {
		this(Charset.defaultCharset().name());
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
		return new InputStreamReader(this.getInputStream(), this.getEncoding());
    }
}
