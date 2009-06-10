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

package com.metamatrix.connector.xml.cache;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.teiid.connector.api.ExecutionContext;

import com.metamatrix.connector.xml.base.Messages;


public class CachingInputStreamFilter extends FilterInputStream {

	private StringBuffer buff = new StringBuffer();
	private Integer chunkIndex;
	private String requestId;
	private boolean idCached = false;
	private ExecutionContext context;
	
	public CachingInputStreamFilter(InputStream in, ExecutionContext context, String requestId) {
		super(in);
		this.context = context;
		this.requestId = requestId;
		chunkIndex = Integer.valueOf(0);
	}

    @Override
	public int read() throws IOException
    {
        int retval = in.read();
        if (retval != -1) {
        	String aChar = Character.valueOf((char)retval).toString();
        	buff.append(aChar);
        }
        chunk();
        return retval;
    }
    
    /**
     * 8192 * 5 / 16
     * 1kb * 5 / 16 bit chars = 2560 chars
     */
	private void chunk() {
		if(!idCached) {
			context.put(requestId, Boolean.TRUE);
			idCached = true;
		}
		
		if(buff.length() >= 2560) {
			context.put(requestId + chunkIndex.toString(), buff.toString());
			chunkIndex++;
			buff = new StringBuffer();
		}	
	}

	@Override
	public int read(byte[] b) throws IOException
    {
    	int retval = in.read(b);
    	if(retval != -1 ) {
    		buff.append(new String(b, 0, retval));
    	}
    	chunk();
    	return retval;
    }

    @Override
	public int read(byte[] b, int off, int len) throws IOException
    {
        int retval = in.read(b, off, len);
        if (retval != -1) {
        	buff.append(new String(b, off, off+retval));
        }
        chunk();
        return retval;
    }

	@Override
	public void close() throws IOException {
		super.close();
		if(buff.length() != 0) {
			chunk();
		}
	}

	@Override
	public synchronized void mark(int readlimit) {
		// we don't suppoort this.
	}

	@Override
	public boolean markSupported() {
		return false;
	}

	@Override
	public synchronized void reset() throws IOException {
		throw new IOException(Messages.getString("InputStream_reset_not_supported"));
	}

	@Override
	public long skip(long n) throws IOException {
		int readval;
		for(int i = 0; i < n; i++) {
			readval = this.read();
			if(readval == -1) {
				return i;
			}
		}
		return n;
	}   
}