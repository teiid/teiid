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

package com.metamatrix.connector.xml.base;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.teiid.connector.api.ConnectorLogger;


public class LoggingInputStreamFilter extends FilterInputStream {

	ConnectorLogger logger;
	StringBuffer buff = new StringBuffer();
	boolean alreadyLogged = false;
	
	public LoggingInputStreamFilter(InputStream in, ConnectorLogger logger) {
		super(in);
		this.logger = logger;
	}

    @Override
	public int read() throws IOException
    {
        int retval = in.read();
        if (retval != -1) {
        	String aChar = Character.valueOf((char)retval).toString();
        	buff.append(aChar);
        } else {
        	log();
        }
        return retval;
    }
    
	@Override
	public int read(byte[] b) throws IOException
    {
    	int retval = in.read(b);
    	if(retval != -1 ) {
    		buff.append(new String(b, 0, retval));
    	}
    	else {
    		log();
        }
        return retval;
    }

    @Override
	public int read(byte[] b, int off, int len) throws IOException
    {
        int retval = in.read(b, off, len);
        if (retval != -1) {
        	buff.append(new String(b, off, off+retval));
        } else {
        	log();
        }
        return retval;
    }

	@Override
	public void close() throws IOException {
		super.close();
		if(!alreadyLogged) {
			log();
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
	
    private void log() {
    	logger.logInfo("XML Connector Framework: response body is: " + buff.toString().trim());
    	alreadyLogged = true;
	}
   
}