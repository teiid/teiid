/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import com.metamatrix.connector.api.ConnectorLogger;

public class LoggingInputStreamFilter extends FilterInputStream {

	ConnectorLogger logger;
	StringBuffer buff = new StringBuffer();
	boolean alreadyLogged = false;
	
	public LoggingInputStreamFilter(InputStream in, ConnectorLogger logger) {
		super(in);
		this.logger = logger;
	}

    public int read() throws IOException
    {
        int retval = in.read();
        if (retval != -1) {
        	String aChar = new Character((char)retval).toString();
        	buff.append(aChar);
        } else {
        	log();
        }
        return retval;
    }
    
	public int read(byte[] b) throws IOException
    {
    	int retval = in.read(b);
    	if(retval != -1 ) {
    		buff.append(new String(b, 0, retval));
    	}
    	if (-1 == retval || retval < b.length) {
    		log();
        }
        return retval;
    }

    public int read(byte[] b, int off, int len) throws IOException
    {
        int retval = in.read(b, off, len);
        if (retval != -1) {
        	buff.append(new String(b));
        } else {
        	log();
        }
        return retval;
    }

	public void close() throws IOException {
		super.close();
		if(!alreadyLogged) {
			log();
		}
	}

	public synchronized void mark(int readlimit) {
		// we don't suppoort this.
	}

	public boolean markSupported() {
		return false;
	}

	public synchronized void reset() throws IOException {
		throw new IOException(Messages.getString("InputStream_reset_not_supported"));
	}

	public long skip(long n) throws IOException {
		for(int i = 0; i < n; i++) {
			if(-1 == this.read()) {
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