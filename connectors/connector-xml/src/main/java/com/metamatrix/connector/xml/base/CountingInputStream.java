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

/**
 * @author mharris
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class CountingInputStream extends FilterInputStream
{
    private long m_size = 0;

	/**
	 * @param in
	 */
	public CountingInputStream(InputStream in) {
		super(in);
	}

    public int read() throws IOException
    {
        int retval = in.read();
        if (retval != -1) {
        	++m_size;
        }
        return retval;
    }
    
    public int read(byte[] b) throws IOException
    {
    	int retval = in.read(b);
    	if(retval != -1) {
    		m_size += retval;
    	}
        return retval;
    }

    public int read(byte[] b, int off, int len) throws IOException
    {
        int retval = in.read(b, off, len);
        if(retval != -1) {
        	m_size += retval;
        }
        return retval;
    }
    
    public long getSize()
    {
    	return m_size;
    }
}
