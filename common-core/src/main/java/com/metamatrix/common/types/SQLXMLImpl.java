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

package com.metamatrix.common.types;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.SQLException;
import java.sql.SQLXML;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import com.metamatrix.common.util.SqlUtil;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.ObjectConverterUtil;


/** 
 * Default SQLXML impl
 */
public class SQLXMLImpl implements SQLXML {
    
    private InputStreamFactory streamFactory;
    
    public SQLXMLImpl(final byte[] bytes) {
    	setStreamFactory(bytes);
    }

	private void setStreamFactory(final byte[] bytes) {
		this.streamFactory = new InputStreamFactory(Streamable.ENCODING) {
			@Override
			public InputStream getInputStream() throws IOException {
				return new ByteArrayInputStream(bytes);
			}
		};
	}
    
    public SQLXMLImpl(final String str) {
    	try {
			setStreamFactory(str.getBytes(Streamable.ENCODING));
		} catch (UnsupportedEncodingException e) {
			throw new MetaMatrixRuntimeException(e);
		}
    }
    
    public SQLXMLImpl(InputStreamFactory factory) {
        this.streamFactory = factory;
    }
    
    public <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
		if (sourceClass == null || sourceClass == StreamSource.class) {
			return (T)new StreamSource(getBinaryStream());
		}
        throw new SQLException("Unsupported source type " + sourceClass);
    }

    public Reader getCharacterStream() throws SQLException {
    	if (this.streamFactory == null) {
    		throw new SQLException("SQLXML already freed"); 
    	}
    	try {
			return new InputStreamReader(this.streamFactory.getInputStream(), this.streamFactory.getEncoding());
		} catch (IOException e) {
			SQLException ex = new SQLException(e.getMessage());
			ex.initCause(e);
			throw ex;
		}
    }

    public InputStream getBinaryStream() throws SQLException {
    	if (this.streamFactory == null) {
    		throw new SQLException("SQLXML already freed"); 
    	}
        try {
			return this.streamFactory.getInputStream();
		} catch (IOException e) {
			SQLException ex = new SQLException(e.getMessage());
			ex.initCause(e);
			throw ex;
		}
    }

    public String getString() throws SQLException {
        try {
            return new String(ObjectConverterUtil.convertToByteArray(getBinaryStream()), Streamable.ENCODING);
        } catch (IOException e) {
			SQLException ex = new SQLException(e.getMessage());
			ex.initCause(e);
			throw ex;
        }
    }

    public OutputStream setBinaryStream() throws SQLException {
        throw SqlUtil.createFeatureNotSupportedException();
    }

    public Writer setCharacterStream() throws SQLException {
        throw SqlUtil.createFeatureNotSupportedException();
    }

    public void setString(String value) throws SQLException {
        throw SqlUtil.createFeatureNotSupportedException();
    }

    public String toString() {
        try {
            return getString();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

	public void free() throws SQLException {
		if (this.streamFactory != null) {
			try {
				this.streamFactory.free();
				this.streamFactory = null;
			} catch (IOException e) {
				SQLException ex = new SQLException(e.getMessage());
				ex.initCause(e);
				throw ex;
			}
		}
	}

	public <T extends Result> T setResult(Class<T> resultClass)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}
}
