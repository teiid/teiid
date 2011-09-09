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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.SQLXML;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamSource;

import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.SqlUtil;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/** 
 * Default SQLXML impl
 * 
 * NOTE that this representation of XML does not become unreadable after
 * read operations.
 */
public class SQLXMLImpl extends BaseLob implements SQLXML {
		
	public SQLXMLImpl() {
		
	}
	
    /**
     * Constructs a SQLXML from bytes that are already encoded in {@link Streamable#ENCODING}
     * @param bytes
     */
    public SQLXMLImpl(final byte[] bytes) {
    	super(new InputStreamFactory() {
			@Override
			public InputStream getInputStream() throws IOException {
				return new ByteArrayInputStream(bytes);
			}
			
			@Override
			public StorageMode getStorageMode() {
				return StorageMode.MEMORY;
			}
			
			@Override
			public long getLength() {
				return bytes.length;
			}
		});
    	setEncoding(Streamable.ENCODING);
	}
    
    public SQLXMLImpl(final String str) {
		this(str.getBytes(Charset.forName(Streamable.ENCODING)));
    }
    
    public SQLXMLImpl(InputStreamFactory factory) {
        super(factory);
    }
    
    @Override
    public Charset getCharset() {
    	Charset cs = super.getCharset();
    	if (cs != null) {
    		return cs;
    	}
    	String enc = null;
    	try {
			enc = XMLType.getEncoding(this.getBinaryStream());
		} catch (SQLException e) {
		}
		if (enc != null) {
			setEncoding(enc);
		} else {
			super.setCharset(Streamable.CHARSET);
		}
    	return super.getCharset();
    }
    
    @SuppressWarnings("unchecked")
	public <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
		if (sourceClass == null || sourceClass == StreamSource.class) {
			return (T)new StreamSource(getBinaryStream(), this.getStreamFactory().getSystemId());
		} else if (sourceClass == StAXSource.class) {
			XMLInputFactory factory = XMLInputFactory.newInstance();
			try {
				return (T) new StAXSource(factory.createXMLStreamReader(getBinaryStream()));
			} catch (XMLStreamException e) {
				throw new SQLException(e);
			}
		} else if (sourceClass == SAXSource.class) {
			return (T) new SAXSource(new InputSource(getBinaryStream()));
		} else if (sourceClass == DOMSource.class) {
			try {
				DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
				dbf.setNamespaceAware(true);
				DocumentBuilder docBuilder = dbf.newDocumentBuilder();
	            Node doc = docBuilder.parse(new InputSource(getBinaryStream()));
	            return (T) new DOMSource(doc);
			} catch (ParserConfigurationException e) {
				throw new SQLException(e);
			} catch (SAXException e) {
				throw new SQLException(e);
			} catch (IOException e) {
				throw new SQLException(e);
			}
		}
        throw new SQLException("Unsupported source type " + sourceClass); //$NON-NLS-1$
    }

    public String getString() throws SQLException {
        try {
            return ObjectConverterUtil.convertToString(getCharacterStream());
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

	public <T extends Result> T setResult(Class<T> resultClass)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}
	
}
