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

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OptionalDataException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.SQLXML;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Result;
import javax.xml.transform.Source;

/**
 * This class represents the SQLXML object along with the Streamable interface.
 * 
 * NOTE that this representation of XML does not become unreadable after
 * read operations.
 */
public final class XMLType extends Streamable<SQLXML> implements SQLXML {
	
	public enum Type {
		UNKNOWN, DOCUMENT, CONTENT, ELEMENT, COMMENT, PI, TEXT
	}
	
	private static final long serialVersionUID = -7922647237095135723L;
	
	private static ThreadLocal<XMLInputFactory> threadLocalFactory = new ThreadLocal<XMLInputFactory>() {
		protected XMLInputFactory initialValue() {
			return XMLInputFactory.newInstance();
		}
	};
	private static XMLInputFactory factory = XMLInputFactory.newInstance();
	private static Boolean factoriesTreadSafe;

	private transient Type type = Type.UNKNOWN;
	private String encoding;

	public static boolean isThreadSafeXmlFactories() {
		if (factoriesTreadSafe == null) {
			factoriesTreadSafe = factory.getClass().getName().contains(".wstx."); //$NON-NLS-1$
		}
		return factoriesTreadSafe;
	}
	
	public static XMLInputFactory getXmlInputFactory() {
		if (isThreadSafeXmlFactories()) {
			return factory;
		}
		return threadLocalFactory.get();
	}
	
    public XMLType(){
        
    }
    
    public XMLType(SQLXML xml) {      
        super(xml);
    }    
                    
    public InputStream getBinaryStream() throws SQLException {
        return this.reference.getBinaryStream();
    }

    public Reader getCharacterStream() throws SQLException {
        return this.reference.getCharacterStream();
    }

    public <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
        return this.reference.getSource(sourceClass);
    }

    public String getString() throws SQLException {
        return this.reference.getString();
    }

    public OutputStream setBinaryStream() throws SQLException {
        return this.reference.setBinaryStream();
    }

    public Writer setCharacterStream() throws SQLException {
        return this.reference.setCharacterStream();
    }

    public void setString(String value) throws SQLException {
        this.reference.setString(value);
    }

	public void free() throws SQLException {
		this.reference.free();
	}

	public <T extends Result> T setResult(Class<T> resultClass)
			throws SQLException {
		return this.reference.setResult(resultClass);
	} 
	
	public Type getType() {
		return type;
	}
	
	public void setType(Type type) {
		this.type = type;
	}
	
	public String getEncoding() {
		return encoding;
	}
	
	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		super.readExternal(in);
		try {
			this.encoding = (String)in.readObject();
		} catch (OptionalDataException e) {
			this.encoding = Streamable.ENCODING;
		}
		try {
			this.type = (Type)in.readObject();
		} catch (OptionalDataException e) {
			this.type = Type.UNKNOWN;
		} catch(IOException e) {
			this.type = Type.UNKNOWN;
		} catch(ClassNotFoundException e) {
			this.type = Type.UNKNOWN;
		}
	}
	
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		if (this.encoding == null) {
			this.encoding = getEncoding(this);
		}
		out.writeObject(this.encoding);
		out.writeObject(this.type);
	}

	/**
	 * Returns the encoding or null if it cannot be determined
	 * @param xml
	 * @return
	 */
	public static String getEncoding(SQLXML xml) {
		try {
			if (xml instanceof XMLType) {
				XMLType type = (XMLType)xml;
				if (type.encoding != null) {
					return type.encoding;
				}
				xml = type.reference;
			}
			if (xml instanceof SQLXMLImpl) {
				Charset cs = ((SQLXMLImpl)xml).getCharset();
				if (cs != null) {
					return cs.displayName();
				}
			}
			return getEncoding(xml.getBinaryStream());
		} catch (SQLException e) {
			return null;
		}
	}

	public static String getEncoding(InputStream is) {
		XMLStreamReader reader;
		try {
			reader = factory.createXMLStreamReader(is);
			return reader.getEncoding();
		} catch (XMLStreamException e) {
			return null;
		} finally {
			try {
				is.close();
			} catch (IOException e) {
			}
		}
	}
	
	@Override
	long computeLength() throws SQLException {
        if (this.reference instanceof SQLXMLImpl) {
        	SQLXMLImpl impl = (SQLXMLImpl)this.reference;
        	return impl.length();
        }
        return BaseLob.length(getBinaryStream());
    }
	
	@Override
	protected void readReference(ObjectInput in) throws IOException {
		byte[] bytes = new byte[(int)getLength()];
		in.readFully(bytes);
		this.reference = new SQLXMLImpl(bytes);
	}
	
	@Override
	protected void writeReference(final DataOutput out) throws IOException {
		try {
			BlobType.writeBinary(out, getBinaryStream(), (int)length);
		} catch (SQLException e) {
			throw new IOException();
		}
	}
}
