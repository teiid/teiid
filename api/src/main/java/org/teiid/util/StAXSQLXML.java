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

package org.teiid.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.sql.SQLException;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stax.StAXSource;

import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.StandardXMLTranslator;
import org.teiid.core.types.Streamable;

/**
 * NOTE that this representation of XML does become unreadable after a read operation.
 */
public class StAXSQLXML extends SQLXMLImpl {

	public interface StAXSourceProvider {
		StAXSource getStaxSource() throws SQLException;
	}

	private static final class SingleUseStAXSourceProvider implements
			StAXSourceProvider {
		private StAXSource source;
		
		public SingleUseStAXSourceProvider(StAXSource source) {
			this.source = source;
		}

		@Override
		public StAXSource getStaxSource() throws SQLException {
			if (source == null) {
				throw new SQLException("Already Freed"); //$NON-NLS-1$
			}
			StAXSource result = source;
			source = null;
			return result;
		}
	}

	private StAXSourceProvider sourceProvider;

	public StAXSQLXML(StAXSource source) {
		this(new SingleUseStAXSourceProvider(source), Streamable.CHARSET);
	}
	
	public StAXSQLXML(StAXSourceProvider provider, Charset charSet) {
		this.sourceProvider = provider;
		this.setCharset(charSet);
	}
	
	@SuppressWarnings("unchecked")
	public <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
		if (sourceClass == null || sourceClass == StAXSource.class) {
			return (T) sourceProvider.getStaxSource();
		}
		return super.getSource(sourceClass);
	}
	
	@Override
	public String getString() throws SQLException {
		StringWriter sw = new StringWriter();
		try {
			new StandardXMLTranslator(getSource(StAXSource.class)).translate(sw);
		} catch (TransformerException e) {
			throw new SQLException(e);
		} catch (IOException e) {
			throw new SQLException(e);
		}
		return sw.toString();
	}
	
	@Override
	public InputStream getBinaryStream() throws SQLException {
		try {
			return new XMLInputStream(getSource(StAXSource.class), XMLOutputFactory.newFactory(), getCharset().name());
		} catch (XMLStreamException e) {
			throw new SQLException(e);
		} catch (FactoryConfigurationError e) {
			throw new SQLException(e);
		}
	}
	
	@Override
	public Reader getCharacterStream() throws SQLException {
		try {
			return new XMLReader(getSource(StAXSource.class), XMLOutputFactory.newFactory());
		} catch (XMLStreamException e) {
			throw new SQLException(e);
		} catch (FactoryConfigurationError e) {
			throw new SQLException(e);
		}
	}
	
}