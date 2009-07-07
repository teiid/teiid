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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.SQLException;
//## JDBC4.0-begin ##
import java.sql.SQLXML;
//## JDBC4.0-end ##

/*## JDBC3.0-JDK1.5-begin ##
import com.metamatrix.core.jdbc.SQLXML; 
## JDBC3.0-JDK1.5-end ##*/
import java.util.Properties;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamSource;

import com.metamatrix.common.util.SqlUtil;


/** 
 * This metamatrix specific implementation of the SQLXML interface;
 */
public class SQLXMLImpl implements SQLXML {
    
    XMLTranslator translator;
        
    public SQLXMLImpl(String str) {
        this.translator = new XMLStreamSourceTranslator(str, new Properties());
    }

    public SQLXMLImpl(String str, Properties props) {
        this.translator = new XMLStreamSourceTranslator(str, props);
    }
    
    public SQLXMLImpl(char[] str) {
        this.translator = new XMLStreamSourceTranslator(str, new Properties());
    }

    public SQLXMLImpl(char[] str, Properties props) {
        this.translator = new XMLStreamSourceTranslator(str, props);
    }    
    
    public SQLXMLImpl(XMLReaderFactory factory) {
        this(factory, new Properties());
    }

    // the reason we have factory insted of reader is the xml may be 
    // and can be streamed multiple times by a client. If we have reader
    // one it closed it may not be accessable.
    public SQLXMLImpl(XMLReaderFactory factory, Properties props) {
        this.translator = new XMLStreamSourceTranslator(factory, props);
    }
   
    public SQLXMLImpl(StreamSource streamSource) {
        this(streamSource, new Properties());
    }
    
    public SQLXMLImpl(StreamSource streamSource, Properties props) {
        this.translator = new XMLStreamSourceTranslator(streamSource, props);
    }
        
    public SQLXMLImpl(DOMSource domSource) {
        this.translator = new XMLDomSourceTranslator(domSource, new Properties());
    }
    
    public SQLXMLImpl(DOMSource domSource, Properties props) {
        this.translator = new XMLDomSourceTranslator(domSource, props);
    }

    public SQLXMLImpl(SAXSource saxSource) {
        this.translator = new XMLSAXSourceTranslator(saxSource, new Properties());
    }    
    
    public SQLXMLImpl(SAXSource saxSource, Properties props) {
        this.translator = new XMLSAXSourceTranslator(saxSource, props);
    }    

    public SQLXMLImpl(XMLTranslator translator) {
        this.translator = translator;
    }    
    
    public <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
        try {
            Source c = this.translator.getSource();
            if (sourceClass == null || sourceClass == c.getClass()) {
            	return (T)c;
            }
        } catch (IOException e) {
            throw new SQLException(e.getMessage());
        }
        throw new SQLException("Unsupported source type " + sourceClass);
    }

    public Reader getCharacterStream() throws SQLException {
        try {
            return translator.getReader();
        } catch (IOException e) {
            throw new SQLException(e.getMessage());
        }
    }

    public InputStream getBinaryStream() throws SQLException {
        try {
            return translator.getInputStream();
        } catch (IOException e) {
            throw new SQLException(e.getMessage());
        }
    }

    public String getString() throws SQLException {
        try {
            return translator.getString();
        } catch (IOException e) {
            throw new SQLException(e.getMessage());
        }
    }

    public OutputStream setBinaryStream() throws SQLException {
        throw new SQLException("not implemented");//$NON-NLS-1$
    }

    public Writer setCharacterStream() throws SQLException {
        throw new SQLException("not implemented");//$NON-NLS-1$
    }

    public void setString(String value) throws SQLException {
        throw new SQLException("not implemented");//$NON-NLS-1$
    }

    public String toString() {
        try {
            return getString();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

	public void free() throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public <T extends Result> T setResult(Class<T> resultClass)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}
}
