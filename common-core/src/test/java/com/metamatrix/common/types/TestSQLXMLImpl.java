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
import java.io.Reader;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.Properties;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import junit.framework.TestCase;

import com.metamatrix.core.util.ObjectConverterUtil;

/**
 * Basically we want to make sure that nobody has changed the fundamental contract
 * of translator
 */
public class TestSQLXMLImpl extends TestCase {

    String testStr = "<foo>test</foo>"; //$NON-NLS-1$
    
    XMLTranslator translator = new XMLTranslator() {        
        public String getString() throws IOException {
            return testStr; 
        }
        public Source getSource() throws IOException {
            return new StreamSource(new StringReader(testStr));
        }
        public Reader getReader() throws IOException {
            return new StringReader(testStr);
        }

        public InputStream getInputStream() throws IOException {
            return new ByteArrayInputStream(testStr.getBytes());
        }

        public Properties getProperties() {
            Properties p = new Properties();
            p.setProperty("indent", "yes"); //$NON-NLS-1$ //$NON-NLS-2$
            return p;
        }        
    };
    
	//## JDBC4.0-begin ##
    public void testGetSource() throws Exception {        
        SQLXMLImpl xml = new SQLXMLImpl(translator);
        assertTrue(xml.getSource(null) instanceof StreamSource);
        
        StreamSource ss = (StreamSource)xml.getSource(null);
        assertEquals(testStr, getContents(ss.getReader()));
    }
	//## JDBC4.0-end ##
    
    public void testGetCharacterStream() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(translator);
        assertEquals(testStr, getContents(xml.getCharacterStream()));
    }

    public void testGetBinaryStream() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(translator);
        assertEquals(testStr, ObjectConverterUtil.convertToString(xml.getBinaryStream()));

    }

    public void testGetString() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(translator);
        assertEquals(testStr, xml.getString());
    }

    public void testSetBinaryStream() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(translator);        
        try {
            xml.setBinaryStream();
            fail("we do not support this yet.."); //$NON-NLS-1$
        } catch (SQLException e) {
        }
    }

    public void testSetCharacterStream() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(translator);        
        try {
            xml.setCharacterStream();
            fail("we do not support this yet.."); //$NON-NLS-1$
        } catch (SQLException e) {
        }
    }

    public void testSetString() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(translator);        
        try {
            xml.setString(testStr);
            fail("we do not support this yet.."); //$NON-NLS-1$
        } catch (SQLException e) {
        }
    }
    
    private String getContents(Reader reader) throws IOException {
        StringBuffer sb = new StringBuffer();
        int chr = reader.read();
        while(chr != -1) {
            sb.append((char)chr);
            chr = reader.read();
        }
        reader.close();       
        return sb.toString();
    } 
    
}
