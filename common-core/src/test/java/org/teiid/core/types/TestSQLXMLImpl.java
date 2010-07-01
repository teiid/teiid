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

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;

import javax.xml.transform.stream.StreamSource;

import org.junit.Test;
import org.teiid.core.util.ObjectConverterUtil;

@SuppressWarnings("nls")
public class TestSQLXMLImpl {

    String testStr = "<foo>test</foo>"; //$NON-NLS-1$
        
    @Test public void testGetSource() throws Exception {        
        SQLXMLImpl xml = new SQLXMLImpl(testStr);
        assertTrue(xml.getSource(null) instanceof StreamSource);
        
        StreamSource ss = (StreamSource)xml.getSource(null);
        assertEquals(testStr, new String(ObjectConverterUtil.convertToByteArray(ss.getInputStream()), Streamable.ENCODING));
    }
    
    @Test public void testGetCharacterStream() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(testStr);
        assertEquals(testStr, ObjectConverterUtil.convertToString(xml.getCharacterStream()));
    }

    @Test public void testGetBinaryStream() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(testStr);
        assertEquals(testStr, new String(ObjectConverterUtil.convertToByteArray(xml.getBinaryStream()), Streamable.ENCODING));
    }

    @Test public void testGetString() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(testStr);
        assertEquals(testStr, xml.getString());
    }

    @Test(expected=SQLException.class) public void testSetBinaryStream() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(testStr);        
        xml.setBinaryStream();
    }

    @Test(expected=SQLException.class) public void testSetCharacterStream() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(testStr);        
        xml.setCharacterStream();
    }

    @Test(expected=SQLException.class) public void testSetString() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(testStr);        
        xml.setString(testStr);
    }
    
    @Test public void testGetString1() throws Exception {
    	SQLXMLImpl clob = new SQLXMLImpl() {
    		public java.io.Reader getCharacterStream() throws java.sql.SQLException {
    			return new Reader() {

    				int pos = 0;
    				
					@Override
					public void close() throws IOException {
						
					}

					@Override
					public int read(char[] cbuf, int off, int len)
							throws IOException {
						if (pos < 5) {
							cbuf[off] = 'a';
							pos++;
							return 1;
						}
						return -1;
					}
    			};
    		}
    	};
    	assertEquals("aaaaa", clob.getString());
    }
    
}
