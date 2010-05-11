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

import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;

import javax.xml.transform.stream.StreamSource;

import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.Streamable;
import org.teiid.core.util.ObjectConverterUtil;

import junit.framework.TestCase;


/**
 * Basically we want to make sure that nobody has changed the fundamental contract
 * of translator
 */
public class TestSQLXMLImpl extends TestCase {

    String testStr = "<foo>test</foo>"; //$NON-NLS-1$
        
	//## JDBC4.0-begin ##
    public void testGetSource() throws Exception {        
        SQLXMLImpl xml = new SQLXMLImpl(testStr);
        assertTrue(xml.getSource(null) instanceof StreamSource);
        
        StreamSource ss = (StreamSource)xml.getSource(null);
        assertEquals(testStr, new String(ObjectConverterUtil.convertToByteArray(ss.getInputStream()), Streamable.ENCODING));
    }
	//## JDBC4.0-end ##
    
    public void testGetCharacterStream() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(testStr);
        assertEquals(testStr, getContents(xml.getCharacterStream()));
    }

    public void testGetBinaryStream() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(testStr);
        assertEquals(testStr, new String(ObjectConverterUtil.convertToByteArray(xml.getBinaryStream()), Streamable.ENCODING));
    }

    public void testGetString() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(testStr);
        assertEquals(testStr, xml.getString());
    }

    public void testSetBinaryStream() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(testStr);        
        try {
            xml.setBinaryStream();
            fail("we do not support this yet.."); //$NON-NLS-1$
        } catch (SQLException e) {
        }
    }

    public void testSetCharacterStream() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(testStr);        
        try {
            xml.setCharacterStream();
            fail("we do not support this yet.."); //$NON-NLS-1$
        } catch (SQLException e) {
        }
    }

    public void testSetString() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(testStr);        
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
