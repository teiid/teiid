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

import javax.sql.rowset.serial.SerialClob;
import javax.sql.rowset.serial.SerialException;

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;

@SuppressWarnings("nls")
public class TestClobValue {

    @Test public void testClobValue() throws Exception {
        String testString = "this is test clob"; //$NON-NLS-1$
        SerialClob clob = new SerialClob(testString.toCharArray()); 
        
        ClobType cv = new ClobType(clob);
        assertEquals(testString, cv.getSubString(1L, (int)cv.length()));
    }
    
    @Test public void testClobValuePersistence() throws Exception {
        String testString = "this is test clob"; //$NON-NLS-1$
        SerialClob clob = new SerialClob(testString.toCharArray());
        
        ClobType cv = new ClobType(clob);
        String key = cv.getReferenceStreamId();
        
        // now force to serialize
        ClobType read = UnitTestUtil.helpSerialize(cv);
        
        assertTrue(read.length() > 0);
                
        // make sure we have kept the reference stream id
        assertEquals(key, read.getReferenceStreamId());
        
        // and lost the original object
        assertNull(read.getReference());
    }
    
    @Test public void testReferencePersistence() throws Exception {
    	String testString = "this is test clob"; //$NON-NLS-1$
        SerialClob clob = new SerialClob(testString.toCharArray());
        
        ClobType cv = new ClobType(clob);
        cv.setReferenceStreamId(null);
        
        // now force to serialize
        ClobType read = UnitTestUtil.helpSerialize(cv);
        
        assertTrue(read.length() > 0);
                
        assertEquals(testString, read.getSubString(1, testString.length()));
    }
    
    @SuppressWarnings("serial")
	@Test public void testReferencePersistenceError() throws Exception {
    	String testString = "this is test clob"; //$NON-NLS-1$
        SerialClob clob = new SerialClob(testString.toCharArray()) {
        	@Override
        	public Reader getCharacterStream() throws SerialException {
        		throw new SerialException();
        	}
        };
        
        ClobType cv = new ClobType(clob);
        cv.setReferenceStreamId(null);
        
        // now force to serialize
        ClobType read = UnitTestUtil.helpSerialize(cv);
        
        assertTrue(read.length() > 0);
        assertNotNull(read.getReferenceStreamId());
        assertNull(read.getReference());
    }
    
    @Test public void testClobSubstring() throws Exception {
    	ClobImpl clob = new ClobImpl() {
    		public java.io.Reader getCharacterStream() throws java.sql.SQLException {
    			return new Reader() {

    				int pos = 0;
    				
					@Override
					public void close() throws IOException {
						
					}

					@Override
					public int read(char[] cbuf, int off, int len)
							throws IOException {
						if (pos < 2) {
							cbuf[off] = 'a';
							pos++;
							return 1;
						}
						return -1;
					}
    			};
    		}
    	};
    	assertEquals("aa", clob.getSubString(1, 3));
    }
    
    public void testClobCompare() throws Exception {
        String testString = "this is test clob"; //$NON-NLS-1$
        SerialClob clob = new SerialClob(testString.toCharArray());
        ClobType ct = new ClobType(clob);
        
        SerialClob clob1 = new SerialClob(testString.toCharArray());
        ClobType ct1 = new ClobType(clob1);
        assertEquals(0, ct1.compareTo(ct));
    }
    
}
