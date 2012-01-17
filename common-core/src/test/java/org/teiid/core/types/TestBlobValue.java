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

import javax.sql.rowset.serial.SerialBlob;

import junit.framework.TestCase;

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;


public class TestBlobValue extends TestCase {

    public void testBlobValue() throws Exception {
        String testString = "this is test blob"; //$NON-NLS-1$
        SerialBlob blob = new SerialBlob(testString.getBytes()); 
        
        BlobType bv = new BlobType(blob);
        assertEquals(testString, new String(bv.getBytes(1L, (int)bv.length())));
    }

    public void testBlobValuePersistence() throws Exception {
        String testString = "this is test clob"; //$NON-NLS-1$
        SerialBlob blob = new SerialBlob(testString.getBytes());
        
        BlobType bv = new BlobType(blob);
        String key = bv.getReferenceStreamId();
        
        // now force to serialize
        BlobType read = UnitTestUtil.helpSerialize(bv);
                
        // make sure we have kept the reference stream id
        assertEquals(key, read.getReferenceStreamId());
        
        // and lost the original object
        assertNull(read.getReference());
    }
    
    @Test public void testReferencePersistence() throws Exception {
    	String testString = "this is test clob"; //$NON-NLS-1$
        SerialBlob blob = new SerialBlob(testString.getBytes());
        
        BlobType bv = new BlobType(blob);
        bv.setReferenceStreamId(null);
        // now force to serialize
        BlobType read = UnitTestUtil.helpSerialize(bv);
                
        assertNull(read.getReferenceStreamId());
        
        assertEquals(testString, new String(read.getBytes(1, (int)blob.length())));
    }
    
    public void testBlobCompare() throws Exception {
        String testString = "this is test clob"; //$NON-NLS-1$
        SerialBlob blob = new SerialBlob(testString.getBytes());
        BlobType bv = new BlobType(blob);
        
        SerialBlob blob1 = new SerialBlob(testString.getBytes());
        BlobType bv1 = new BlobType(blob1);
        assertEquals(0, bv1.compareTo(bv));
    }
    
}
