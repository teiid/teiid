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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import javax.sql.rowset.serial.SerialClob;

import com.metamatrix.core.util.UnitTestUtil;

import junit.framework.TestCase;


public class TestClobValue extends TestCase {

    public void testClobValue() throws Exception {
        String testString = "this is test clob"; //$NON-NLS-1$
        SerialClob clob = new SerialClob(testString.toCharArray()); 
        
        ClobType cv = new ClobType(clob);
        assertEquals(testString, cv.getSubString(1L, (int)cv.length()));
    }

    
    public void testClobValuePersistence() throws Exception {
        String testString = "this is test clob"; //$NON-NLS-1$
        SerialClob clob = new SerialClob(testString.toCharArray());
        
        String key = "keytodata"; //$NON-NLS-1$
        ClobType cv = new ClobType(clob);
        cv.setReferenceStreamId(key); 
        
        // now force to serialize
        File saved = new File(UnitTestUtil.getTestScratchPath()+"/clobassaved.bin"); //$NON-NLS-1$
        ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(saved));
        out.writeObject(cv);
        out.close();
        
        // now read back the object from serilized state
        ObjectInputStream in = new ObjectInputStream(new FileInputStream(saved));
        ClobType read = (ClobType)in.readObject();
                
        // make sure we have kept the reference stream id
        assertEquals(key, read.getReferenceStreamId());
        
        // and lost the original object
        try {
            read.getCharacterStream();
            fail("this must thrown a reference stream exception"); //$NON-NLS-1$
        } catch (InvalidReferenceException e) {
            // pass
        }
        
        saved.delete();
    }
    
}
