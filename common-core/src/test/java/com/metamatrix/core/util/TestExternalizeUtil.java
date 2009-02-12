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

package com.metamatrix.core.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import junit.framework.TestCase;

import com.metamatrix.core.MetaMatrixCoreException;

public class TestExternalizeUtil extends TestCase {

    private ByteArrayOutputStream bout;
    private ObjectOutputStream oout;   
    
    private ByteArrayInputStream bin;
    private ObjectInputStream oin;
    
    
    /**
     * Constructor for TestExternalizeUtil.
     * @param name
     */
    public TestExternalizeUtil(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        bout = new ByteArrayOutputStream(4096);
        oout = new ObjectOutputStream(bout);
    }
    
    public void tearDown() throws Exception {
        oout.close();
        bout.close();
        
        if (oin!=null) {
            oin.close();
        }
        if (bin!=null) {
            bin.close();
        }
    }
    
    /**
     * Test ExternalizeUtil writeThrowable() and readThrowable() on Throwables. 
     * @throws Exception
     */
    public void testWriteThrowable() throws Exception {
        Throwable t3 = new Throwable("throwable level 3"); //$NON-NLS-1$
        Throwable t2 = new Throwable("throwable level 2", t3); //$NON-NLS-1$
        Throwable t1 = new Throwable("throwable level 1", t2); //$NON-NLS-1$

        ExternalizeUtil.writeThrowable(oout, t1);
        oout.flush();        
        bin = new ByteArrayInputStream(bout.toByteArray());
        oin = new ObjectInputStream(bin);
        
        Throwable result1 = ExternalizeUtil.readThrowable(oin);        
        assertEqualThrowables(t1, result1);
        
        Throwable result2 = result1.getCause();
        assertEqualThrowables(t2, result2);
        
        Throwable result3 = result2.getCause();
        assertEqualThrowables(t3, result3);
        
    }
    
    /**
     * Test ExternalizeUtil writeThrowable() and readThrowable() on MetaMatrixCoreExceptions. 
     * @throws Exception
     */
    public void testWriteThrowableMetaMatrixCoreException() throws Exception {
        MetaMatrixCoreException t3 = new MetaMatrixCoreException(new Exception("test-externalizable")); //$NON-NLS-1$ 
        MetaMatrixCoreException t2 = new MetaMatrixCoreException(t3); 
        MetaMatrixCoreException t1 = new MetaMatrixCoreException(t2); 

        
        ExternalizeUtil.writeThrowable(oout, t1);
        oout.flush();        
        bin = new ByteArrayInputStream(bout.toByteArray());
        oin = new ObjectInputStream(bin);
        
        MetaMatrixCoreException result1 = (MetaMatrixCoreException) ExternalizeUtil.readThrowable(oin);
        assertEqualThrowables(t1, result1);
        
        MetaMatrixCoreException result2 = (MetaMatrixCoreException) result1.getCause();
        assertEqualThrowables(t2, result2);
        
        MetaMatrixCoreException result3 = (MetaMatrixCoreException) result2.getCause();
        assertEqualThrowables(t3, result3);
    }    
    
    /**
     * Assert that the two exceptions have the same message and status.
     */
    public static void assertEqualThrowables(Throwable e1, Throwable e2) {
        assertEquals(e1.getClass(), e2.getClass());
        assertEquals(e1.getMessage(), e2.getMessage());
        
        StackTraceElement[] stack1 = e1.getStackTrace();
        StackTraceElement[] stack2 = e2.getStackTrace();
        assertEquals(stack1.length, stack2.length);
        for (int i=0; i<stack1.length; i++) {
            assertEquals(stack1[i], stack2[i]);
        }
    }

}
