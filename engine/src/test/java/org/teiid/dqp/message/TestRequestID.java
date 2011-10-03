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

package org.teiid.dqp.message;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.dqp.message.RequestID;

import junit.framework.TestCase;


/**
 */
public class TestRequestID extends TestCase {

    /**
     * Constructor for TestRequestID.
     * @param name
     */
    public TestRequestID(String name) {
        super(name);
    }
    
    public void testGetters1() {
        String connID = "100"; //$NON-NLS-1$
        long executionID = 200;
        RequestID r = new RequestID(connID, executionID);
        assertEquals("Lost connectionID", connID, r.getConnectionID()); //$NON-NLS-1$
        assertEquals("Lost executionID", executionID, r.getExecutionID()); //$NON-NLS-1$
        assertEquals("Wrong string representation", "100.200", r.toString()); //$NON-NLS-1$ //$NON-NLS-2$
        
    }
    
    public void testGetters2() {
        long executionID = 200;
        RequestID r = new RequestID(executionID);
        assertEquals("Lost connectionID", null, r.getConnectionID()); //$NON-NLS-1$
        assertEquals("Lost executionID", executionID, r.getExecutionID()); //$NON-NLS-1$
        assertEquals("Wrong string representation", "C.200", r.toString()); //$NON-NLS-1$ //$NON-NLS-2$
        
    }  
    
    public void testEquivalence1() {
        RequestID r1 = new RequestID("100", 200); //$NON-NLS-1$
        UnitTestUtil.helpTestEquivalence(0, r1, r1);
    }

    public void testEquivalence2() {
        RequestID r1 = new RequestID("100", 200); //$NON-NLS-1$
        RequestID r2 = new RequestID("100", 200); //$NON-NLS-1$
        UnitTestUtil.helpTestEquivalence(0, r1, r2);  
    }

    public void testEquivalence3() {
        RequestID r1 = new RequestID("101", 200); //$NON-NLS-1$
        RequestID r2 = new RequestID("100", 200); //$NON-NLS-1$
        UnitTestUtil.helpTestEquivalence(1, r1, r2);  
    }

    public void testEquivalence4() {
        RequestID r1 = new RequestID("100", 200); //$NON-NLS-1$
        RequestID r2 = new RequestID("100", 201); //$NON-NLS-1$
        UnitTestUtil.helpTestEquivalence(1, r1, r2);  
    }

    public void testEquivalence7() {
        RequestID r1 = new RequestID(200);
        RequestID r2 = new RequestID("100", 200); //$NON-NLS-1$
        UnitTestUtil.helpTestEquivalence(1, r1, r2);  
    }
    
    public void testSerialize1() throws Exception {
        RequestID copy = UnitTestUtil.helpSerialize(new RequestID("1", 100)); //$NON-NLS-1$

        assertEquals("1", copy.getConnectionID()); //$NON-NLS-1$
        assertEquals(100, copy.getExecutionID());
        assertEquals("1.100", copy.toString()); //$NON-NLS-1$
    }

    public void testSerialize2() throws Exception {
        RequestID copy = UnitTestUtil.helpSerialize(new RequestID(100));

        assertEquals(null, copy.getConnectionID());
        assertEquals(100, copy.getExecutionID());
        assertEquals("C.100", copy.toString()); //$NON-NLS-1$
    }

}
