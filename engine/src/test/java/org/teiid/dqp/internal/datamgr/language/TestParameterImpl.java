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

package org.teiid.dqp.internal.datamgr.language;

import java.sql.ResultSet;

import org.teiid.connector.language.IParameter.Direction;
import org.teiid.dqp.internal.datamgr.language.ParameterImpl;
import org.teiid.dqp.internal.datamgr.language.ProcedureImpl;


import junit.framework.TestCase;

public class TestParameterImpl extends TestCase {

    /**
     * Constructor for TestParameterImpl.
     * @param name
     */
    public TestParameterImpl(String name) {
        super(name);
    }

    public static ParameterImpl example(int index) throws Exception {
        ProcedureImpl procImpl = TestProcedureImpl.example();
        return (ParameterImpl) procImpl.getParameters().get(index);
    }

    public void testGetIndex() throws Exception {
        assertEquals(2, example(1).getIndex());
    }

    public void testGetDirection() throws Exception {
        assertEquals(Direction.RESULT_SET, example(0).getDirection());
        assertEquals(Direction.IN, example(1).getDirection());
        assertEquals(Direction.IN, example(2).getDirection());
    }

    public void testGetType() throws Exception {
        assertTrue(example(0).getType().equals(ResultSet.class));
        assertTrue(example(1).getType().equals(String.class));
        assertTrue(example(2).getType().equals(Integer.class));
    }

    public void testGetValue() throws Exception {
        assertEquals("x", example(1).getValue()); //$NON-NLS-1$
        assertEquals(new Integer(1), example(2).getValue());
    }
    
    public void testGetValueSpecified() throws Exception {
        assertEquals(false, example(0).getValueSpecified());
        
        ParameterImpl param = new ParameterImpl(1, Direction.IN, null, String.class, null);
        // Test construction state (null value)
        assertEquals(false, param.getValueSpecified());
        // Test value specified not set on null
        param.setValue(null);
        assertEquals(false, param.getValueSpecified());        
        // Test value specified is set on actual value
        param.setValue("RINGTAIL LEMUR"); //$NON-NLS-1$
        assertEquals(true, param.getValueSpecified());
        // Test value specified override
        param.setValueSpecified(false); 
        assertEquals(false, param.getValueSpecified());
    }
    

}
