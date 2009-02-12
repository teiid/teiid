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

package com.metamatrix.dqp.message;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.util.UnitTestUtil;

public class TestResultsMessage extends TestCase {

    /**
     * Constructor for TestResultsMessage.
     * @param name
     */
    public TestResultsMessage(String name) {
        super(name);
    }

    public static ResultsMessage example() {
        ResultsMessage message = new ResultsMessage();
        message.setColumnNames(new String[] {"A", "B", "C", "D"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        message.setDataTypes(new String[] {DataTypeManager.DefaultDataTypes.BIG_INTEGER,
                                           DataTypeManager.DefaultDataTypes.BIG_INTEGER,
                                           DataTypeManager.DefaultDataTypes.BIG_INTEGER,
                                           DataTypeManager.DefaultDataTypes.BIG_INTEGER});
        message.setFetchSize(100);
        message.setFinalRow(200);
        message.setFirstRow(1);
        message.setLastRow(100);
        List parameters = new ArrayList();
        parameters.add(new ParameterInfo(ParameterInfo.IN, 0));
        parameters.add(new ParameterInfo(ParameterInfo.RESULT_SET, 5));
        message.setParameters(parameters);
        message.setPartialResults(false);
        Map planDescs = new HashMap();
        planDescs.put("key1", "val1"); //$NON-NLS-1$ //$NON-NLS-2$
        planDescs.put("key2", "val2"); //$NON-NLS-1$ //$NON-NLS-2$
        planDescs.put("key3", "val3"); //$NON-NLS-1$ //$NON-NLS-2$
        planDescs.put("key4", "val4"); //$NON-NLS-1$ //$NON-NLS-2$
        message.setPlanDescription(planDescs);

        List results = new ArrayList();
        results.add(new BigInteger("100")); //$NON-NLS-1$
        results.add(new BigInteger("200")); //$NON-NLS-1$
        results.add(new BigInteger("300")); //$NON-NLS-1$
        results.add(new BigInteger("400")); //$NON-NLS-1$
        message.setResults(new List[] {results});
        List schemas = new ArrayList();
        schemas.add("schema1"); //$NON-NLS-1$
        schemas.add("schema2"); //$NON-NLS-1$
        schemas.add("schema3"); //$NON-NLS-1$
        message.setSchemas(schemas);
        List warnings = new ArrayList();
        warnings.add(new Exception("warning1")); //$NON-NLS-1$
        warnings.add(new Exception("warning2")); //$NON-NLS-1$
        message.setWarnings(warnings);
        return message;
    }
    
    public void testSerialize() throws Exception {
        ResultsMessage message = example();
        
        ResultsMessage copy = UnitTestUtil.helpSerialize(message);
        
        assertNotNull(copy.getColumnNames());
        assertEquals(4, copy.getColumnNames().length);
        assertEquals("A", copy.getColumnNames()[0]); //$NON-NLS-1$
        assertEquals("B", copy.getColumnNames()[1]); //$NON-NLS-1$
        assertEquals("C", copy.getColumnNames()[2]); //$NON-NLS-1$
        assertEquals("D", copy.getColumnNames()[3]); //$NON-NLS-1$
        
        assertNotNull(copy.getDataTypes());
        assertEquals(4, copy.getDataTypes().length);
        assertEquals(DataTypeManager.DefaultDataTypes.BIG_INTEGER, copy.getDataTypes()[0]);
        assertEquals(DataTypeManager.DefaultDataTypes.BIG_INTEGER, copy.getDataTypes()[1]);
        assertEquals(DataTypeManager.DefaultDataTypes.BIG_INTEGER, copy.getDataTypes()[2]);
        assertEquals(DataTypeManager.DefaultDataTypes.BIG_INTEGER, copy.getDataTypes()[3]);
        
        assertEquals(100, copy.getFetchSize());
        assertEquals(200, copy.getFinalRow());
        assertEquals(1, copy.getFirstRow());
        assertEquals(100, copy.getLastRow());
        
        assertNotNull(copy.getParameters());
        assertEquals(2, copy.getParameters().size());
        ParameterInfo info1 = (ParameterInfo) copy.getParameters().get(0);
        assertEquals(ParameterInfo.IN, info1.getType());
        assertEquals(0, info1.getNumColumns());
        ParameterInfo info2 = (ParameterInfo) copy.getParameters().get(1);
        assertEquals(ParameterInfo.RESULT_SET, info2.getType());
        assertEquals(5, info2.getNumColumns());
        
        assertFalse(copy.isPartialResults());
        
        assertNotNull(copy.getPlanDescription());
        assertEquals(4, copy.getPlanDescription().size());
        assertTrue(copy.getPlanDescription().containsKey("key1")); //$NON-NLS-1$
        assertTrue(copy.getPlanDescription().containsKey("key2")); //$NON-NLS-1$
        assertTrue(copy.getPlanDescription().containsKey("key3")); //$NON-NLS-1$
        assertTrue(copy.getPlanDescription().containsKey("key4")); //$NON-NLS-1$
        assertEquals("val1", copy.getPlanDescription().get("key1")); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("val2", copy.getPlanDescription().get("key2")); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("val3", copy.getPlanDescription().get("key3")); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("val4", copy.getPlanDescription().get("key4")); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertNotNull(copy.getResults());
        assertEquals(1, copy.getResults().length);
        assertNotNull(copy.getResults()[0]);
        assertEquals(4, copy.getResults()[0].size());
        assertEquals(new BigInteger("100"), copy.getResults()[0].get(0)); //$NON-NLS-1$
        assertEquals(new BigInteger("200"), copy.getResults()[0].get(1)); //$NON-NLS-1$
        assertEquals(new BigInteger("300"), copy.getResults()[0].get(2)); //$NON-NLS-1$
        assertEquals(new BigInteger("400"), copy.getResults()[0].get(3)); //$NON-NLS-1$
        
        assertNotNull(copy.getSchemas());
        assertEquals(3, copy.getSchemas().size());
        // We know that the implementation creates a List
        assertTrue(copy.getSchemas().contains("schema1")); //$NON-NLS-1$
        assertTrue(copy.getSchemas().contains("schema2")); //$NON-NLS-1$
        assertTrue(copy.getSchemas().contains("schema3")); //$NON-NLS-1$
        
        assertNotNull(copy.getWarnings());
        assertEquals(2, copy.getWarnings().size());
        assertEquals(Exception.class, copy.getWarnings().get(0).getClass());
        assertEquals("warning1", ((Exception)copy.getWarnings().get(0)).getMessage()); //$NON-NLS-1$
        assertEquals(Exception.class, copy.getWarnings().get(1).getClass());
        assertEquals("warning2", ((Exception)copy.getWarnings().get(1)).getMessage()); //$NON-NLS-1$
        
    }

}
