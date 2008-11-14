/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.query.processor.relational;

import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.StorageManager;
import com.metamatrix.common.buffer.impl.TestBufferManagerImpl;
import com.metamatrix.common.buffer.storage.memory.MemoryStorageManager;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.function.aggregate.AggregateFunction;

/**
 */
public class TestDuplicateFilter extends TestCase {

    /**
     * Constructor for TestDuplicateFilter.
     * @param arg0
     */
    public TestDuplicateFilter(String arg0) {
        super(arg0);
    }
    
    private StorageManager createMemoryStorageManager() {
        return new MemoryStorageManager();
    }
    
    private StorageManager createFakeDatabaseStorageManager() {
        return new MemoryStorageManager() {
            public int getStorageType() { 
                return StorageManager.TYPE_DATABASE;    
            }  
        };        
    }    

    public void helpTestDuplicateFilter(Object[] input, Class dataType, Object[] expected) throws MetaMatrixComponentException, MetaMatrixProcessingException {
        BufferManager mgr = TestBufferManagerImpl.getTestBufferManager(1000000, createMemoryStorageManager(), createFakeDatabaseStorageManager());
        
        AggregateFunction collector = new AggregateFunctionCollector();                           
        DuplicateFilter filter = new DuplicateFilter(collector, mgr, "test", mgr.getProcessorBatchSize()); //$NON-NLS-1$
        filter.initialize(dataType);
        filter.reset();
        
        // Add inputs
        for(int i=0; i<input.length; i++) {
            filter.addInput(input[i]);    
        }        
        
        // Get outputs that made it to collector and compare
        List actual = (List) filter.getResult();
        //System.out.println("Actual values = " + actual);
        assertEquals("Did not get expected number of results", expected.length, actual.size()); //$NON-NLS-1$

        for(int i=0; i<expected.length; i++) {
            assertEquals("Did not getexpected value for " + i, expected[i], actual.get(i)); //$NON-NLS-1$
        }                
    }

    public void testNoInputs() throws Exception {
        helpTestDuplicateFilter(new Object[0], DataTypeManager.DefaultDataClasses.STRING, new Object[0]);           
    }
    
    public void testSmall()  throws Exception {
        Object[] input = new Object[] { "a", "b", "a", "c", "a", "c", "c", "f" }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
        Object[] expected = new Object[] { "a", "b", "c", "f" }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$

        helpTestDuplicateFilter(input, DataTypeManager.DefaultDataClasses.STRING, expected);        
    }
    
    public void testBig() throws Exception {
        int NUM_VALUES = 10000;
        int NUM_OUTPUT = 200;
        Object[] input = new Object[NUM_VALUES];

        for(int i=0; i<NUM_VALUES; i++) {
            input[i] = new Integer(i % NUM_OUTPUT);
        }

        Object[] expected = new Object[NUM_OUTPUT];
        for(int i=0; i<NUM_OUTPUT; i++) { 
            expected[i] = new Integer(i);    
        }
        
        helpTestDuplicateFilter(input, DataTypeManager.DefaultDataClasses.INTEGER, expected);        
    }
    
}
