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

package org.teiid.query.processor.relational;

import java.util.Arrays;

import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.function.aggregate.Count;
import org.teiid.query.processor.relational.SortingFilter;
import org.teiid.query.sql.symbol.ElementSymbol;

import junit.framework.TestCase;


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
    
    public void helpTestDuplicateFilter(Object[] input, Class dataType, int expected) throws TeiidComponentException, TeiidProcessingException {
        BufferManager mgr = BufferManagerFactory.getStandaloneBufferManager();
        
        SortingFilter filter = new SortingFilter(new Count(), mgr, "test", true); //$NON-NLS-1$
        filter.initialize(dataType, dataType);
        ElementSymbol element = new ElementSymbol("val"); //$NON-NLS-1$
        element.setType(dataType);
        filter.setElements(Arrays.asList(element));
        filter.reset();
        
        // Add inputs
        for(int i=0; i<input.length; i++) {
            filter.addInputDirect(input[i], null);    
        }        
        
        Integer actual = (Integer) filter.getResult();
        assertEquals("Did not get expected number of results", expected, actual.intValue()); //$NON-NLS-1$
    }

    public void testNoInputs() throws Exception {
        helpTestDuplicateFilter(new Object[0], DataTypeManager.DefaultDataClasses.STRING, 0);           
    }
    
    public void testSmall()  throws Exception {
        Object[] input = new Object[] { "a", "b", "a", "c", "a", "c", "c", "f" }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$

        helpTestDuplicateFilter(input, DataTypeManager.DefaultDataClasses.STRING, 4);        
    }
    
    public void testBig() throws Exception {
        int NUM_VALUES = 10000;
        int NUM_OUTPUT = 200;
        Object[] input = new Object[NUM_VALUES];

        for(int i=0; i<NUM_VALUES; i++) {
            input[i] = new Integer(i % NUM_OUTPUT);
        }

        helpTestDuplicateFilter(input, DataTypeManager.DefaultDataClasses.INTEGER, NUM_OUTPUT);        
    }
    
}
