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

package com.metamatrix.query.processor.relational;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import org.junit.Test;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.BufferManagerFactory;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleBuffer;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.buffer.BufferManager.TupleSourceType;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.processor.relational.SortUtility.Mode;
import com.metamatrix.query.sql.lang.OrderBy;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.util.CommandContext;

public class TestSortNode {
    
    public static final int BATCH_SIZE = 100;
    
    private void helpTestSort(List elements, List[] data, List sortElements, List sortTypes, List[] expected, Mode mode) throws MetaMatrixComponentException, MetaMatrixProcessingException {
        BufferManager mgr = NodeTestUtil.getTestBufferManager(100, BATCH_SIZE, BATCH_SIZE);
        CommandContext context = new CommandContext ("pid", "test", null, null, null);               //$NON-NLS-1$ //$NON-NLS-2$
        
        BlockingFakeRelationalNode dataNode = new BlockingFakeRelationalNode(2, data);
        dataNode.setReturnPeriod(3);
        dataNode.setElements(elements);
        dataNode.initialize(context, mgr, null);    
        
        SortNode sortNode = new SortNode(1);
    	sortNode.setSortElements(sortElements, sortTypes);
        sortNode.setMode(mode);
        sortNode.setElements(elements);
        sortNode.addChild(dataNode);        
        sortNode.initialize(context, mgr, null);    
        
        sortNode.open();
        
        int currentRow = 1;
        while(true) {
        	try {
	            TupleBatch batch = sortNode.nextBatch();
	            if (mode != Mode.DUP_REMOVE) {
	                for(int row = currentRow; row <= batch.getEndRow(); row++) {
	                    assertEquals("Rows don't match at " + row, expected[row-1], batch.getTuple(row)); //$NON-NLS-1$
	                }
	            }
	            currentRow += batch.getRowCount();    
	            if(batch.getTerminationFlag()) {
	                break;
	            }
        	} catch (BlockedException e) {
        		
        	}
        }
        assertEquals(expected.length, currentRow - 1);
    }

    /*
     * 1 batch all in memory
     */
    private void helpTestBasicSort(List[] expected, Mode mode) throws Exception {
        ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);

        ElementSymbol es2 = new ElementSymbol("e2"); //$NON-NLS-1$
        es2.setType(DataTypeManager.DefaultDataClasses.STRING);
        
        List elements = new ArrayList();
        elements.add(es1);
        elements.add(es2);
        
        List[] data = new List[20];
        for(int i=0; i<20; i++) { 
            data[i] = new ArrayList();
            data[i].add(new Integer((i*51) % 11));
            
            String str = String.valueOf(i*3);
            str = str.substring(0,1);
            data[i].add(str);              
        }
        
        List sortElements = new ArrayList();
        sortElements.add(es1);
        
        List sortTypes = new ArrayList();
        sortTypes.add(new Boolean(OrderBy.ASC));
        
        helpTestSort(elements, data, sortElements, sortTypes, expected, mode);
    }
    
    private void helpTestAllSorts(int batches) throws Exception {
        ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);

        List elements = new ArrayList();
        elements.add(es1);
        
        int rows = batches * BATCH_SIZE;

        ListNestedSortComparator comparator = new ListNestedSortComparator(new int[] {0}, OrderBy.DESC);

        List[] expected = new List[rows];
        List[] data = new List[rows];
        TreeSet<List> distinct = new TreeSet<List>(comparator);
        for(int i=0; i<rows; i++) { 
            Integer value = new Integer((i*51) % 11);
            data[i] = Arrays.asList(value);
            expected[i] = Arrays.asList(value);
            distinct.add(Arrays.asList(value));
        }
        List[] expectedDistinct = distinct.toArray(new List[distinct.size()]);
        
        List sortElements = new ArrayList();
        sortElements.add(es1);
        
        List sortTypes = new ArrayList();
        sortTypes.add(new Boolean(OrderBy.DESC));

        Arrays.sort(expected, comparator);
        
        for (Mode mode : Mode.values()) {
        	if (mode == Mode.DUP_REMOVE) {
        		helpTestSort(elements, data, sortElements, sortTypes, mode==Mode.SORT?expected:expectedDistinct, mode);
        	}
        }
    }
        
    @Test public void testNoSort() throws Exception {
        helpTestAllSorts(0);
    }    
    
    @Test public void testBasicSort() throws Exception {
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(0), "0" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(0), "3" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(1), "2" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(1), "5" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(2), "1" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(2), "4" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(3), "6" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(3), "3" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(4), "3" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(5), "2" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(5), "5" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(6), "1" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(6), "4" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(7), "3" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(7), "3" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(8), "2" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(9), "1" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(9), "5" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(10), "9" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(10), "4" })                //$NON-NLS-1$
        };
        
        helpTestBasicSort(expected, Mode.SORT);
    }
    
    @Test public void testDupSortDesc() throws Exception {
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(0), "0" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(0), "3" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(1), "2" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(1), "5" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(2), "1" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(2), "4" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(3), "6" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(3), "3" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(4), "3" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(5), "2" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(5), "5" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(6), "1" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(6), "4" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(7), "3" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(7), "3" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(8), "2" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(9), "1" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(9), "5" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(10), "9" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(10), "4" })                //$NON-NLS-1$
        };
        
        helpTestBasicSort(expected, Mode.SORT);
    }
    
    /**
     * Note the ordering here is not stable
     * @throws Exception
     */
    @Test public void testBasicSortRemoveDup() throws Exception {
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(0), "0" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(0), "3" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(1), "2" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(1), "5" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(2), "1" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(2), "4" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(3), "3" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(3), "6" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(4), "3" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(5), "2" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(5), "5" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(6), "1" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(6), "4" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(7), "3" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(8), "2" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(9), "1" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(9), "5" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(10), "4" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(10), "9" })                //$NON-NLS-1$
        };

        helpTestBasicSort(expected, Mode.DUP_REMOVE);
    }   
    
    @Test public void testBasicSortRemoveDupSort() throws Exception {
    	List[] expected = new List[] { 
                Arrays.asList(new Object[] { new Integer(0), "0" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(0), "3" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(1), "2" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(1), "5" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(2), "1" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(2), "4" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(3), "3" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(3), "6" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(4), "3" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(5), "2" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(5), "5" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(6), "1" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(6), "4" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(7), "3" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(8), "2" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(9), "1" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(9), "5" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(10), "4" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(10), "9" })                //$NON-NLS-1$
            };

        helpTestBasicSort(expected, Mode.DUP_REMOVE_SORT);
    }   
    
    @Test public void testBiggerSort() throws Exception {
        helpTestAllSorts(100);
    }
 
    @Test public void testAllSort() throws Exception {
        helpTestAllSorts(1);
    }       
    
    @Test public void testDupRemove() throws Exception {
    	ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
        TupleBuffer tsid = bm.createTupleBuffer(Arrays.asList(es1), "test", TupleSourceType.PROCESSOR); //$NON-NLS-1$
        tsid.addTuple(Arrays.asList(1));
    	SortUtility su = new SortUtility(tsid.createIndexedTupleSource(), Arrays.asList(es1), Arrays.asList(Boolean.TRUE), Mode.DUP_REMOVE, bm, "test"); //$NON-NLS-1$
    	TupleBuffer out = su.sort();
    	TupleSource ts = out.createIndexedTupleSource();
    	assertEquals(Arrays.asList(1), ts.nextTuple());
    	try {
    		ts.nextTuple();
    		fail();
    	} catch (BlockedException e) {
    		
    	}
    	tsid.addTuple(Arrays.asList(2));
    	su.sort();
    	assertEquals(Arrays.asList(2), ts.nextTuple());
    }
    
}
