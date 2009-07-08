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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.mockito.Mockito;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BlockedOnMemoryException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.BufferManagerFactory;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.BufferManager.TupleSourceStatus;
import com.metamatrix.common.buffer.BufferManager.TupleSourceType;
import com.metamatrix.common.buffer.impl.SizeUtility;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.processor.relational.NodeTestUtil.TestableBufferManagerImpl;
import com.metamatrix.query.processor.relational.SortUtility.Mode;
import com.metamatrix.query.sql.lang.OrderBy;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.util.CommandContext;

public class TestSortNode {
    
    public static final int BATCH_SIZE = 100;
    public static final int INT_BATCH_SIZE = TestSortNode.getIntBatchSize(); //the size of 100 integers    
    
    private void helpTestSort(long bytesInMemory, List elements, List[] data, List sortElements, List sortTypes, List[] expected, Set blockOn, Mode mode) throws MetaMatrixComponentException, MetaMatrixProcessingException {
        BufferManager mgr = NodeTestUtil.getTestBufferManager(bytesInMemory);
        TestableBufferManagerImpl impl = (TestableBufferManagerImpl) mgr;
        impl.setBlockOn(blockOn);
        impl.getConfig().setTotalAvailableMemory(bytesInMemory);
        impl.getConfig().setGroupUsePercentage(100);
        impl.getConfig().setManagementInterval(0);
        CommandContext context = new CommandContext ("pid", "test", null, null, null);               //$NON-NLS-1$ //$NON-NLS-2$
        
        FakeRelationalNode dataNode = new FakeRelationalNode(2, data);
        dataNode.setElements(elements);
        dataNode.initialize(context, mgr, null);    
        
        SortNode sortNode = new SortNode(1);
        if (mode == Mode.DUP_REMOVE) {
        	sortTypes = Arrays.asList(new Boolean[elements.size()]);
        	Collections.fill(sortTypes, OrderBy.ASC);
            sortNode.setSortElements(elements, sortTypes);
        } else {
        	sortNode.setSortElements(sortElements, sortTypes);
        }
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
            } catch (BlockedOnMemoryException e) {
                if (!impl.wasBlocked()) {
                    throw new BlockedOnMemoryException();
                }
            }
        }
        assertEquals(expected.length, currentRow - 1);
    }

    public static int getIntBatchSize() {
        List[] expected = new List[] { 
                Arrays.asList(new Object[] { new Integer(0) }), 
           };     
        
        String[] types = { "integer" };     //$NON-NLS-1$

        int size = (int)SizeUtility.getBatchSize( types, expected ) * BATCH_SIZE;
        return size;
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
        
        /*
         * the following code will do four tests.
         * no blocking
         * blocking during sort phase
         * blocking during merge (for remove dups this tests defect 24736)
         * blocking during sort node output 
         */
        for (int i = 0; i < 4; i++) {
            Set blockedOn = new HashSet();
            if (i > 0) {
                blockedOn.add(new Integer(i));
            }
            helpTestSort(INT_BATCH_SIZE*2, elements, data, sortElements, sortTypes, expected, blockedOn, mode);
        }
    }
    
    private void helpTestBiggerSort(int batches, int inMemoryBatches) throws Exception {
        ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);

        List elements = new ArrayList();
        elements.add(es1);
        
        int rows = batches * BATCH_SIZE;
        
        List unsortedNumbers = new ArrayList();
        List[] data = new List[rows];
        for(int i=0; i<rows; i++) { 
            data[i] = new ArrayList();
            
            Integer value = new Integer((i*51) % 12321);
            data[i].add(value);
            unsortedNumbers.add(value);
        }
        
        List sortElements = new ArrayList();
        sortElements.add(es1);
        
        List sortTypes = new ArrayList();
        sortTypes.add(new Boolean(OrderBy.ASC));

        Collections.sort(unsortedNumbers);                
        List[] expected = new List[rows];
        for(int i=0; i<unsortedNumbers.size(); i++) { 
            expected[i] = new ArrayList();
            expected[i].add(unsortedNumbers.get(i));
        }
        
        
        for (Mode mode : Mode.values()) {
	        /*
	         * the following code will do four tests, blocking in a variety of places
	         */
	        for (int i = 0; i < 3; i++) {
	            Set blockedOn = new HashSet();
	            if (i > 0) {
	                //block on a variety of positions
	                blockedOn.add(new Integer(i));
	                blockedOn.add(new Integer(inMemoryBatches*i));
	                blockedOn.add(new Integer(batches*i));
	                blockedOn.add(new Integer(batches*(i+1)));
	            }
	            //5 batches in memory out of 10 total
	            helpTestSort(INT_BATCH_SIZE * inMemoryBatches, elements, data, sortElements, sortTypes, expected, blockedOn, mode);
	        }
        }
    }
        
    @Test public void testNoSort() throws Exception {
        helpTestBiggerSort(0, 2);
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
        helpTestBiggerSort(10, 5);
    }
 
    @Test(expected=BlockedOnMemoryException.class) public void testBiggerSortLowMemory() throws Exception {
        helpTestBiggerSort(5, 1);
    }       

    /**
     * Progress can be made here since 2 batches fit in memory
     * 
     * This is also a test of the multi-pass merge
     */
    @Test public void testBiggerSortLowMemory2() throws Exception {
        helpTestBiggerSort(5, 2);
    }
    
    @Test public void testDupRemove() throws Exception {
    	ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
        TupleSourceID tsid = bm.createTupleSource(Arrays.asList(es1), new String[] {DataTypeManager.DefaultDataTypes.INTEGER}, "test", TupleSourceType.PROCESSOR); //$NON-NLS-1$
        bm.addTupleBatch(tsid, new TupleBatch(1, new List[] {Arrays.asList(1)}));
    	SortUtility su = new SortUtility(tsid, Arrays.asList(es1), Arrays.asList(Boolean.TRUE), Mode.DUP_REMOVE, bm, "test", true); //$NON-NLS-1$
    	TupleSourceID out = su.sort();
    	TupleSource ts = bm.getTupleSource(out);
    	assertEquals(Arrays.asList(1), ts.nextTuple());
    	try {
    		ts.nextTuple();
    		fail();
    	} catch (BlockedException e) {
    		
    	}
    	bm.addTupleBatch(tsid, new TupleBatch(2, new List[] {Arrays.asList(2)}));
    	su.sort();
    	assertEquals(Arrays.asList(2), ts.nextTuple());
    }
    
}
