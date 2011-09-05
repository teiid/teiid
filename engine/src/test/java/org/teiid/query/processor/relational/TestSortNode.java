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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import org.junit.Test;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.SortSpecification.NullOrdering;
import org.teiid.query.processor.relational.SortUtility.Mode;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("unchecked")
public class TestSortNode {
    
    public static final int BATCH_SIZE = 100;
    
    private void helpTestSort(List elements, List[] data, List sortElements, List sortTypes, List[] expected, Mode mode) throws TeiidComponentException, TeiidProcessingException {
        BufferManager mgr = BufferManagerFactory.getTestBufferManager(100, BATCH_SIZE, BATCH_SIZE);
        CommandContext context = new CommandContext ("pid", "test", null, null, 1);               //$NON-NLS-1$ //$NON-NLS-2$
        
        BlockingFakeRelationalNode dataNode = new BlockingFakeRelationalNode(2, data);
        dataNode.setReturnPeriod(3);
        dataNode.setElements(elements);
        dataNode.initialize(context, mgr, null);    
        
        SortNode sortNode = new SortNode(1);
    	sortNode.setSortElements(new OrderBy(sortElements, sortTypes).getOrderByItems());
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

        ListNestedSortComparator<Integer> comparator = new ListNestedSortComparator<Integer>(new int[] {0}, OrderBy.DESC);

        List<Integer>[] expected = new List[rows];
        List<Integer>[] data = new List[rows];
        TreeSet<List<Integer>> distinct = new TreeSet<List<Integer>>(comparator);
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
    		helpTestSort(elements, data, sortElements, sortTypes, mode==Mode.SORT?expected:expectedDistinct, mode);
        }
    }
    
    @Test public void testComparatorNullOrdering() {
    	ListNestedSortComparator<Integer> comparator = new ListNestedSortComparator<Integer>(new int[] {0}, OrderBy.DESC);
    	comparator.setNullOrdering(Arrays.asList(NullOrdering.FIRST));
    	List<Integer>[] data = new List[3];
        data[0] = Arrays.asList(1);
        data[1] = Arrays.asList((Integer)null);
        data[2] = Arrays.asList(2);
        Arrays.sort(data, comparator);
        assertNull(data[0].get(0));
        comparator.setNullOrdering(Arrays.asList(NullOrdering.LAST));
        Arrays.sort(data, comparator);
        assertNull(data[2].get(0));
        comparator = new ListNestedSortComparator<Integer>(new int[] {0}, OrderBy.ASC);
        Arrays.sort(data, comparator);
        assertNull(data[0].get(0));
        comparator.setNullOrdering(Arrays.asList(NullOrdering.LAST));
        Arrays.sort(data, comparator);
        assertNull(data[2].get(0));
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
    	SortUtility su = new SortUtility(tsid.createIndexedTupleSource(), Arrays.asList(es1), Arrays.asList(Boolean.TRUE), Mode.DUP_REMOVE, bm, "test", tsid.getSchema()); //$NON-NLS-1$
    	TupleBuffer out = su.sort();
    	TupleSource ts = out.createIndexedTupleSource();
    	assertEquals(Arrays.asList(1), ts.nextTuple());
    	try {
    		ts.nextTuple();
    		fail();
    	} catch (BlockedException e) {
    		
    	}
    	tsid.addTuple(Arrays.asList(2));
    	tsid.addTuple(Arrays.asList(3));
    	su.sort();
    	assertEquals(Arrays.asList(2), ts.nextTuple());
    }
    
    @Test public void testDupRemoveLowMemory() throws Exception {
    	ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        BufferManager bm = BufferManagerFactory.getTestBufferManager(0, 2);
        TupleBuffer tsid = bm.createTupleBuffer(Arrays.asList(es1), "test", TupleSourceType.PROCESSOR); //$NON-NLS-1$
        tsid.addTuple(Arrays.asList(1));
        tsid.addTuple(Arrays.asList(2));
    	SortUtility su = new SortUtility(tsid.createIndexedTupleSource(), Arrays.asList(es1), Arrays.asList(Boolean.TRUE), Mode.DUP_REMOVE, bm, "test", tsid.getSchema()); //$NON-NLS-1$
    	TupleBuffer out = su.sort();
    	TupleSource ts = out.createIndexedTupleSource();
    	assertEquals(Arrays.asList(1), ts.nextTuple());
    	assertEquals(Arrays.asList(2), ts.nextTuple());
    	try {
    		ts.nextTuple();
    		fail();
    	} catch (BlockedException e) {
    		
    	}
    	tsid.addTuple(Arrays.asList(3));
    	tsid.addTuple(Arrays.asList(4));
    	tsid.addTuple(Arrays.asList(5));
    	tsid.addTuple(Arrays.asList(6));
    	tsid.addTuple(Arrays.asList(6));
    	tsid.addTuple(Arrays.asList(6));
    	tsid.close();
    	su.sort();
		ts.nextTuple();
		ts.nextTuple();
		assertNotNull(ts.nextTuple());
		assertNotNull(ts.nextTuple());
		assertNull(ts.nextTuple());
    }

}
