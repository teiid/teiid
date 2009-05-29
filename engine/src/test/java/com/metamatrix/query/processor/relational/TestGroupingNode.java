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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.BufferManagerFactory;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.buffer.impl.BufferManagerImpl;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.function.FunctionDescriptor;
import com.metamatrix.query.function.FunctionLibraryManager;
import com.metamatrix.query.processor.FakeDataManager;
import com.metamatrix.query.processor.FakeTupleSource;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.util.CommandContext;

public class TestGroupingNode extends TestCase {

	// ################################## FRAMEWORK ################################
	
	public TestGroupingNode(String name) { 
		super(name);
	}	
	
	// ################################## TEST HELPERS ################################

	public static TupleSource createTupleSource1() { 
		List symbols = new ArrayList();
		symbols.add(new ElementSymbol("col1")); //$NON-NLS-1$
		symbols.add(new ElementSymbol("col2")); //$NON-NLS-1$
		
		List[] tuples = new List[] {
			Arrays.asList(new Object[] { new Integer(5), new Integer(3) }),
			Arrays.asList(new Object[] { new Integer(2), new Integer(1) }),
			Arrays.asList(new Object[] { new Integer(4), null }),
			Arrays.asList(new Object[] { null, new Integer(3) }),
			Arrays.asList(new Object[] { new Integer(0), new Integer(4) }),
			Arrays.asList(new Object[] { new Integer(1), new Integer(2) }),
			Arrays.asList(new Object[] { new Integer(4), new Integer(2) }),
			Arrays.asList(new Object[] { new Integer(6), new Integer(4) }),
			Arrays.asList(new Object[] { new Integer(6), new Integer(3) }),
			Arrays.asList(new Object[] { new Integer(3), new Integer(0) }),
			Arrays.asList(new Object[] { new Integer(4), new Integer(3) }),
			Arrays.asList(new Object[] { new Integer(2), new Integer(1) }),
			Arrays.asList(new Object[] { new Integer(2), new Integer(1) }),
			Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),
			Arrays.asList(new Object[] { null, null }),
		};
		
		return new FakeTupleSource(symbols, tuples);
	} 
	
	private void helpProcess(BufferManager mgr,
                             GroupingNode node,
                             CommandContext context,
                             List[] expected) throws MetaMatrixComponentException,
                                             BlockedException,
                                             MetaMatrixProcessingException {
        TupleSource dataSource = createTupleSource1();
        helpProcess(mgr, node, context, expected, dataSource);
    }
    
    private void helpProcess(BufferManager mgr,
                             GroupingNode node,
                             CommandContext context,
                             List[] expected,
                             TupleSource dataSource) throws MetaMatrixComponentException,
                                                    BlockedException,
                                                    MetaMatrixProcessingException {
        RelationalNode dataNode = new FakeRelationalNode(0, dataSource, mgr.getProcessorBatchSize());
        dataNode.setElements(dataSource.getSchema());            
        dataNode.initialize(context, mgr, null);
        node.addChild(dataNode);            
        node.open();
        
        int currentRow = 1;
        while(true) {
            try {
                TupleBatch batch = node.nextBatch();
                for(int row = currentRow; row <= batch.getEndRow(); row++) {
                    List tuple = batch.getTuple(row);
                    assertEquals("Rows don't match at " + row, expected[row-1], tuple); //$NON-NLS-1$
                }
                currentRow += batch.getRowCount();
                if(batch.getTerminationFlag()) {
                    break;
                }
            } catch (BlockedException e) {
                //ignore
            }
        }
        assertEquals(expected.length, currentRow -1);
    }
    
	// ################################## ACTUAL TESTS ################################
	
	public void test1() throws Exception {
        BufferManager mgr = BufferManagerFactory.getStandaloneBufferManager();

        // Set up
		GroupingNode node = new GroupingNode(1);
		List outputElements = new ArrayList();
		ElementSymbol col1 = new ElementSymbol("col1"); //$NON-NLS-1$
		col1.setType(Integer.class);
		ElementSymbol col2 = new ElementSymbol("col2"); //$NON-NLS-1$
		col2.setType(Integer.class);
		outputElements.add(col1);
		outputElements.add(new AggregateSymbol("countAll", "COUNT", false, null)); //$NON-NLS-1$ //$NON-NLS-2$
		outputElements.add(new AggregateSymbol("count", "COUNT", false, col2)); //$NON-NLS-1$ //$NON-NLS-2$
		outputElements.add(new AggregateSymbol("countDist", "COUNT", true, col2)); //$NON-NLS-1$ //$NON-NLS-2$
		outputElements.add(new AggregateSymbol("sum", "SUM", false, col2)); //$NON-NLS-1$ //$NON-NLS-2$
		outputElements.add(new AggregateSymbol("sumDist", "SUM", true, col2)); //$NON-NLS-1$ //$NON-NLS-2$
		outputElements.add(new AggregateSymbol("avg", "AVG", false, col2)); //$NON-NLS-1$ //$NON-NLS-2$
		outputElements.add(new AggregateSymbol("avgDist", "AVG", true, col2)); //$NON-NLS-1$ //$NON-NLS-2$
		outputElements.add(new AggregateSymbol("min", "MIN", false, col2)); //$NON-NLS-1$ //$NON-NLS-2$
		outputElements.add(new AggregateSymbol("minDist", "MIN", true, col2)); //$NON-NLS-1$ //$NON-NLS-2$
		outputElements.add(new AggregateSymbol("max", "MAX", false, col2)); //$NON-NLS-1$ //$NON-NLS-2$
		outputElements.add(new AggregateSymbol("maxDist", "MAX", true, col2)); //$NON-NLS-1$ //$NON-NLS-2$
		node.setElements(outputElements);
		
		List groupingElements = new ArrayList();
		groupingElements.add(new ElementSymbol("col1")); //$NON-NLS-1$
		node.setGroupingElements(groupingElements);	  
        CommandContext context = new CommandContext("pid", "test", null, null, null);               //$NON-NLS-1$ //$NON-NLS-2$
        node.initialize(context, mgr, null);
        
        List[] expected = new List[] {
            Arrays.asList(new Object[] { null, new Integer(2), new Integer(1), new Integer(1), new Long(3), new Long(3), new Double(3.0), new Double(3.0), new Integer(3), new Integer(3), new Integer(3), new Integer(3) }),
            Arrays.asList(new Object[] { new Integer(0), new Integer(1), new Integer(1), new Integer(1), new Long(4), new Long(4), new Double(4.0), new Double(4.0), new Integer(4), new Integer(4), new Integer(4), new Integer(4) }),
            Arrays.asList(new Object[] { new Integer(1), new Integer(1), new Integer(1), new Integer(1), new Long(2), new Long(2), new Double(2.0), new Double(2.0), new Integer(2), new Integer(2), new Integer(2), new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(2), new Integer(4), new Integer(4), new Integer(2), new Long(5), new Long(3), new Double(1.25), new Double(1.5), new Integer(1), new Integer(1), new Integer(2), new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(3), new Integer(1), new Integer(1), new Integer(1), new Long(0), new Long(0), new Double(0.0), new Double(0.0), new Integer(0), new Integer(0), new Integer(0), new Integer(0) }),
            Arrays.asList(new Object[] { new Integer(4), new Integer(3), new Integer(2), new Integer(2), new Long(5), new Long(5), new Double(2.5), new Double(2.5), new Integer(2), new Integer(2), new Integer(3), new Integer(3) }),
            Arrays.asList(new Object[] { new Integer(5), new Integer(1), new Integer(1), new Integer(1), new Long(3), new Long(3), new Double(3.0), new Double(3.0), new Integer(3), new Integer(3), new Integer(3), new Integer(3) }),
            Arrays.asList(new Object[] { new Integer(6), new Integer(2), new Integer(2), new Integer(2), new Long(7), new Long(7), new Double(3.5), new Double(3.5), new Integer(3), new Integer(3), new Integer(4), new Integer(4) })
        };
        
        helpProcess(mgr, node, context, expected);
	}

    public void test2() throws Exception {
        BufferManager mgr = BufferManagerFactory.getStandaloneBufferManager();

        GroupingNode node = getExampleGroupingNode();         
        CommandContext context = new CommandContext("pid", "test", null, null, null);               //$NON-NLS-1$ //$NON-NLS-2$
        node.initialize(context, mgr, null);
        
        List[] expected = new List[] {
            Arrays.asList(new Object[] { null, new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(0), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(1), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(3), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(4), new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(5), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(6), new Integer(2) })
        };
                
        helpProcess(mgr, node, context, expected);
    }

    // Same as test2, but uses processor batch size smaller than number of groups
    public void test3() throws Exception {
        BufferManager mgr = BufferManagerFactory.getStandaloneBufferManager();
        ((BufferManagerImpl)mgr).getConfig().setProcessorBatchSize(5);

        GroupingNode node = getExampleGroupingNode();         
        CommandContext context = new CommandContext("pid", "test", null, null,  null);               //$NON-NLS-1$ //$NON-NLS-2$
        node.initialize(context, mgr, null);
        
        List[] expected = new List[] {
            Arrays.asList(new Object[] { null, new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(0), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(1), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(3), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(4), new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(5), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(6), new Integer(2) })
        };
                
        helpProcess(mgr, node, context, expected);
    }
    
    public void testDefect5769() throws Exception {
        BufferManager mgr = BufferManagerFactory.getStandaloneBufferManager();

        ElementSymbol bigDecimal = new ElementSymbol("value"); //$NON-NLS-1$
        bigDecimal.setType(DataTypeManager.DefaultDataClasses.BIG_DECIMAL);        

        // Set up
        GroupingNode node = new GroupingNode(1);        
        List outputElements = new ArrayList();
        outputElements.add(new AggregateSymbol("bigSum", "SUM", false, bigDecimal)); //$NON-NLS-1$ //$NON-NLS-2$
        outputElements.add(new AggregateSymbol("bigAvg", "AVG", false, bigDecimal)); //$NON-NLS-1$ //$NON-NLS-2$
        node.setElements(outputElements);
        
        // Set grouping elements to null 
        node.setGroupingElements(null);         
        CommandContext context = new CommandContext("pid", "test", null, null, null);               //$NON-NLS-1$ //$NON-NLS-2$
        node.initialize(context, mgr, null);
        
        List[] data = new List[] {
            Arrays.asList(new Object[] { new BigDecimal("0.0") }),     //$NON-NLS-1$
            Arrays.asList(new Object[] { new BigDecimal("1.0") }),     //$NON-NLS-1$
            Arrays.asList(new Object[] { new BigDecimal("2.0") }),     //$NON-NLS-1$
            Arrays.asList(new Object[] { new BigDecimal("3.0") }),     //$NON-NLS-1$
            Arrays.asList(new Object[] { new BigDecimal("4.0") }) //$NON-NLS-1$
        };
        
        List[] expected = new List[] {
            Arrays.asList(new Object[] { new BigDecimal("10.0"), new BigDecimal("2.000000000") }) //$NON-NLS-1$ //$NON-NLS-2$
        };
                
        List symbols = new ArrayList();
        symbols.add(bigDecimal);
        TupleSource dataSource = new FakeTupleSource(symbols, data);            
        helpProcess(mgr, node, context, expected, dataSource);
    }

    public void testdefect9842() throws Exception {
        BufferManager mgr = BufferManagerFactory.getStandaloneBufferManager();

        ElementSymbol col1 = new ElementSymbol("col1"); //$NON-NLS-1$
        col1.setType(Integer.class);
        ElementSymbol bigDecimal = new ElementSymbol("value"); //$NON-NLS-1$
        bigDecimal.setType(DataTypeManager.DefaultDataClasses.BIG_DECIMAL);        

        // Set up
        GroupingNode node = new GroupingNode(1);        
        List outputElements = new ArrayList();
        outputElements.add(col1);
        outputElements.add(new AggregateSymbol("bigSum", "SUM", false, bigDecimal)); //$NON-NLS-1$ //$NON-NLS-2$
        outputElements.add(new AggregateSymbol("bigAvg", "AVG", false, bigDecimal)); //$NON-NLS-1$ //$NON-NLS-2$
        node.setElements(outputElements);
        
        // Set grouping elements to null 
        List groupingElements = new ArrayList();
        groupingElements.add(new ElementSymbol("col1")); //$NON-NLS-1$
        node.setGroupingElements(groupingElements);         
        CommandContext context = new CommandContext("pid", "test", null, null, null);               //$NON-NLS-1$ //$NON-NLS-2$
        node.initialize(context, mgr, null);
        
        List[] data = new List[] {
            Arrays.asList(new Object[] { new Integer(1), new BigDecimal("0.0") }),     //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(1), new BigDecimal("1.0") }),     //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(2), new BigDecimal("2.0") }),     //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(2), new BigDecimal("3.0") }),     //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(2), new BigDecimal("4.0") }) //$NON-NLS-1$
        };
        
        List[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(1), new BigDecimal("1.0"), new BigDecimal("0.500000000") }), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] { new Integer(2), new BigDecimal("9.0"), new BigDecimal("3.000000000") }) //$NON-NLS-1$ //$NON-NLS-2$
        };
                
        List symbols = new ArrayList();
        symbols.add(col1);
        symbols.add(bigDecimal);
        TupleSource dataSource = new FakeTupleSource(symbols, data);            
        helpProcess(mgr, node, context, expected, dataSource);
    }

    private void helpTestLookupFunctionInAggregate(int batchSize) throws Exception {
        BufferManager mgr = BufferManagerFactory.getStandaloneBufferManager();
        ((BufferManagerImpl)mgr).getConfig().setProcessorBatchSize(batchSize);

        // Set up
        GroupingNode node = new GroupingNode(1);
        List outputElements = new ArrayList();
        ElementSymbol col1 = new ElementSymbol("col1"); //$NON-NLS-1$
        col1.setType(Integer.class);
        ElementSymbol col2 = new ElementSymbol("col2"); //$NON-NLS-1$
        col2.setType(Integer.class);
        
        Function func = new Function("lookup", new Expression[] { new Constant("pm1.g1"), new Constant("e2"), new Constant("e1"), col2 }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        FunctionDescriptor desc = FunctionLibraryManager.getFunctionLibrary().findFunction("lookup", new Class[] { String.class, String.class, String.class, Integer.class } ); //$NON-NLS-1$
        func.setFunctionDescriptor(desc);
        func.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        
        outputElements.add(col1);
        outputElements.add(new AggregateSymbol("count", "COUNT", false, func)); //$NON-NLS-1$ //$NON-NLS-2$
        outputElements.add(new AggregateSymbol("sum", "SUM", false, func)); //$NON-NLS-1$ //$NON-NLS-2$
        outputElements.add(new AggregateSymbol("sumDist", "SUM", true, func)); //$NON-NLS-1$ //$NON-NLS-2$
        node.setElements(outputElements);
        
        List groupingElements = new ArrayList();
        groupingElements.add(new ElementSymbol("col1")); //$NON-NLS-1$
        node.setGroupingElements(groupingElements);   
        CommandContext context = new CommandContext("pid", "test", null, null, null);    //$NON-NLS-1$ //$NON-NLS-2$
        
        FakeDataManager dataMgr = new FakeDataManager();
        dataMgr.setThrowBlocked(true);
        Map valueMap = new HashMap();
        valueMap.put(new Integer(0), new Integer(1));
        valueMap.put(new Integer(1), new Integer(2));
        valueMap.put(new Integer(2), new Integer(3));
        valueMap.put(new Integer(3), new Integer(4));
        valueMap.put(new Integer(4), new Integer(5));
        dataMgr.defineCodeTable("pm1.g1", "e1", "e2", valueMap); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                   
        node.initialize(context, mgr, dataMgr);
        
        List[] expected = new List[] {
            Arrays.asList(new Object[] { null,           new Integer(1), new Long(4), new Long(4) }),
            Arrays.asList(new Object[] { new Integer(0), new Integer(1), new Long(5), new Long(5) }),
            Arrays.asList(new Object[] { new Integer(1), new Integer(1), new Long(3), new Long(3) }),
            Arrays.asList(new Object[] { new Integer(2), new Integer(4), new Long(9), new Long(5) }),
            Arrays.asList(new Object[] { new Integer(3), new Integer(1), new Long(1), new Long(1) }),
            Arrays.asList(new Object[] { new Integer(4), new Integer(2), new Long(7), new Long(7) }),
            Arrays.asList(new Object[] { new Integer(5), new Integer(1), new Long(4), new Long(4) }),
            Arrays.asList(new Object[] { new Integer(6), new Integer(2), new Long(9), new Long(9) })
        };
        
        helpProcess(mgr, node, context, expected);
    }
    
    public void helpTestEmptyGroup(boolean groupBy) throws Exception {
        BufferManager mgr = BufferManagerFactory.getStandaloneBufferManager();

        ElementSymbol col1 = new ElementSymbol("col1"); //$NON-NLS-1$
        col1.setType(Integer.class);
        ElementSymbol bigDecimal = new ElementSymbol("value"); //$NON-NLS-1$
        bigDecimal.setType(DataTypeManager.DefaultDataClasses.BIG_DECIMAL);        

        // Set up
        GroupingNode node = new GroupingNode(1);        
        List outputElements = new ArrayList();
        outputElements.add(new AggregateSymbol("bigSum", "SUM", false, bigDecimal)); //$NON-NLS-1$ //$NON-NLS-2$
        outputElements.add(new AggregateSymbol("bigAvg", "AVG", false, bigDecimal)); //$NON-NLS-1$ //$NON-NLS-2$
        node.setElements(outputElements);
        
        // Set grouping elements to null 
        if (groupBy) {
            List groupingElements = new ArrayList();
            groupingElements.add(new ElementSymbol("col1")); //$NON-NLS-1$
            node.setGroupingElements(groupingElements);
        }
        CommandContext context = new CommandContext("pid", "test", null, null, null);               //$NON-NLS-1$ //$NON-NLS-2$
        node.initialize(context, mgr, null);
        
        List[] data = new List[] {
        };
        
        List[] expected = new List[] {
            Arrays.asList(new Object[] { null, null })
        };
        
        if (groupBy) {
            expected = new List[] {};
        }
                
        List symbols = new ArrayList();
        symbols.add(col1);
        symbols.add(bigDecimal);
        TupleSource dataSource = new FakeTupleSource(symbols, data);            
        helpProcess(mgr, node, context, expected, dataSource);
    }
    
    public void testTestEmptyGroupWithoutGroupBy() throws Exception {
        helpTestEmptyGroup(false);
    }
    
    public void testTestEmptyGroupWithGroupBy() throws Exception {
        helpTestEmptyGroup(true);
    }

    public void testLookupFunctionMultipleBatches() throws Exception {
        helpTestLookupFunctionInAggregate(3);
    }
    
    public void testDupSort() throws Exception {
        BufferManager mgr = BufferManagerFactory.getStandaloneBufferManager();

        GroupingNode node = getExampleGroupingNode();     
        node.setRemoveDuplicates(true);
        CommandContext context = new CommandContext("pid", "test", null, null,  null);               //$NON-NLS-1$ //$NON-NLS-2$
        node.initialize(context, mgr, null);
        
        List[] expected = new List[] {
            Arrays.asList(new Object[] { null, new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(0), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(1), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(3), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(4), new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(5), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(6), new Integer(2) })
        };
                
        helpProcess(mgr, node, context, expected);
    }

	private GroupingNode getExampleGroupingNode() {
		GroupingNode node = new GroupingNode(1);
        List outputElements = new ArrayList();
        ElementSymbol col1 = new ElementSymbol("col1"); //$NON-NLS-1$
        col1.setType(Integer.class);
        ElementSymbol col2 = new ElementSymbol("col2"); //$NON-NLS-1$
        col2.setType(Integer.class);
        outputElements.add(col1);
        outputElements.add(new AggregateSymbol("countDist", "COUNT", true, col2)); //$NON-NLS-1$ //$NON-NLS-2$
        node.setElements(outputElements);
        
        List groupingElements = new ArrayList();
        groupingElements.add(new ElementSymbol("col1")); //$NON-NLS-1$
        node.setGroupingElements(groupingElements);
		return node;
	}
    
}
