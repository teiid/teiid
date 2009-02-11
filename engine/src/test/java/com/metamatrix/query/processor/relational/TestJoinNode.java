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
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.function.FunctionDescriptor;
import com.metamatrix.query.function.FunctionLibraryManager;
import com.metamatrix.query.processor.FakeDataManager;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.util.CommandContext;

public class TestJoinNode extends TestCase {
    private static final int NO_CRITERIA = 0;
    private static final int EQUAL_CRITERIA = 1;
    private static final int FUNCTION_CRITERIA = 2;
    
    private int criteriaType = EQUAL_CRITERIA;
    
    private JoinType joinType;
    
    private List[] leftTuples = createTuples1();
    private List[] rightTuples = createTuples2();
    
    private List[] expected;
    private List[] expectedReversed;
    
    private boolean expectSwap = false;
    private boolean expectSwapReversed = false;
    
    private JoinNode join;
    private JoinStrategy joinStrategy;
    private RelationalNode leftNode;
    private RelationalNode rightNode;

    private FakeDataManager dataMgr;

    public TestJoinNode(String testName) {
        super(testName);
    }
    
    protected int getProcessorBatchSize() {
        return 100;
    }
    
    protected List[] createTuples1() {
        return new List[] { 
            Arrays.asList(new Object[] { new Integer(5) }),    
            Arrays.asList(new Object[] { new Integer(3) }),    
            Arrays.asList(new Object[] { new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(10) }),    
            Arrays.asList(new Object[] { new Integer(11) }),    
            Arrays.asList(new Object[] { new Integer(11) })
        };    
    }

    private List[] createTuples2() {
        return new List[] { 
            Arrays.asList(new Object[] { new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(4) }),    
            Arrays.asList(new Object[] { null }),    
            Arrays.asList(new Object[] { new Integer(7) }),    
            Arrays.asList(new Object[] { new Integer(7) }),    
            Arrays.asList(new Object[] { new Integer(6) })
        };    
    }
    
    private List[] createTuples3() {
        return new List[] { 
           Arrays.asList(new Object[] { null }),  
           Arrays.asList(new Object[] { null }),  
           Arrays.asList(new Object[] { new Integer(10) }),    
           Arrays.asList(new Object[] { new Integer(10) }),    
           Arrays.asList(new Object[] { new Integer(9) }),    
           Arrays.asList(new Object[] { new Integer(9) }),    
           Arrays.asList(new Object[] { new Integer(9) }),    
           Arrays.asList(new Object[] { null }),    
           Arrays.asList(new Object[] { new Integer(1) }),    
           Arrays.asList(new Object[] { new Integer(2) }),    
           Arrays.asList(new Object[] { new Integer(3) }),
           Arrays.asList(new Object[] { new Integer(5) }),
           Arrays.asList(new Object[] { new Integer(15) })
       };    
   }
    
    private List[] createTuples4() {
        return new List[] { 
           Arrays.asList(new Object[] { new Integer(1) }),    
           Arrays.asList(new Object[] { new Integer(4) }),    
           Arrays.asList(new Object[] { new Integer(2) }),    
           Arrays.asList(new Object[] { new Integer(2) }),    
           Arrays.asList(new Object[] { new Integer(4) }),    
           Arrays.asList(new Object[] { null }),    
           Arrays.asList(new Object[] { new Integer(7) }),    
           Arrays.asList(new Object[] { new Integer(9) }),    
           Arrays.asList(new Object[] { new Integer(5) }),
           Arrays.asList(new Object[] { new Integer(6) }),
           Arrays.asList(new Object[] { new Integer(10) }),
           Arrays.asList(new Object[] { null }),
           Arrays.asList(new Object[] { null })
       };    
   }

    private List[] createTuples(int startingValue, int count) {
        return createTuples(startingValue, count, false);
    }
    
    private List[] createTuples(int startingValue, int count, boolean addNullAsSecondColumn) {
        ArrayList lists = new ArrayList();
        for (int i=0; i<count; i++) {
            Object[] tuple;
            if (addNullAsSecondColumn) {
                tuple = new Object[] {new Integer(startingValue + i), null };
            } else {
                tuple = new Object[] {new Integer(startingValue + i) };
            }
            lists.add( Arrays.asList(tuple) );
        }
        return (List[]) lists.toArray(new List[] {});
    }

    private List[] createResults(int startingValue, int count) {
        return createTuples(startingValue, count, true);
    }
    
    private void helpCreateJoin() {
        ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        
        ElementSymbol es2 = new ElementSymbol("e2"); //$NON-NLS-1$
        es2.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        
        List leftElements = new ArrayList();
        leftElements.add(es1);
        leftNode = new FakeRelationalNode(1, leftTuples);
        leftNode.setElements(leftElements);
        
        List rightElements = new ArrayList();
        rightElements.add(es2);
        rightNode = new FakeRelationalNode(2, rightTuples);
        rightNode.setElements(rightElements);
        
        List joinElements = new ArrayList();
        joinElements.add(es1);
        joinElements.add(es2);
        
        join = new JoinNode(3);
        joinStrategy = new NestedLoopJoinStrategy();
        join.setJoinStrategy(joinStrategy);
        join.setElements(joinElements);
        join.setJoinType(joinType);
        
        switch (criteriaType) {
            case NO_CRITERIA :
                break;

            case EQUAL_CRITERIA :
                join.setJoinCriteria(new CompareCriteria(es1, CompareCriteria.EQ, es2));
                break;

            case FUNCTION_CRITERIA :
                Function func = new Function("lookup", new Expression[] { new Constant("pm1.g1"), new Constant("e2"), new Constant("e1"), es1 }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                FunctionDescriptor desc = FunctionLibraryManager.getFunctionLibrary().findFunction("lookup", new Class[] { String.class, String.class, String.class, Integer.class }); //$NON-NLS-1$
                func.setFunctionDescriptor(desc);
                func.setType(DataTypeManager.DefaultDataClasses.INTEGER);
                CompareCriteria joinCriteria = new CompareCriteria(es2, CompareCriteria.EQ, func);
                join.setJoinCriteria(joinCriteria);
                break;
        }
    }
        
    public void helpTestJoin() throws MetaMatrixComponentException, MetaMatrixProcessingException {
        helpTestJoinDirect(expected, expectSwap);
        helpTestJoinReversed();
    }

    private void helpTestJoinReversed() throws MetaMatrixComponentException, MetaMatrixProcessingException {
        List[] temp = leftTuples;
        leftTuples = rightTuples;
        rightTuples = temp;
        helpTestJoinDirect(expectedReversed, expectSwapReversed);        
    }
    
    public void helpTestJoinDirect(List[] expectedResults, boolean swapExpected) throws MetaMatrixComponentException, MetaMatrixProcessingException {
        helpCreateJoin();                
        BufferManager mgr = NodeTestUtil.getTestBufferManager(1, getProcessorBatchSize());
        CommandContext context = new CommandContext("pid", "test", null, 100, null, null, null, null);               //$NON-NLS-1$ //$NON-NLS-2$
        
        join.addChild(leftNode);
        join.addChild(rightNode);
        
        leftNode.initialize(context, mgr, dataMgr);
        rightNode.initialize(context, mgr, dataMgr);
        join.initialize(context, mgr, dataMgr);
        
        join.open();
        
        int currentRow = 1;
        while(true) {
            try {
                TupleBatch batch = join.nextBatch();
                for(int row = currentRow; row <= batch.getEndRow(); row++) {
                    List tuple = batch.getTuple(row);
                    //System.out.println(tuple);
                    assertEquals("Rows don't match at " + row, expectedResults[row-1], tuple); //$NON-NLS-1$
                }
                
                if(batch.getTerminationFlag()) {
                    break;
                }
                currentRow += batch.getRowCount();    
            } catch(BlockedException e) {
                // ignore and retry
            }
        }
        
        join.close(); 
    }
    
    public void testNoRows() throws Exception {
        leftTuples = new List[0];
        rightTuples = new List[0];
        joinType = JoinType.JOIN_INNER;
        expected = new List[0];
        expectedReversed = new List[0];
        helpTestJoin();        
    }

    public void testInnerJoin() throws Exception {
        joinType = JoinType.JOIN_INNER;
        expected = new List[] {
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(1), new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) })            
        };
        expectedReversed = new List[] {
            Arrays.asList(new Object[] { new Integer(1), new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),            
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) })    
        };
        helpTestJoin();        
    }

//    public void testRightOuterJoin() {
//        joinType = JoinType.JOIN_RIGHT_OUTER;
//        expected = new List[] {
//            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),    
//            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),    
//            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
//            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
//            Arrays.asList(new Object[] { new Integer(1), new Integer(1) }),    
//            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
//            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),            
//            Arrays.asList(new Object[] { null, null }),    
//            Arrays.asList(new Object[] { null, new Integer(7) }),    
//            Arrays.asList(new Object[] { null, new Integer(7) }),    
//            Arrays.asList(new Object[] { null, new Integer(6) })            
//        };
//        expectedReversed = new List[] {
//            Arrays.asList(new Object[] { new Integer(1), new Integer(1) }),    
//            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
//            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),            
//            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),    
//            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),    
//            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
//            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),   
//            Arrays.asList(new Object[] { null, new Integer(5) }),    
//            Arrays.asList(new Object[] { null, new Integer(3) }),
//            Arrays.asList(new Object[] { null, new Integer(10) }),            
//            Arrays.asList(new Object[] { null, new Integer(11) }),    
//            Arrays.asList(new Object[] { null, new Integer(11) })           
//        };
//        helpTestJoin();        
//    }

    public void testLeftOuterJoin() throws Exception {
        joinType = JoinType.JOIN_LEFT_OUTER;
        expected = new List[] {
            Arrays.asList(new Object[] { new Integer(5), null }),    
            Arrays.asList(new Object[] { new Integer(3), null }),    
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(1), new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),            
            Arrays.asList(new Object[] { new Integer(10), null }),            
            Arrays.asList(new Object[] { new Integer(11), null }),    
            Arrays.asList(new Object[] { new Integer(11), null })            
        };
        expectedReversed = new List[] {
            Arrays.asList(new Object[] { new Integer(1), new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),            
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
            Arrays.asList(new Object[] { null, null }),    
            Arrays.asList(new Object[] { new Integer(7), null }),    
            Arrays.asList(new Object[] { new Integer(7), null }),    
            Arrays.asList(new Object[] { new Integer(6), null })            
        };
        helpTestJoin();        
    }    

    public void testLeftOuterJoinWithSwap() throws Exception {
        int outerSize = getProcessorBatchSize() + 1;
        leftTuples = createTuples(1,outerSize);
        rightTuples = createTuples(201,outerSize+1);
        joinType = JoinType.JOIN_LEFT_OUTER;
        expectSwap = true;
        expected = createResults(1, outerSize);
        expectedReversed = createResults(201, outerSize+1);
        helpTestJoin(); 
    }    

    public void testCrossJoin() throws Exception {
        joinType = JoinType.JOIN_CROSS;
        criteriaType = NO_CRITERIA;
        expected = new List[] {
            Arrays.asList(new Object[] { new Integer(5), new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(5), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(5), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(5), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(5), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(5), null }),    
            Arrays.asList(new Object[] { new Integer(5), new Integer(7) }),    
            Arrays.asList(new Object[] { new Integer(5), new Integer(7) }),    
            Arrays.asList(new Object[] { new Integer(5), new Integer(6) }), 
            Arrays.asList(new Object[] { new Integer(3), new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(3), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(3), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(3), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(3), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(3), null }),    
            Arrays.asList(new Object[] { new Integer(3), new Integer(7) }),    
            Arrays.asList(new Object[] { new Integer(3), new Integer(7) }),    
            Arrays.asList(new Object[] { new Integer(3), new Integer(6) }),    
            Arrays.asList(new Object[] { new Integer(2), new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(2), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(2), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(2), null }),    
            Arrays.asList(new Object[] { new Integer(2), new Integer(7) }),    
            Arrays.asList(new Object[] { new Integer(2), new Integer(7) }),    
            Arrays.asList(new Object[] { new Integer(2), new Integer(6) }),                   
            Arrays.asList(new Object[] { new Integer(4), new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(4), null }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(7) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(7) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(6) }),    
            Arrays.asList(new Object[] { new Integer(1), new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(1), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(1), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(1), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(1), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(1), null }),    
            Arrays.asList(new Object[] { new Integer(1), new Integer(7) }),    
            Arrays.asList(new Object[] { new Integer(1), new Integer(7) }),    
            Arrays.asList(new Object[] { new Integer(1), new Integer(6) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(4), null }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(7) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(7) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(6) }),    
            Arrays.asList(new Object[] { new Integer(10), new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(10), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(10), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(10), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(10), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(10), null }),    
            Arrays.asList(new Object[] { new Integer(10), new Integer(7) }),    
            Arrays.asList(new Object[] { new Integer(10), new Integer(7) }),    
            Arrays.asList(new Object[] { new Integer(10), new Integer(6) }),
            Arrays.asList(new Object[] { new Integer(11), new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(11), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(11), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(11), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(11), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(11), null }),    
            Arrays.asList(new Object[] { new Integer(11), new Integer(7) }),    
            Arrays.asList(new Object[] { new Integer(11), new Integer(7) }),    
            Arrays.asList(new Object[] { new Integer(11), new Integer(6) }),
            Arrays.asList(new Object[] { new Integer(11), new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(11), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(11), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(11), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(11), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(11), null }),    
            Arrays.asList(new Object[] { new Integer(11), new Integer(7) }),    
            Arrays.asList(new Object[] { new Integer(11), new Integer(7) }),    
            Arrays.asList(new Object[] { new Integer(11), new Integer(6) })
        };   
        expectedReversed = new List[] {
            Arrays.asList(new Object[] { new Integer(1), new Integer(5)}),
            Arrays.asList(new Object[] { new Integer(1), new Integer(3)}),
            Arrays.asList(new Object[] { new Integer(1), new Integer(2)}),
            Arrays.asList(new Object[] { new Integer(1), new Integer(4)}),
            Arrays.asList(new Object[] { new Integer(1), new Integer(1)}),
            Arrays.asList(new Object[] { new Integer(1), new Integer(4)}),
            Arrays.asList(new Object[] { new Integer(1), new Integer(10)}),
            Arrays.asList(new Object[] { new Integer(1), new Integer(11)}),
            Arrays.asList(new Object[] { new Integer(1), new Integer(11)}),
            
            Arrays.asList(new Object[] { new Integer(4), new Integer(5)}),
            Arrays.asList(new Object[] { new Integer(4), new Integer(3)}),
            Arrays.asList(new Object[] { new Integer(4), new Integer(2)}),
            Arrays.asList(new Object[] { new Integer(4), new Integer(4)}),
            Arrays.asList(new Object[] { new Integer(4), new Integer(1)}),
            Arrays.asList(new Object[] { new Integer(4), new Integer(4)}),
            Arrays.asList(new Object[] { new Integer(4), new Integer(10)}),
            Arrays.asList(new Object[] { new Integer(4), new Integer(11)}),
            Arrays.asList(new Object[] { new Integer(4), new Integer(11)}),

            Arrays.asList(new Object[] { new Integer(2), new Integer(5)}),
            Arrays.asList(new Object[] { new Integer(2), new Integer(3)}),
            Arrays.asList(new Object[] { new Integer(2), new Integer(2)}),
            Arrays.asList(new Object[] { new Integer(2), new Integer(4)}),
            Arrays.asList(new Object[] { new Integer(2), new Integer(1)}),
            Arrays.asList(new Object[] { new Integer(2), new Integer(4)}),
            Arrays.asList(new Object[] { new Integer(2), new Integer(10)}),
            Arrays.asList(new Object[] { new Integer(2), new Integer(11)}),
            Arrays.asList(new Object[] { new Integer(2), new Integer(11)}),

            Arrays.asList(new Object[] { new Integer(2), new Integer(5)}),
            Arrays.asList(new Object[] { new Integer(2), new Integer(3)}),
            Arrays.asList(new Object[] { new Integer(2), new Integer(2)}),
            Arrays.asList(new Object[] { new Integer(2), new Integer(4)}),
            Arrays.asList(new Object[] { new Integer(2), new Integer(1)}),
            Arrays.asList(new Object[] { new Integer(2), new Integer(4)}),
            Arrays.asList(new Object[] { new Integer(2), new Integer(10)}),
            Arrays.asList(new Object[] { new Integer(2), new Integer(11)}),
            Arrays.asList(new Object[] { new Integer(2), new Integer(11)}),
                        
            Arrays.asList(new Object[] { new Integer(4), new Integer(5)}),
            Arrays.asList(new Object[] { new Integer(4), new Integer(3)}),
            Arrays.asList(new Object[] { new Integer(4), new Integer(2)}),
            Arrays.asList(new Object[] { new Integer(4), new Integer(4)}),
            Arrays.asList(new Object[] { new Integer(4), new Integer(1)}),
            Arrays.asList(new Object[] { new Integer(4), new Integer(4)}),
            Arrays.asList(new Object[] { new Integer(4), new Integer(10)}),
            Arrays.asList(new Object[] { new Integer(4), new Integer(11)}),
            Arrays.asList(new Object[] { new Integer(4), new Integer(11)}),
                        
            Arrays.asList(new Object[] { null, new Integer(5)}),
            Arrays.asList(new Object[] { null, new Integer(3)}),
            Arrays.asList(new Object[] { null, new Integer(2)}),
            Arrays.asList(new Object[] { null, new Integer(4)}),
            Arrays.asList(new Object[] { null, new Integer(1)}),
            Arrays.asList(new Object[] { null, new Integer(4)}),
            Arrays.asList(new Object[] { null, new Integer(10)}),
            Arrays.asList(new Object[] { null, new Integer(11)}),
            Arrays.asList(new Object[] { null, new Integer(11)}),

            Arrays.asList(new Object[] { new Integer(7), new Integer(5)}),
            Arrays.asList(new Object[] { new Integer(7), new Integer(3)}),
            Arrays.asList(new Object[] { new Integer(7), new Integer(2)}),
            Arrays.asList(new Object[] { new Integer(7), new Integer(4)}),
            Arrays.asList(new Object[] { new Integer(7), new Integer(1)}),
            Arrays.asList(new Object[] { new Integer(7), new Integer(4)}),
            Arrays.asList(new Object[] { new Integer(7), new Integer(10)}),
            Arrays.asList(new Object[] { new Integer(7), new Integer(11)}),
            Arrays.asList(new Object[] { new Integer(7), new Integer(11)}),

            Arrays.asList(new Object[] { new Integer(7), new Integer(5)}),
            Arrays.asList(new Object[] { new Integer(7), new Integer(3)}),
            Arrays.asList(new Object[] { new Integer(7), new Integer(2)}),
            Arrays.asList(new Object[] { new Integer(7), new Integer(4)}),
            Arrays.asList(new Object[] { new Integer(7), new Integer(1)}),
            Arrays.asList(new Object[] { new Integer(7), new Integer(4)}),
            Arrays.asList(new Object[] { new Integer(7), new Integer(10)}),
            Arrays.asList(new Object[] { new Integer(7), new Integer(11)}),
            Arrays.asList(new Object[] { new Integer(7), new Integer(11)}),
                        
            Arrays.asList(new Object[] { new Integer(6), new Integer(5)}),
            Arrays.asList(new Object[] { new Integer(6), new Integer(3)}),
            Arrays.asList(new Object[] { new Integer(6), new Integer(2)}),
            Arrays.asList(new Object[] { new Integer(6), new Integer(4)}),
            Arrays.asList(new Object[] { new Integer(6), new Integer(1)}),
            Arrays.asList(new Object[] { new Integer(6), new Integer(4)}),
            Arrays.asList(new Object[] { new Integer(6), new Integer(10)}),
            Arrays.asList(new Object[] { new Integer(6), new Integer(11)}),
            Arrays.asList(new Object[] { new Integer(6), new Integer(11)})
    };   

        helpTestJoin();
    }
    
    public void testInnerJoinWithLookupFunction() throws Exception {
        criteriaType = FUNCTION_CRITERIA;
        joinType = JoinType.JOIN_INNER;
        expected = new List[] {
                Arrays.asList(new Object[] { new Integer(5), new Integer(6) }),    
                Arrays.asList(new Object[] { new Integer(3), new Integer(4) }),    
                Arrays.asList(new Object[] { new Integer(3), new Integer(4) }),    
                Arrays.asList(new Object[] { new Integer(1), new Integer(2) }),    
                Arrays.asList(new Object[] { new Integer(1), new Integer(2) }),    
                Arrays.asList(new Object[] { new Integer(10), new Integer(7) }),    
                Arrays.asList(new Object[] { new Integer(10), new Integer(7) })            
                };
        expectedReversed = new List[] {
            Arrays.asList(new Object[] { new Integer(1), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(5) }),    
            Arrays.asList(new Object[] { new Integer(2), new Integer(3) }),    
            Arrays.asList(new Object[] { new Integer(2), new Integer(3) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(5) }),    
            Arrays.asList(new Object[] { new Integer(10), new Integer(7) }),    
            Arrays.asList(new Object[] { new Integer(10), new Integer(7) })            
                };

        dataMgr = new FakeDataManager();
        dataMgr.setThrowBlocked(true);
        Map valueMap = new HashMap();
        valueMap.put(new Integer(1), new Integer(2));
        valueMap.put(new Integer(2), new Integer(3));
        valueMap.put(new Integer(3), new Integer(4));
        valueMap.put(new Integer(4), new Integer(5));
        valueMap.put(new Integer(5), new Integer(6));
        valueMap.put(new Integer(10), new Integer(7));
        valueMap.put(new Integer(11), new Integer(8));
        dataMgr.defineCodeTable("pm1.g1", "e1", "e2", valueMap);         //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                
        helpTestJoin();        
    }    
    
    public void testFullOuterJoin() throws Exception {
        this.joinType = JoinType.JOIN_FULL_OUTER;
        this.leftTuples = createTuples3();
        this.rightTuples = createTuples4();
        expected = new List[] {
           Arrays.asList(new Object[] { null, null }),  
           Arrays.asList(new Object[] { null, null }),  
           Arrays.asList(new Object[] { new Integer(10), new Integer(10) }),    
           Arrays.asList(new Object[] { new Integer(10), new Integer(10) }),    
           Arrays.asList(new Object[] { new Integer(9), new Integer(9) }),
           Arrays.asList(new Object[] { new Integer(9), new Integer(9) }),
           Arrays.asList(new Object[] { new Integer(9), new Integer(9) }),
           Arrays.asList(new Object[] { null, null }), 
           Arrays.asList(new Object[] { new Integer(1), new Integer(1) }), 
           Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),
           Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),
           Arrays.asList(new Object[] { new Integer(3), null }),
           Arrays.asList(new Object[] { new Integer(5), new Integer(5) }),
           Arrays.asList(new Object[] { new Integer(15), null }),
           Arrays.asList(new Object[] { null, new Integer(4) }),
           Arrays.asList(new Object[] { null, new Integer(4) }),
           Arrays.asList(new Object[] { null, null }),
           Arrays.asList(new Object[] { null, new Integer(7) }),
           Arrays.asList(new Object[] { null, new Integer(6) }),
           Arrays.asList(new Object[] { null, null }),
           Arrays.asList(new Object[] { null, null })
        };
        
        helpTestJoinDirect(expected, false);
    }
}
