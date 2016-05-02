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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.processor.FakeDataManager;
import org.teiid.query.processor.relational.MergeJoinStrategy.SortOption;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings({"unchecked", "rawtypes"})
public class TestJoinNode {
    private static final int NO_CRITERIA = 0;
    private static final int EQUAL_CRITERIA = 1;
    private static final int FUNCTION_CRITERIA = 2;
    
    private int criteriaType = EQUAL_CRITERIA;
    
    protected JoinType joinType;
    
    protected List[] leftTuples;
    protected List[] rightTuples;
    
    protected List[] expected;
    private List[] expectedReversed;
    
    protected JoinNode join;
    protected JoinStrategy joinStrategy;
    private BlockingFakeRelationalNode leftNode;
    private RelationalNode rightNode;
    
    private FakeDataManager dataMgr;

    @Before public void setup() {
    	leftTuples = createTuples1();
    	rightTuples = createTuples2();
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
    
    protected void helpCreateJoin() {
        ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        
        ElementSymbol es2 = new ElementSymbol("e2"); //$NON-NLS-1$
        es2.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        
        List leftElements = new ArrayList();
        leftElements.add(es1);
        leftNode = new BlockingFakeRelationalNode(1, leftTuples);
        leftNode.setElements(leftElements);
        
        List rightElements = new ArrayList();
        rightElements.add(es2);
        rightNode = new BlockingFakeRelationalNode(2, rightTuples) {
        	public boolean hasBuffer(boolean requireFinal) {
        		return false;
        	}

        	public TupleBuffer getBuffer(int maxRows) throws BlockedException, TeiidComponentException, TeiidProcessingException {
        		fail();
        		throw new AssertionError();
        	};
        };
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
                join.setJoinExpressions(Arrays.asList(es1), Arrays.asList(es2));
                joinStrategy = new MergeJoinStrategy(SortOption.SORT, SortOption.SORT, false);
                join.setJoinStrategy(joinStrategy);
                break;

            case FUNCTION_CRITERIA :
                Function func = new Function("lookup", new Expression[] { new Constant("pm1.g1"), new Constant("e2"), new Constant("e1"), es1 }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                FunctionDescriptor desc = RealMetadataFactory.SFM.getSystemFunctionLibrary().findFunction("lookup", new Class[] { String.class, String.class, String.class, Integer.class }); //$NON-NLS-1$
                func.setFunctionDescriptor(desc);
                func.setType(DataTypeManager.DefaultDataClasses.INTEGER);
                CompareCriteria joinCriteria = new CompareCriteria(es2, CompareCriteria.EQ, func);
                join.setJoinCriteria(joinCriteria);
                break;
        }
    }
        
    public void helpTestJoin() throws TeiidComponentException, TeiidProcessingException {
    	for (int batchSize : new int[] {1, 10, leftTuples.length, 100}) {
	        helpCreateJoin();        
	        if (batchSize == 0) {
	        	continue;
	        }
	        helpTestJoinDirect(expected, batchSize, 100000);
	        List[] temp = leftTuples;
	        leftTuples = rightTuples;
	        rightTuples = temp;
	        helpCreateJoin();                
	        helpTestJoinDirect(expectedReversed, batchSize, 100000);
	        temp = leftTuples;
	        leftTuples = rightTuples;
	        rightTuples = temp;
    	}
    }
    
    public void helpTestJoinDirect(List[] expectedResults, int batchSize, int processingBytes) throws TeiidComponentException, TeiidProcessingException {
        BufferManagerImpl mgr = BufferManagerFactory.getTestBufferManager(processingBytes, batchSize);
        mgr.setTargetBytesPerRow(100);
        CommandContext context = new CommandContext("pid", "test", null, null, 1);               //$NON-NLS-1$ //$NON-NLS-2$
        
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
                for(;currentRow <= batch.getEndRow(); currentRow++) {
                    List tuple = batch.getTuple(currentRow);
                    assertEquals("Rows don't match at " + currentRow, expectedResults[currentRow-1], tuple); //$NON-NLS-1$
                }
                if(batch.getTerminationFlag()) {
                    break;
                }
            } catch(BlockedException e) {
                // ignore and retry
            }
        }
        assertEquals(expectedResults.length, currentRow - 1);
        join.close(); 
    }
    
    @Test public void testNoRows() throws Exception {
        leftTuples = new List[0];
        rightTuples = new List[0];
        joinType = JoinType.JOIN_INNER;
        expected = new List[0];
        expectedReversed = new List[0];
        helpTestJoin();        
    }

    @Test public void testInnerJoin() throws Exception {
        joinType = JoinType.JOIN_INNER;
        expected = new List[] {
        	Arrays.asList(new Object[] { new Integer(1), new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) })            
        };
        expectedReversed = expected;
        helpTestJoin();        
    }

    @Test public void testLeftOuterJoin() throws Exception {
        joinType = JoinType.JOIN_LEFT_OUTER;
        expected = new List[] {
            Arrays.asList(new Object[] { new Integer(1), new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(3), null }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),            
            Arrays.asList(new Object[] { new Integer(5), null }),    
            Arrays.asList(new Object[] { new Integer(10), null }),            
            Arrays.asList(new Object[] { new Integer(11), null }),    
            Arrays.asList(new Object[] { new Integer(11), null })            
        };
        expectedReversed = new List[] {
        	Arrays.asList(new Object[] { null, null }),    
            Arrays.asList(new Object[] { new Integer(1), new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),            
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(4), new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(6), null }),
            Arrays.asList(new Object[] { new Integer(7), null }),    
            Arrays.asList(new Object[] { new Integer(7), null })    
        };
        helpTestJoin();        
    }    

    @Test public void testLeftOuterJoinWithSwap() throws Exception {
        int outerSize = 11;
        leftTuples = createTuples(1,outerSize);
        rightTuples = createTuples(201,outerSize+1);
        joinType = JoinType.JOIN_LEFT_OUTER;
        expected = createResults(1, outerSize);
        expectedReversed = createResults(201, outerSize+1);
        helpTestJoin(); 
    }    

    @Test public void testCrossJoin() throws Exception {
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
    
    @Test public void testInnerJoinWithLookupFunction() throws Exception {
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
            Arrays.asList(new Object[] { new Integer(4), new Integer(5) })    
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
    
    @Test public void testFullOuterJoin() throws Exception {
        this.joinType = JoinType.JOIN_FULL_OUTER;
        this.leftTuples = createTuples3();
        this.rightTuples = createTuples4();
        expected = new List[] {
           Arrays.asList(new Object[] { null, null }),  
           Arrays.asList(new Object[] { null, null }),  
           Arrays.asList(new Object[] { null, null }), 
           Arrays.asList(new Object[] { null, null }),
           Arrays.asList(new Object[] { null, null }),
           Arrays.asList(new Object[] { null, null }),
           Arrays.asList(new Object[] { new Integer(1), new Integer(1) }), 
           Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),
           Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),
           Arrays.asList(new Object[] { new Integer(3), null }),
           Arrays.asList(new Object[] { null, new Integer(4) }),
           Arrays.asList(new Object[] { null, new Integer(4) }),
           Arrays.asList(new Object[] { new Integer(5), new Integer(5) }),
           Arrays.asList(new Object[] { null, new Integer(6) }),
           Arrays.asList(new Object[] { null, new Integer(7) }),
           Arrays.asList(new Object[] { new Integer(9), new Integer(9) }),
           Arrays.asList(new Object[] { new Integer(9), new Integer(9) }),
           Arrays.asList(new Object[] { new Integer(9), new Integer(9) }),
           Arrays.asList(new Object[] { new Integer(10), new Integer(10) }),    
           Arrays.asList(new Object[] { new Integer(10), new Integer(10) }),    
           Arrays.asList(new Object[] { new Integer(15), null })
        };
        expectedReversed = new List[] {
            Arrays.asList(new Object[] { null, null }),  
            Arrays.asList(new Object[] { null, null }),  
            Arrays.asList(new Object[] { null, null }), 
            Arrays.asList(new Object[] { null, null }),
            Arrays.asList(new Object[] { null, null }),
            Arrays.asList(new Object[] { null, null }),
            Arrays.asList(new Object[] { new Integer(1), new Integer(1) }), 
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),
            Arrays.asList(new Object[] { null, 3 }),
            Arrays.asList(new Object[] { 4, null }),
            Arrays.asList(new Object[] { 4, null }),
            Arrays.asList(new Object[] { new Integer(5), new Integer(5) }),
            Arrays.asList(new Object[] { 6, null }),
            Arrays.asList(new Object[] { 7, null }),
            Arrays.asList(new Object[] { new Integer(9), new Integer(9) }),
            Arrays.asList(new Object[] { new Integer(9), new Integer(9) }),
            Arrays.asList(new Object[] { new Integer(9), new Integer(9) }),
            Arrays.asList(new Object[] { new Integer(10), new Integer(10) }),    
            Arrays.asList(new Object[] { new Integer(10), new Integer(10) }),    
            Arrays.asList(new Object[] { null, 15 })
        };
        helpTestJoin();
    }
    
    @Test public void testMergeJoinOptimization() throws Exception {
        helpTestEnhancedSortMergeJoin(99, false);
    }

	private void helpTestEnhancedSortMergeJoin(int batchSize, boolean repeated)
			throws TeiidComponentException, TeiidProcessingException {
		this.joinType = JoinType.JOIN_INNER;
        int rows = 100;
        List[] data = new List[rows];
        for(int i=0; i<rows; i++) { 
            data[i] = new ArrayList();
            Integer value = new Integer((i*17) % 47);
            data[i].add(value);
        }
        this.leftTuples = data;
        this.rightTuples = createTuples2();
        if (!repeated) {
        	expected = new List[] {
                Arrays.asList(new Object[] { 4, 4 }),
                Arrays.asList(new Object[] { 4, 4 }),
                Arrays.asList(new Object[] { 7, 7 }),
                Arrays.asList(new Object[] { 7, 7 }),
                Arrays.asList(new Object[] { 2, 2 }),
                Arrays.asList(new Object[] { 2, 2 }),
                Arrays.asList(new Object[] { 6, 6 }),
                Arrays.asList(new Object[] { 1, 1 }),  
                Arrays.asList(new Object[] { 4, 4 }),
                Arrays.asList(new Object[] { 4, 4 }),
                Arrays.asList(new Object[] { 7, 7 }),
                Arrays.asList(new Object[] { 7, 7 }),
                Arrays.asList(new Object[] { 2, 2 }),
                Arrays.asList(new Object[] { 2, 2 }),
                Arrays.asList(new Object[] { 6, 6 }),
                Arrays.asList(new Object[] { 1, 1 }),
                Arrays.asList(new Object[] { 4, 4 }),
                Arrays.asList(new Object[] { 4, 4 }),
             };
        } else {
	        expected = new List[] {
	           Arrays.asList(new Object[] { 4, 4 }),
	           Arrays.asList(new Object[] { 4, 4 }),
	           Arrays.asList(new Object[] { 1, 1 }),  
	           Arrays.asList(new Object[] { 6, 6 }),
	           Arrays.asList(new Object[] { 2, 2 }),
	           Arrays.asList(new Object[] { 2, 2 }),
	           Arrays.asList(new Object[] { 7, 7 }),
	           Arrays.asList(new Object[] { 7, 7 }),
	           Arrays.asList(new Object[] { 4, 4 }),
	           Arrays.asList(new Object[] { 4, 4 }),
	           Arrays.asList(new Object[] { 1, 1 }),
	           Arrays.asList(new Object[] { 6, 6 }),
	           Arrays.asList(new Object[] { 2, 2 }),
	           Arrays.asList(new Object[] { 2, 2 }),
	           Arrays.asList(new Object[] { 7, 7 }),
	           Arrays.asList(new Object[] { 7, 7 }),
	           Arrays.asList(new Object[] { 4, 4 }),
	           Arrays.asList(new Object[] { 4, 4 }),
	        };
        }
        helpCreateJoin();               
        this.joinStrategy = new EnhancedSortMergeJoinStrategy(SortOption.SORT, SortOption.SORT);
        this.join.setJoinStrategy(joinStrategy);
        helpTestJoinDirect(expected, batchSize, 1);
	}
	
	@Test public void testMergeJoinOptimizationLeftOuter() throws Exception {
		this.joinType = JoinType.JOIN_LEFT_OUTER;
        int rows = 12;
        List[] data = new List[rows];
        for(int i=0; i<rows; i++) { 
            data[i] = new ArrayList();
            Integer value = new Integer((i*17) % 45);
            data[i].add(value);
        }
        this.leftTuples = data;
        this.rightTuples = createTuples2();
        expected = new List[] {
           Arrays.asList(new Object[] { 0, null }),
           Arrays.asList(new Object[] {17, null }),
           Arrays.asList(new Object[] {34, null }),
           Arrays.asList(new Object[] { 6, 6 }),
           Arrays.asList(new Object[] {23, null }),
           Arrays.asList(new Object[] {40, null }),
           Arrays.asList(new Object[] {12, null }),
           Arrays.asList(new Object[] {29, null }),
           Arrays.asList(new Object[] { 1, 1 }),  
           Arrays.asList(new Object[] {18, null }),
           Arrays.asList(new Object[] {35, null }),
           Arrays.asList(new Object[] { 7, 7 }),
           Arrays.asList(new Object[] { 7, 7 }),
        };
        
        System.out.println(Arrays.toString(this.leftTuples));
        helpCreateJoin();
        EnhancedSortMergeJoinStrategy esmjs = new EnhancedSortMergeJoinStrategy(SortOption.NOT_SORTED, SortOption.SORT);
        this.joinStrategy = esmjs;
        this.join.setJoinStrategy(joinStrategy);
        
        helpTestJoinDirect(expected, 10, 1);
	}
	
	@Test public void testMergeJoinOptimizationLeftOuterEmpty() throws Exception {
		this.joinType = JoinType.JOIN_LEFT_OUTER;
        int rows = 12;
        List[] data = new List[rows];
        for(int i=0; i<rows; i++) { 
            data[i] = new ArrayList();
            Integer value = new Integer((i*17) % 45);
            data[i].add(value);
        }
        this.leftTuples = data;
        this.rightTuples = new List[0];
        expected = new List[] {
           Arrays.asList(new Object[] { 0, null }),
           Arrays.asList(new Object[] {17, null }),
           Arrays.asList(new Object[] {34, null }),
           Arrays.asList(new Object[] { 6, null }),
           Arrays.asList(new Object[] {23, null }),
           Arrays.asList(new Object[] {40, null }),
           Arrays.asList(new Object[] {12, null }),
           Arrays.asList(new Object[] {29, null }),
           Arrays.asList(new Object[] { 1, null }),  
           Arrays.asList(new Object[] {18, null }),
           Arrays.asList(new Object[] {35, null }),
           Arrays.asList(new Object[] { 7, null }),
        };
        
        System.out.println(Arrays.toString(this.leftTuples));
        helpCreateJoin();
        EnhancedSortMergeJoinStrategy esmjs = new EnhancedSortMergeJoinStrategy(SortOption.NOT_SORTED, SortOption.SORT);
        this.joinStrategy = esmjs;
        this.join.setJoinStrategy(joinStrategy);
        
        helpTestJoinDirect(expected, 10, 1);
	}
    
    @Test public void testMergeJoinOptimizationMultiBatch() throws Exception {
    	helpTestEnhancedSortMergeJoin(10, false);
    }
    
    @Test public void testMergeJoinOptimizationMultiBatch1() throws Exception {
    	helpTestEnhancedSortMergeJoin(1, true);
    }
    
    @Test public void testMergeJoinOptimizationNoRows() throws Exception {
        this.joinType = JoinType.JOIN_INNER;
        this.leftTuples = createTuples1();
        this.rightTuples = new List[] {};
        expected = new List[] {};
        helpCreateJoin();               
        this.joinStrategy = new EnhancedSortMergeJoinStrategy(SortOption.SORT, SortOption.SORT);
        this.join.setJoinStrategy(joinStrategy);
        helpTestJoinDirect(expected, 100, 1);
    }
    
    @Test public void testMergeJoinOptimizationWithDistinct() throws Exception {
        this.joinType = JoinType.JOIN_INNER;
        int rows = 50;
        List[] data = new List[rows];
        for(int i=0; i<rows; i++) { 
            data[i] = new ArrayList();
            Integer value = new Integer((i*17) % 47);
            data[i].add(value);
        }
        this.leftTuples = data;
        this.rightTuples = new List[] {
            Arrays.asList(4),
            Arrays.asList(7),
            Arrays.asList(2),
            Arrays.asList(6),
            Arrays.asList(1),  
            Arrays.asList(8),
        };
        expected = new List[] {
           Arrays.asList(new Object[] { 4, 4 }),
           Arrays.asList(new Object[] { 8, 8 }),
           Arrays.asList(new Object[] { 7, 7 }),
           Arrays.asList(new Object[] { 2, 2 }),
           Arrays.asList(new Object[] { 6, 6 }),
           Arrays.asList(new Object[] { 1, 1 }),
        };
        helpCreateJoin();               
        this.joinStrategy = new EnhancedSortMergeJoinStrategy(SortOption.SORT, SortOption.SORT);
        this.join.setJoinStrategy(joinStrategy);
        //this.join.setRightDistinct(true);
        helpTestJoinDirect(expected, 40, 1);
    }
    
    @Test public void testMergeJoinOptimizationWithDistinctAlreadySorted() throws Exception {
        this.joinType = JoinType.JOIN_INNER;
        int rows = 50;
        List[] data = new List[rows];
        for(int i=0; i<rows; i++) { 
            data[i] = new ArrayList();
            Integer value = new Integer((i*17) % 47);
            data[i].add(value);
        }
        this.leftTuples = data;
        this.rightTuples = new List[] {
            Arrays.asList(1),  
            Arrays.asList(2),
            Arrays.asList(4),
            Arrays.asList(6),
            Arrays.asList(7),
            Arrays.asList(8),
        };
        expected = new List[] {
           Arrays.asList(new Object[] { 4, 4 }),
           Arrays.asList(new Object[] { 8, 8 }),
           Arrays.asList(new Object[] { 7, 7 }),
           Arrays.asList(new Object[] { 2, 2 }),
           Arrays.asList(new Object[] { 6, 6 }),
           Arrays.asList(new Object[] { 1, 1 })
        };
        helpCreateJoin();               
        this.joinStrategy = new EnhancedSortMergeJoinStrategy(SortOption.SORT, SortOption.ALREADY_SORTED);
        this.join.setJoinStrategy(joinStrategy);
        helpTestJoinDirect(expected, 40, 1);
    }
    
    @Test public void testRepeatedMerge() throws Exception {
    	helpTestRepeatedMerge(false);
    }

    @Test public void testRepeatedMergeWithDistinct() throws Exception {
    	helpTestRepeatedMerge(true);
    }
    
    public void helpTestRepeatedMerge(boolean indexDistinct) throws Exception {
        this.joinType = JoinType.JOIN_INNER;
        int rows = 69;
        List[] data = new List[rows];
        for(int i=0; i<rows; i++) { 
            data[i]=Arrays.asList((i*17) % 91);
        }
        if (indexDistinct) {
        	data[2] = Arrays.asList(0);
        }
        data[6] = Arrays.asList((Integer)null);
        this.rightTuples = data;
        this.leftTuples = new List[17];
        for (int i = 0; i < this.leftTuples.length; i++) {
        	this.leftTuples[i] = Arrays.asList(i*4);
        }
        if (!indexDistinct) {
        	this.leftTuples[3] = Arrays.asList(0);
        }
        this.leftTuples[11] = Arrays.asList((Integer)null);
        
        expected = new List[] {
        		Arrays.asList(64, 64),
        		Arrays.asList(36, 36),
        		Arrays.asList(8, 8),
        		Arrays.asList(48, 48),
        		Arrays.asList(20, 20),
        		Arrays.asList(60, 60),
        		Arrays.asList(32, 32),
        		Arrays.asList(4, 4),
        		Arrays.asList(16, 16),
        		Arrays.asList(56, 56),
        		Arrays.asList(28, 28),
        		Arrays.asList(0, 0),
        		Arrays.asList(0, 0),
        };
        helpCreateJoin();               
        EnhancedSortMergeJoinStrategy psj = new EnhancedSortMergeJoinStrategy(SortOption.SORT, SortOption.SORT);
        psj.setPreferMemCutoff(1);
        this.joinStrategy = psj;
        this.join.setJoinStrategy(joinStrategy);
        helpTestJoinDirect(expected, 4, 1000);
    }
    
    @Test public void testSortMergeWithDistinct() throws TeiidComponentException, TeiidProcessingException {
    	this.leftTuples = new List[] {Arrays.asList(1, 2), Arrays.asList(1, 3)};
        this.rightTuples = new List[] {Arrays.asList(1, 4), Arrays.asList(1, 5)};
        
        expected = new List[] {
                Arrays.asList(1, 2, 1, 4),
                Arrays.asList(1, 2, 1, 5),
                Arrays.asList(1, 3, 1, 4),
                Arrays.asList(1, 3, 1, 5),
        };

    	ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        
        ElementSymbol es2 = new ElementSymbol("e2"); //$NON-NLS-1$
        es2.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        
        List leftElements = Arrays.asList(es1, es2);
        leftNode = new BlockingFakeRelationalNode(1, leftTuples);
        leftNode.setElements(leftElements);
        
        ElementSymbol es3 = new ElementSymbol("e3"); //$NON-NLS-1$
        es3.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        
        ElementSymbol es4 = new ElementSymbol("e4"); //$NON-NLS-1$
        es4.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        
        List rightElements = Arrays.asList(es3, es4);

        rightNode = new BlockingFakeRelationalNode(2, rightTuples) {
        	public boolean hasBuffer(boolean requireFinal) {
        		return false;
        	}

        	public TupleBuffer getBuffer(int maxRows) throws BlockedException, TeiidComponentException, TeiidProcessingException {
        		fail();
        		throw new AssertionError();
        	};
        };
        rightNode.setElements(rightElements);
        
        List joinElements = new ArrayList();
        joinElements.addAll(leftElements);
        joinElements.addAll(rightElements);
        
        joinType = JoinType.JOIN_INNER;
        joinStrategy = new MergeJoinStrategy(SortOption.SORT_DISTINCT, SortOption.SORT_DISTINCT, false);
        
        join = new JoinNode(3);
        join.setElements(joinElements);
        join.setJoinType(joinType);
        
        join.setJoinExpressions(Arrays.asList(es1), Arrays.asList(es3));
        join.setJoinStrategy(joinStrategy);
        
        helpTestJoinDirect(expected, 100, 100000);
    }
}
