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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.util.CommandContext;

/**
 */
public class TestUnionAllNode extends TestCase {

    /**
     * Constructor for TestUnionAllNode.
     * @param arg0
     */
    public TestUnionAllNode(String arg0) {
        super(arg0);
    }
    
    public void helpTestUnion(RelationalNode[] children, RelationalNode union, List[] expected) throws MetaMatrixComponentException, MetaMatrixProcessingException {
        BufferManager mgr = NodeTestUtil.getTestBufferManager(1, 2);
        CommandContext context = new CommandContext("pid", "test", null, null, null);               //$NON-NLS-1$ //$NON-NLS-2$
        
        for(int i=0; i<children.length; i++) {
            union.addChild(children[i]);
            children[i].initialize(context, mgr, null);
        }
        
        union.initialize(context, mgr, null);
        
        union.open();
        
        int currentRow = 1;
        while(true) {
            try {
                TupleBatch batch = union.nextBatch();
                for(int row = currentRow; row <= batch.getEndRow(); row++) {
                    List tuple = batch.getTuple(row);
                    //System.out.println(tuple);
                    assertEquals("Rows don't match at " + row, expected[row-1], tuple); //$NON-NLS-1$
                }
                
                currentRow += batch.getRowCount();    

                if(batch.getTerminationFlag()) {
                    break;
                }
            } catch(BlockedException e) {
                // ignore and retry
            }
        }
        
        union.close();
        
        assertEquals("Didn't match expected counts", expected.length, currentRow-1); //$NON-NLS-1$
    }
    
    public void testNoRows() throws MetaMatrixComponentException, MetaMatrixProcessingException {
        ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);

        ElementSymbol es2 = new ElementSymbol("e2"); //$NON-NLS-1$
        es2.setType(DataTypeManager.DefaultDataClasses.INTEGER);

        List leftElements = new ArrayList();
        leftElements.add(es1);
        RelationalNode leftNode = new FakeRelationalNode(1, new List[0]);
        leftNode.setElements(leftElements);

        List rightElements = new ArrayList();
        rightElements.add(es2);
        RelationalNode rightNode = new FakeRelationalNode(2, new List[0]);
        rightNode.setElements(rightElements);
        
        List unionElements = new ArrayList();
        unionElements.add(es1);

        UnionAllNode union = new UnionAllNode(3);
        union.setElements(unionElements);
        
        helpTestUnion(new RelationalNode[] {leftNode, rightNode}, union, new List[0]);        
    }

    public void helpTestUnionConfigs(int sources, int blockModIndex, int rowsPerSource, int batchSize, List[] expected) throws MetaMatrixComponentException, MetaMatrixProcessingException {
        ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);

        ElementSymbol es2 = new ElementSymbol("e2"); //$NON-NLS-1$
        es2.setType(DataTypeManager.DefaultDataClasses.INTEGER);

        RelationalNode[] nodes = new RelationalNode[sources];
        for(int i=0; i<nodes.length; i++) {
            List childElements = new ArrayList();
            childElements.add(es1);
            
            // Build source data
            List[] tuples = new List[rowsPerSource];
            for(int r = 0; r<rowsPerSource; r++) {
                tuples[r] = Arrays.asList(new Object[] { new Integer(i) });
            }
            
            if(blockModIndex >= 0 && (i % blockModIndex == 0)) {
                nodes[i] = new BlockingFakeRelationalNode(i, tuples, batchSize);
            } else {                
                nodes[i] = new FakeRelationalNode(i, tuples, batchSize);
            }
            nodes[i].setElements(childElements);           
        }
        
        List unionElements = new ArrayList();
        unionElements.add(es1);

        UnionAllNode union = new UnionAllNode(nodes.length);
        union.setElements(unionElements);
        
        helpTestUnion(nodes, union, expected);           
    }
    
    public void testBasicUnion() throws MetaMatrixComponentException, MetaMatrixProcessingException {
        List expected[] = new List[] {
            Arrays.asList(new Object[] { new Integer(0) }),    
            Arrays.asList(new Object[] { new Integer(0) }),    
            Arrays.asList(new Object[] { new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(1) })
          
        };

        helpTestUnionConfigs(2, -1, 2, 50, expected);
        
    }

    public void testBasicUnionMultipleSources() throws MetaMatrixComponentException, MetaMatrixProcessingException {
        List expected[] = new List[] {
            Arrays.asList(new Object[] { new Integer(0) }),    
            Arrays.asList(new Object[] { new Integer(0) }),    
            Arrays.asList(new Object[] { new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(3) }),
            Arrays.asList(new Object[] { new Integer(3) }),    
            Arrays.asList(new Object[] { new Integer(4) }),    
            Arrays.asList(new Object[] { new Integer(4) })          
        };

        helpTestUnionConfigs(5, -1, 2, 50, expected);
    }

    public void testMultipleSourcesHalfBlockingNodes() throws MetaMatrixComponentException, MetaMatrixProcessingException  {
        List expected[] = new List[] {
            Arrays.asList(new Object[] { new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(0) }),    
            Arrays.asList(new Object[] { new Integer(3) }),    
            Arrays.asList(new Object[] { new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(4) })          
        };

        helpTestUnionConfigs(5, 2, 1, 50, expected);
    }
    
    public void testMultipleSourcesAllBlockingNodes() throws MetaMatrixComponentException, MetaMatrixProcessingException {
        List expected[] = new List[] {
            Arrays.asList(new Object[] { new Integer(0) }),    
            Arrays.asList(new Object[] { new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(3) }),
            Arrays.asList(new Object[] { new Integer(4) })          
        };

        helpTestUnionConfigs(5, 1, 1, 50, expected);       
    }    
    
    public void testMultipleSourceMultiBatchAllBlocking() throws MetaMatrixComponentException, MetaMatrixProcessingException {
        List expected[] = new List[] {
            Arrays.asList(new Object[] { new Integer(0) }),    
            Arrays.asList(new Object[] { new Integer(0) }),    
            Arrays.asList(new Object[] { new Integer(0) }),    
            Arrays.asList(new Object[] { new Integer(0) }),    
            Arrays.asList(new Object[] { new Integer(0) }),    
            Arrays.asList(new Object[] { new Integer(0) }),    

            Arrays.asList(new Object[] { new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(1) }),    
            Arrays.asList(new Object[] { new Integer(1) }),    

            Arrays.asList(new Object[] { new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(2) }),    
            Arrays.asList(new Object[] { new Integer(2) })          
        };

        helpTestUnionConfigs(3, 1, 6, 1, expected);       
    }    

}
