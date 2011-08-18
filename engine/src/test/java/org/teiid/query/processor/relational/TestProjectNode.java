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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.processor.FakeDataManager;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;


/**
 */
public class TestProjectNode extends TestCase {

    /**
     * Constructor for TestSortNode.
     * @param arg0
     */
    public TestProjectNode(String arg0) {
        super(arg0);
    }
    
    public ProjectNode helpSetupProject(List elements, List[] data, List childElements, ProcessorDataManager dataMgr) throws TeiidComponentException {
        BufferManager mgr = BufferManagerFactory.getStandaloneBufferManager();
        CommandContext context = new CommandContext("pid", "test", null, null, 1);               //$NON-NLS-1$ //$NON-NLS-2$
        
        FakeRelationalNode dataNode = new FakeRelationalNode(2, data);
        dataNode.setElements(childElements);
        dataNode.initialize(context, mgr, null);    
        
        ProjectNode projectNode = new ProjectNode(1);
        projectNode.setSelectSymbols(elements);
        projectNode.setElements(elements);
        projectNode.addChild(dataNode);
        projectNode.initialize(context, mgr, dataMgr);    
        
        return projectNode;        
    }
    
    public void helpTestProject(List elements, List[] data, List childElements, List[] expected, ProcessorDataManager dataMgr) throws TeiidComponentException, TeiidProcessingException {
        ProjectNode projectNode = helpSetupProject(elements, data, childElements, dataMgr);
        
        projectNode.open();
        
        int currentRow = 1;
        while(true) {
            try {
                TupleBatch batch = projectNode.nextBatch();
                for(int row = currentRow; row <= batch.getEndRow(); row++) {
                    assertEquals("Rows don't match at " + row, expected[row-1], batch.getTuple(row)); //$NON-NLS-1$
                }
                
                if(batch.getTerminationFlag()) {
                    break;
                }
                currentRow += batch.getRowCount();    
            } catch(BlockedException e) {
                // ignore and try again
            }
        }
    }

    public void helpTestProjectFails(List elements, List[] data, List childElements, String expectedError) throws TeiidComponentException, TeiidProcessingException {
        ProjectNode projectNode = helpSetupProject(elements, data, childElements, null);
                
        try {
            projectNode.open();
            
            while(true) {
                TupleBatch batch = projectNode.nextBatch();                
                if(batch.getTerminationFlag()) {
                    break;
                }
            }
            
            fail("Expected error but test succeeded"); //$NON-NLS-1$
        } catch(ExpressionEvaluationException e) {
            //note that this should not be a component exception, which would indicate that something abnormal happened
            assertEquals("Got unexpected exception", expectedError.toUpperCase(), e.getMessage().toUpperCase()); //$NON-NLS-1$
        }                
    }
        
    public void testNoProject() throws Exception {
        ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);

        ElementSymbol es2 = new ElementSymbol("e2"); //$NON-NLS-1$
        es2.setType(DataTypeManager.DefaultDataClasses.STRING);
        
        List elements = new ArrayList();
        elements.add(es1);
        elements.add(es2);
        
        List[] data = new List[0];
        
        List projectElements = new ArrayList();
        projectElements.add(es1);
        projectElements.add(es2);
        
        helpTestProject(elements, data, projectElements, data, null);
        
    }

    public void testProjectPassThrough() throws Exception {
        ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);

        ElementSymbol es2 = new ElementSymbol("e2"); //$NON-NLS-1$
        es2.setType(DataTypeManager.DefaultDataClasses.STRING);
        
        List elements = new ArrayList();
        elements.add(es1);
        elements.add(es2);
        
        List projectElements = new ArrayList();
        projectElements.add(es1);
        projectElements.add(es2);
        
        List[] data = new List[20];
        for(int i=0; i<20; i++) { 
            data[i] = new ArrayList();
            data[i].add(new Integer((i*51) % 11));
            
            String str = "" + (i*3); //$NON-NLS-1$
            str = str.substring(0,1);
            data[i].add(str);              
        }

        helpTestProject(elements, data, projectElements, data, null);        
    }

    public void testProjectReorder() throws Exception {
        ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);

        ElementSymbol es2 = new ElementSymbol("e2"); //$NON-NLS-1$
        es2.setType(DataTypeManager.DefaultDataClasses.STRING);
        
        List elements = new ArrayList();
        elements.add(es1);
        elements.add(es2);
        
        List projectElements = new ArrayList();
        projectElements.add(es2);
        projectElements.add(es1);
        
        List[] data = new List[20];
        List[] expected = new List[20];
        for(int i=0; i<20; i++) { 
            data[i] = new ArrayList();
            expected[i] = new ArrayList();

            data[i].add(new Integer((i*51) % 11));
                        
            String str = "" + (i*3); //$NON-NLS-1$
            str = str.substring(0,1);
            data[i].add(str);              
            
            expected[i].add(data[i].get(1));
            expected[i].add(data[i].get(0));
        }

        helpTestProject(elements, data, projectElements, expected, null);        
    }

    public void testProjectExpression() throws Exception {
        ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.STRING);       
        List elements = new ArrayList();
        elements.add(es1);
        
        Function func = new Function("concat", new Expression[] { es1, new Constant("abc")}); //$NON-NLS-1$ //$NON-NLS-2$
        FunctionDescriptor fd = RealMetadataFactory.SFM.getSystemFunctionLibrary().findFunction("concat", new Class[] { DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.STRING }); //$NON-NLS-1$
        func.setFunctionDescriptor(fd);
        func.setType(DataTypeManager.DefaultDataClasses.STRING);
        ExpressionSymbol expr = new ExpressionSymbol("expr", func); //$NON-NLS-1$
        List projectElements = new ArrayList();
        projectElements.add(expr);
        
        List[] data = new List[] { 
            Arrays.asList(new Object[] { "1" }),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "2" }) }; //$NON-NLS-1$
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "1abc" }),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "2abc" }) }; //$NON-NLS-1$

        helpTestProject(projectElements, data, elements, expected, null);        
    }

    public void testProjectExpressionFunctionFails() throws Exception {
        ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.STRING);       
        List elements = new ArrayList();
        elements.add(es1);
        
        Function func = new Function("convert", new Expression[] { es1, new Constant("integer")}); //$NON-NLS-1$ //$NON-NLS-2$
        FunctionDescriptor fd = RealMetadataFactory.SFM.getSystemFunctionLibrary().findFunction("convert", new Class[] { DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.STRING }); //$NON-NLS-1$
        func.setFunctionDescriptor(fd);
        func.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        ExpressionSymbol expr = new ExpressionSymbol("expr", func); //$NON-NLS-1$
        List projectElements = new ArrayList();
        projectElements.add(expr);
        
        List[] data = new List[] { 
            Arrays.asList(new Object[] { "1" }),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "2x" }) }; //$NON-NLS-1$

        String expectedMessage = "ERROR CODE:ERR.015.001.0003 MESSAGE:Unable to evaluate convert(e1, integer): ERROR CODE:ERR.015.001.0003 MESSAGE:Error while evaluating function convert"; //$NON-NLS-1$

        helpTestProjectFails(projectElements, data, elements, expectedMessage);        
    }
    
    public void testProjectWithLookupFunction() throws Exception {
        ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.STRING);       
        List elements = new ArrayList();
        elements.add(es1);

        Function func = new Function("lookup", new Expression[] { new Constant("pm1.g1"), new Constant("e2"), new Constant("e1"), es1 }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        FunctionDescriptor desc = RealMetadataFactory.SFM.getSystemFunctionLibrary().findFunction("lookup", new Class[] { String.class, String.class, String.class, String.class } ); //$NON-NLS-1$
        func.setFunctionDescriptor(desc);
        func.setType(DataTypeManager.DefaultDataClasses.STRING);
        
        ExpressionSymbol expr = new ExpressionSymbol("expr", func); //$NON-NLS-1$
        List projectElements = new ArrayList();
        projectElements.add(expr);
        
        List[] data = new List[] { 
            Arrays.asList(new Object[] { "1" }),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "2" }) }; //$NON-NLS-1$
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a" }),  //$NON-NLS-1$
            Arrays.asList(new Object[] { "b" }) }; //$NON-NLS-1$

        FakeDataManager dataMgr = new FakeDataManager();
        dataMgr.setThrowBlocked(true);
        Map valueMap = new HashMap();
        valueMap.put("1", "a"); //$NON-NLS-1$ //$NON-NLS-2$
        valueMap.put("2", "b"); //$NON-NLS-1$ //$NON-NLS-2$
        dataMgr.defineCodeTable("pm1.g1", "e1", "e2", valueMap); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
         
        helpTestProject(projectElements, data, elements, expected, dataMgr);        
    }    
}
