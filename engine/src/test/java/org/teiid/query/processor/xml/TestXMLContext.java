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

package org.teiid.query.processor.xml;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.teiid.core.TeiidComponentException;
import org.teiid.query.processor.xml.XMLContext;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;

import junit.framework.TestCase;



public class TestXMLContext extends TestCase {
    static String resultSetName = "ResultSet"; //$NON-NLS-1$
    
    public void testGetCurrentRow() throws Exception{

        XMLContext context = new XMLContext();
        
        List[] rows = new List[] { 
             Arrays.asList( new Object[] { "Lamp", new Integer(5), null } ),         //$NON-NLS-1$
             Arrays.asList( new Object[] { "Screwdriver", new Integer(100), null } ),         //$NON-NLS-1$
             Arrays.asList( new Object[] { "Goat", new Integer(4), null } )         //$NON-NLS-1$
        };
        
        FakePlanExecutor executor = new FakePlanExecutor(resultSetName, rows);
        context.setResultSet(resultSetName, executor);
        
        List currentRow = context.getCurrentRow(resultSetName);

        // this is only behaviour of the Fake Plan executor
        assertNull(currentRow);
        
        // move cursor forward
        currentRow = context.getNextRow(resultSetName);
        assertEquals(Arrays.asList( new Object[] { "Lamp", new Integer(5), null } ), currentRow); //$NON-NLS-1$
        
        // check the cursor again should be same as previous.
        currentRow = context.getCurrentRow(resultSetName);
        assertEquals(Arrays.asList( new Object[] { "Lamp", new Integer(5), null } ), currentRow); //$NON-NLS-1$
        
        // check the cursor 2nd again, make sure it is not moved
        currentRow = context.getCurrentRow(resultSetName);
        assertEquals(Arrays.asList( new Object[] { "Lamp", new Integer(5), null } ), currentRow); //$NON-NLS-1$
        
        // test remove
        context.removeResultSet(resultSetName);
        try {
            currentRow = context.getCurrentRow(resultSetName);
            fail("must have failed because the results are removed."); //$NON-NLS-1$
        }catch(TeiidComponentException e) {
        }
    }    
    
        
    public void testGetCurrentRows() throws Exception{
        
        XMLContext parentContext = new XMLContext();
        XMLContext childContext = new XMLContext(parentContext);
        
        List[] rows1 = new List[] { 
             Arrays.asList( new Object[] { "Lamp", new Integer(5), null } ),         //$NON-NLS-1$
             Arrays.asList( new Object[] { "Screwdriver", new Integer(100), null } ),         //$NON-NLS-1$
             Arrays.asList( new Object[] { "Goat", new Integer(4), null } )         //$NON-NLS-1$
        };       
        
        List[] rows2 = new List[] { 
             Arrays.asList( new Object[] { "Lamp2", new Integer(54), null } ),         //$NON-NLS-1$
             Arrays.asList( new Object[] { "Screwdriver2", new Integer(1000), null } ),         //$NON-NLS-1$
             Arrays.asList( new Object[] { "Goat2", new Integer(43), null } )         //$NON-NLS-1$
        };       
        String results1= "resultsOne"; //$NON-NLS-1$
        String results2= "resultsTwo"; //$NON-NLS-1$
        
        FakePlanExecutor executor = new FakePlanExecutor(results1, rows1);
        parentContext.setResultSet(results1, executor);
        parentContext.getNextRow(results1);

        executor = new FakePlanExecutor(results2, rows2);
        childContext.setResultSet(results2, executor);
        childContext.getNextRow(results2);

        // make sure parent context dos not have access to the child's results
        assertEquals(Arrays.asList( new Object[] { "Lamp", new Integer(5), null } ), parentContext.getCurrentRow(results1)); //$NON-NLS-1$
        try {
            parentContext.getCurrentRow(results2);
            fail("should fail to get child contexts results from parent.."); //$NON-NLS-1$
        } catch (TeiidComponentException e) {
        } 
        
        // note that we only using the current context which is the "child"
        // here we should have access to both parent and child's reslt sets.
        assertEquals(Arrays.asList( new Object[] { "Lamp", new Integer(5), null } ), childContext.getCurrentRow(results1)); //$NON-NLS-1$
        assertEquals(Arrays.asList( new Object[] { "Lamp2", new Integer(54), null } ), childContext.getCurrentRow(results2)); //$NON-NLS-1$

        // move the child context result and make sure parent context result stays same
        childContext.getNextRow(results2);
        assertEquals(Arrays.asList( new Object[] { "Lamp", new Integer(5), null } ), childContext.getCurrentRow(results1)); //$NON-NLS-1$        
        assertEquals(Arrays.asList( new Object[] { "Screwdriver2", new Integer(1000), null } ), childContext.getCurrentRow(results2)); //$NON-NLS-1$        
    }     
    
    public void testGetCurrentRowRecursive() throws Exception{
        
        XMLContext parentContext = new XMLContext();
        XMLContext childContext = new XMLContext(parentContext);
        
        List[] rows1 = new List[] { 
             Arrays.asList( new Object[] { "Lamp", new Integer(5), null } ),         //$NON-NLS-1$
             Arrays.asList( new Object[] { "Screwdriver", new Integer(100), null } ),         //$NON-NLS-1$
             Arrays.asList( new Object[] { "Goat", new Integer(4), null } )         //$NON-NLS-1$
        };       
        
        List[] rows2 = new List[] { 
             Arrays.asList( new Object[] { "Lamp2", new Integer(54), null } ),         //$NON-NLS-1$
             Arrays.asList( new Object[] { "Screwdriver2", new Integer(1000), null } ),         //$NON-NLS-1$
             Arrays.asList( new Object[] { "Goat2", new Integer(43), null } )         //$NON-NLS-1$
        };       
        
        FakePlanExecutor executor = new FakePlanExecutor(resultSetName, rows1);
        parentContext.setResultSet(resultSetName, executor);
        parentContext.getNextRow(resultSetName);

        // know here that 'FakeName' is a different result set for the recursive node
        // and you should be able to retrive rows using both 'FakeName' and realname
        executor = new FakePlanExecutor("FakeName", rows2); //$NON-NLS-1$
        childContext.setResultSet(resultSetName, executor); 
        childContext.getNextRow(resultSetName);

        assertEquals(Arrays.asList( new Object[] { "Lamp", new Integer(5), null } ), parentContext.getCurrentRow(resultSetName)); //$NON-NLS-1$
        assertEquals(Arrays.asList( new Object[] { "Lamp2", new Integer(54), null } ), childContext.getCurrentRow(resultSetName)); //$NON-NLS-1$
    }    
    
    public void testGetReferenceValues() throws Exception {
        XMLContext context = new XMLContext();
        String FOO = "Foo"; //$NON-NLS-1$
        String BAR = "Bar"; //$NON-NLS-1$
        
        ElementSymbol X = new ElementSymbol("Foo.X"); //$NON-NLS-1$
        ElementSymbol Y = new ElementSymbol("Foo.Y"); //$NON-NLS-1$

        GroupSymbol Foo = new GroupSymbol(FOO); 
        X.setGroupSymbol(Foo);
        Y.setGroupSymbol(Foo);

        ElementSymbol AX = new ElementSymbol("Bar.X"); //$NON-NLS-1$
        ElementSymbol AY = new ElementSymbol("Bar.Y"); //$NON-NLS-1$

        GroupSymbol Bar = new GroupSymbol(BAR); 
        AX.setGroupSymbol(Bar);
        AY.setGroupSymbol(Bar);

        List fooSchema = Arrays.asList( new Object[] {X, Y});
        List barSchema = Arrays.asList( new Object[] {AX,AY}); 
        
        List[] fooRows = new List[] { 
             Arrays.asList( new Object[] { "Lamp", new Integer(5)} ),         //$NON-NLS-1$
             Arrays.asList( new Object[] { "Screwdriver", new Integer(100)} ),         //$NON-NLS-1$
             Arrays.asList( new Object[] { "Goat", new Integer(4) } )         //$NON-NLS-1$
        };       
        
        List[] barRows = new List[] { 
             Arrays.asList( new Object[] { "Lamp2", new Integer(54)} ),         //$NON-NLS-1$
             Arrays.asList( new Object[] { "Screwdriver2", new Integer(1000)} ),         //$NON-NLS-1$
             Arrays.asList( new Object[] { "Goat2", new Integer(43)} )         //$NON-NLS-1$
        };       
        
        FakePlanExecutor executorFoo = new FakePlanExecutor(FOO, fooSchema, fooRows); 
        context.setResultSet(FOO, executorFoo);
        context.setVariableValues(FOO, context.getNextRow(FOO));
        
        XMLContext childContext = new XMLContext(context);
        
        FakePlanExecutor executorBar = new FakePlanExecutor(BAR, barSchema, barRows); 
        childContext.setResultSet(FOO, executorBar); 
        childContext.setVariableValues(FOO, childContext.getNextRow(FOO));
        
        // THIS is the TEST.. we need the Foo elements with Bar values 
        Map referenceValues = childContext.getReferenceValues();
        assertEquals(2, referenceValues.size());
        
        assertEquals("Lamp2", referenceValues.get(X)); //$NON-NLS-1$
        assertEquals(new Integer(54), referenceValues.get(Y)); 
        
        // advance BAR and get new results for the references.
        childContext.setVariableValues(FOO, childContext.getNextRow(FOO));
        referenceValues = childContext.getReferenceValues();
        assertEquals("Screwdriver2", referenceValues.get(X)); //$NON-NLS-1$
        assertEquals(new Integer(1000), referenceValues.get(Y)); 
    }
    
}
