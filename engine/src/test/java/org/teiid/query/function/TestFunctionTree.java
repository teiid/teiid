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

package org.teiid.query.function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionParameter;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.function.metadata.FunctionCategoryConstants;
import org.teiid.query.function.source.SystemSource;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestFunctionTree {

    /** 
     * Walk through all functions by metadata and verify that we can look 
     * each one up by signature
     */
    @Test public void testWalkTree() {
        SystemSource source = new SystemSource(false);
        FunctionTree ft = new FunctionTree("foo", source);
        
        Collection<String> categories = ft.getCategories();
        for (String category : categories) {
            Collection<FunctionForm> functions = ft.getFunctionForms(category);
            assertTrue(functions.size() > 0);
        }        
    }
    
    public String z() {
    	return null;
    }
    
    protected static String x() {
    	return null;
    }
    
    public static String y() {
    	return null;
    }
    
    @Test public void testLoadErrors() {
    	FunctionMethod method = new FunctionMethod(
    			"dummy", null, null, PushDown.CAN_PUSHDOWN, "nonexistentClass", "noMethod",  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
	 	    	new FunctionParameter[0], 
	 	    	new FunctionParameter("output", DataTypeManager.DefaultDataTypes.STRING), false, Determinism.DETERMINISTIC); //$NON-NLS-1$
    	
    	//allowed, since we're not validating the class
    	new FunctionLibrary(RealMetadataFactory.SFM.getSystemFunctions(), new FunctionTree("foo", new UDFSource(Arrays.asList(method))));
    	
    	//should fail, no class
    	try {
    		new FunctionLibrary(RealMetadataFactory.SFM.getSystemFunctions(), new FunctionTree("foo", new UDFSource(Arrays.asList(method)), true));
    		fail();
    	} catch (TeiidRuntimeException e) {
    		
    	}
    	
    	method.setInvocationClass(TestFunctionTree.class.getName());
    	
    	//should fail, no method
    	try {
    		new FunctionLibrary(RealMetadataFactory.SFM.getSystemFunctions(), new FunctionTree("foo", new UDFSource(Arrays.asList(method)), true));
    		fail();
    	} catch (TeiidRuntimeException e) {
    		
    	}
    	
    	method.setInvocationMethod("testLoadErrors");
    	
    	//should fail, not void
    	try {
    		new FunctionLibrary(RealMetadataFactory.SFM.getSystemFunctions(), new FunctionTree("foo", new UDFSource(Arrays.asList(method)), true));
    		fail();
    	} catch (TeiidRuntimeException e) {
    		
    	}    	
    	
    	method.setInvocationMethod("x");
    	
    	//should fail, not public
    	try {
    		new FunctionLibrary(RealMetadataFactory.SFM.getSystemFunctions(), new FunctionTree("foo", new UDFSource(Arrays.asList(method)), true));
    		fail();
    	} catch (TeiidRuntimeException e) {
    		
    	}    
    	
    	method.setInvocationMethod("z");
    	
    	//should fail, not static
    	try {
    		new FunctionLibrary(RealMetadataFactory.SFM.getSystemFunctions(), new FunctionTree("foo", new UDFSource(Arrays.asList(method)), true));
    		fail();
    	} catch (TeiidRuntimeException e) {
    		
    	} 

    	method.setInvocationMethod("y");
    	
    	//valid!
    	new FunctionLibrary(RealMetadataFactory.SFM.getSystemFunctions(), new FunctionTree("foo", new UDFSource(Arrays.asList(method)), true));
    }
    
    @Test public void testNullCategory() {
    	FunctionMethod method = new FunctionMethod(
    			"dummy", null, null, PushDown.MUST_PUSHDOWN, "nonexistentClass", "noMethod",  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
	 	    	new FunctionParameter[0], 
	 	    	new FunctionParameter("output", DataTypeManager.DefaultDataTypes.STRING), //$NON-NLS-1$
	 	    	false, Determinism.DETERMINISTIC);
    	
    	Collection<org.teiid.metadata.FunctionMethod> list = Arrays.asList(method);
    	FunctionMetadataSource fms = Mockito.mock(FunctionMetadataSource.class);
    	Mockito.stub(fms.getFunctionMethods()).toReturn(list);
    	FunctionTree ft = new FunctionTree("foo", fms);
    	assertEquals(1, ft.getFunctionForms(FunctionCategoryConstants.MISCELLANEOUS).size());
    }
	
/*

//DEBUGGING CODE - this will print out the tree root in readable form
//(This code will either have to be pasted in to FunctionTree class, or
//somehow the Map treeRoot must be gotten from the FunctionTree)

    private static void debugPrintTreeRoot(Map treeRoot){
        System.out.println("<!><!><!><!><!><!><!><!><!><!><!><!>");
        System.out.println("FunctionTree treeRoot");
        StringBuffer s = new StringBuffer();
        debugPrintNode(treeRoot, 0, s);
        System.out.println(s.toString());
        System.out.println("<!><!><!><!><!><!><!><!><!><!><!><!>");
    }

    private static void debugPrintNode(Map node, int depth, StringBuffer s){
        Iterator i = node.entrySet().iterator();
        Map.Entry anEntry = null;
        while(i.hasNext()) {
            anEntry = (Map.Entry)i.next();
            appendLine(s, depth, "Key: " + anEntry.getKey());
            Object value = anEntry.getValue();
            if (value instanceof Map){
                s.append(" Map... ");
                debugPrintNode((Map)value, depth + 1, s);
            } else {
                s.append(" Value: " + value);
            }
        }
    }

    private static void appendLine(StringBuffer s, int depth, String value){
        s.append("\n");
        for (int i = 0; i< depth; i++){
            s.append("  ");
        }
        s.append(value);
    }
*/
}
