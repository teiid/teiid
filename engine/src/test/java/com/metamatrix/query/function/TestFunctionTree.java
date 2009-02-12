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

package com.metamatrix.query.function;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;

import org.mockito.Mockito;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.function.metadata.FunctionCategoryConstants;
import com.metamatrix.query.function.metadata.FunctionMethod;
import com.metamatrix.query.function.metadata.FunctionParameter;
import com.metamatrix.query.function.source.SystemSource;

public class TestFunctionTree extends TestCase {

	// ################################## FRAMEWORK ################################
	
	public TestFunctionTree(String name) { 
		super(name);		
	}	
	
	// ################################## TEST HELPERS ################################
	
	// ################################## ACTUAL TESTS ################################
	
    /** 
     * Walk through all functions by metadata and verify that we can look 
     * each one up by signature
     */
    public void testWalkTree() {
        SystemSource source = new SystemSource();
        FunctionTree ft = new FunctionTree(source);
        
        Collection categories = ft.getCategories();
        Iterator catIter = categories.iterator();
        while(catIter.hasNext()) { 
            String category = (String) catIter.next();
            LogManager.logInfo("test", "Category: " + category); //$NON-NLS-1$ //$NON-NLS-2$
            
            Collection functions = ft.getFunctionForms(category);
            Iterator functionIter = functions.iterator();
            while(functionIter.hasNext()) { 
                FunctionForm form = (FunctionForm) functionIter.next();
                LogManager.logInfo("test", "\tFunction: " + form.getDisplayString());                 //$NON-NLS-1$ //$NON-NLS-2$
            }            
        }        
    }
    
    /**
     * Test what happens when a function is loaded that does not have a class in the
     * classpath.  This *should* be ok as long as the function is not invoked.
     */
    public void testUnloadableFunction() { 
        // Create dummy source
    	FunctionMetadataSource dummySource = new FunctionMetadataSource() {
    	 	public Collection getFunctionMethods() {
    	 	    // Build dummy method
    	 	    FunctionMethod method = new FunctionMethod(
    	 	    	"dummy", null, "no category", "nonexistentClass", "noMethod",  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    	 	    	new FunctionParameter[0], 
    	 	    	new FunctionParameter("output", DataTypeManager.DefaultDataTypes.STRING) ); //$NON-NLS-1$

    	 	    // Wrap method in a list 
    	 		List methods = new ArrayList();
    	 		methods.add(method);
    	 		return methods;    
    	 	}  
    	 	
    	 	public Class getInvocationClass(String className) throws ClassNotFoundException { 
    	 	    throw new ClassNotFoundException("Could not find class " + className); //$NON-NLS-1$
    	 	}
    	 	
    	 	public void loadFunctions(InputStream source) throws IOException{
    	 	}
    	};	 
    	
    	FunctionLibraryManager.registerSource(dummySource);
    	FunctionLibraryManager.reloadSources();
    }
    
    public void testNullCategory() {
    	FunctionMetadataSource fms = Mockito.mock(FunctionMetadataSource.class);
    	Mockito.stub(fms.getFunctionMethods()).toReturn(Arrays.asList(new FunctionMethod(
    			"dummy", null, null, FunctionMethod.MUST_PUSHDOWN, "nonexistentClass", "noMethod",  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	 	    	new FunctionParameter[0], 
	 	    	new FunctionParameter("output", DataTypeManager.DefaultDataTypes.STRING) //$NON-NLS-1$
    	)));
    	FunctionTree ft = new FunctionTree(fms);
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
