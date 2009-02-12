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

import java.lang.reflect.Method;

import junit.framework.TestCase;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.query.function.metadata.FunctionMethod;

public class TestFunctionDescriptorImpl extends TestCase {

    /**
     * Constructor for TestFunctionDescriptorImpl.
     * @param name
     */
    public TestFunctionDescriptorImpl(String name) {
        super(name);
    }
    
    /**
     * Find the invocation method for a function.
     * @param source The function metadata source, which knows how to obtain the invocation class
     * @param invocationClass The class to invoke for this function
     * @param invocationMethod The method to invoke for this function
     * @param numArgs Number of arguments in method
     * @throws NoSuchMethodException 
     * @throws SecurityException 
     */
    private Method lookupMethod(String invocationClass, String invocationMethod, int numArgs) throws SecurityException, NoSuchMethodException {
        // Build signature
        Class[] objectSignature = new Class[numArgs];
        for(int i=0; i<numArgs; i++) {
            objectSignature[i] = java.lang.Object.class;
        }

        // Find Method
        Method method = null;
        try {
            Class methodClass = Class.forName(invocationClass);
            method = methodClass.getMethod(invocationMethod, objectSignature);
        } catch(ClassNotFoundException e) {
            // Failed to load class, so can't load method - this will fail at invocation time.
            // We don't fail here because this situation can occur in the modeler, which does
            // not have the function jar files.  The modeler never invokes, so this isn't a
            // problem.
            return null;
        } 

        // Validate method
        if(! FunctionTree.isValidMethod(method)) {
            return null;
        }
        return method;
    }
    
    public void test1() throws Exception {
        FunctionDescriptor f1 = new FunctionDescriptor("+", FunctionMethod.CAN_PUSHDOWN, //$NON-NLS-1$
            new Class[] { DataTypeManager.DefaultDataClasses.INTEGER, DataTypeManager.DefaultDataClasses.INTEGER },
            DataTypeManager.DefaultDataClasses.INTEGER,
            lookupMethod("com.metamatrix.query.function.FunctionMethods", "plus", 2) , false, true, FunctionMethod.DETERMINISTIC);    //$NON-NLS-1$ //$NON-NLS-2$
            
        UnitTestUtil.helpTestEquivalence(0, f1, f1);             
    }

    public void test2() throws Exception  {
        FunctionDescriptor f1 = new FunctionDescriptor("+", FunctionMethod.CAN_PUSHDOWN,//$NON-NLS-1$
            new Class[] { DataTypeManager.DefaultDataClasses.INTEGER, DataTypeManager.DefaultDataClasses.INTEGER },
            DataTypeManager.DefaultDataClasses.INTEGER,
            lookupMethod("com.metamatrix.query.function.FunctionMethods", "plus", 2), false, true, FunctionMethod.DETERMINISTIC ); //$NON-NLS-1$ //$NON-NLS-2$

        FunctionDescriptor f2 = new FunctionDescriptor("+", FunctionMethod.CAN_PUSHDOWN,//$NON-NLS-1$
            new Class[] { DataTypeManager.DefaultDataClasses.INTEGER, DataTypeManager.DefaultDataClasses.INTEGER },
            DataTypeManager.DefaultDataClasses.INTEGER,
            lookupMethod("com.metamatrix.query.function.FunctionMethods", "plus", 2), false, true, FunctionMethod.DETERMINISTIC ); //$NON-NLS-1$ //$NON-NLS-2$

        UnitTestUtil.helpTestEquivalence(0, f1, f2);
    }
    
    public void test3() throws Exception  {
        FunctionDescriptor f1 = new FunctionDescriptor("+", FunctionMethod.CAN_PUSHDOWN,//$NON-NLS-1$
            new Class[] { DataTypeManager.DefaultDataClasses.INTEGER, DataTypeManager.DefaultDataClasses.INTEGER },
            DataTypeManager.DefaultDataClasses.INTEGER,
            lookupMethod("com.metamatrix.query.function.FunctionMethods", "plus", 2), false, false, FunctionMethod.DETERMINISTIC ); //$NON-NLS-1$ //$NON-NLS-2$

        
        FunctionDescriptor f2 = new FunctionDescriptor("+", FunctionMethod.CAN_PUSHDOWN,//$NON-NLS-1$
            new Class[] { DataTypeManager.DefaultDataClasses.INTEGER, DataTypeManager.DefaultDataClasses.INTEGER },
            DataTypeManager.DefaultDataClasses.INTEGER,
            lookupMethod("com.metamatrix.query.function.FunctionMethods", "plus", 2), false, true, FunctionMethod.DETERMINISTIC ); //$NON-NLS-1$ //$NON-NLS-2$

        assertNotSame("objects should not be equal", f1, f2); //$NON-NLS-1$
    }

}
