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

package com.metamatrix.query.optimizer;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.metamatrix.query.function.FunctionLibraryManager;
import com.metamatrix.query.function.FunctionMetadataSource;
import com.metamatrix.query.function.metadata.FunctionMethod;
import com.metamatrix.query.function.metadata.FunctionParameter;

public class FakeFunctionMetadataSource implements FunctionMetadataSource {

    public Collection getFunctionMethods() {
        List methods = new ArrayList();
        methods.add(new FunctionMethod("xyz", "", "misc", FunctionMethod.MUST_PUSHDOWN,  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                                       FakeFunctionMetadataSource.class.getName(), "xyz", //$NON-NLS-1$
                                       new FunctionParameter[0],  
                                       new FunctionParameter("out", "integer"))); //$NON-NLS-1$ //$NON-NLS-2$
        
        FunctionParameter p1 = new FunctionParameter("astring", "string");  //$NON-NLS-1$  //$NON-NLS-2$
        FunctionParameter result = new FunctionParameter("trimstring", "string"); //$NON-NLS-1$  //$NON-NLS-2$
        FunctionMethod method = new FunctionMethod("MYRTRIM", "", "", FakeFunctionMetadataSource.class.getName(), "myrtrim", new FunctionParameter[] {p1}, result);  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        method.setPushdown(FunctionMethod.CAN_PUSHDOWN);
        methods.add(method);
        return methods;
    }
    
    public Class getInvocationClass(String className) throws ClassNotFoundException { 
        return Class.forName(className); 
    }
    
 	public void loadFunctions(InputStream source) throws IOException{
 	}
    
    // dummy function
    public static Object xyz() {
        return null;
    }
    
    /** defect 15348*/
    public static Object myrtrim(Object astring) {
        String string = (String)astring;
        return string.trim();
    }
    
    public static void setupFunctionLibrary() {
        FunctionLibraryManager.registerSource(new FakeFunctionMetadataSource());        
    }
    
}
