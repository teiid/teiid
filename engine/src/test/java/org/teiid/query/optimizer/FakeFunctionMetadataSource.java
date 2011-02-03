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

package org.teiid.query.optimizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.FunctionParameter;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.query.function.FunctionMetadataSource;
import org.teiid.query.function.metadata.FunctionMethod;


public class FakeFunctionMetadataSource implements FunctionMetadataSource {

    public Collection<org.teiid.metadata.FunctionMethod> getFunctionMethods() {
        List<org.teiid.metadata.FunctionMethod> methods = new ArrayList<org.teiid.metadata.FunctionMethod>();
        methods.add(new FunctionMethod("xyz", "", "misc", PushDown.MUST_PUSHDOWN,  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                                       FakeFunctionMetadataSource.class.getName(), "xyz", //$NON-NLS-1$
                                       new FunctionParameter[0],  
                                       new FunctionParameter("out", "integer"))); //$NON-NLS-1$ //$NON-NLS-2$
        
        FunctionParameter p1 = new FunctionParameter("astring", "string");  //$NON-NLS-1$  //$NON-NLS-2$
        FunctionParameter result = new FunctionParameter("trimstring", "string"); //$NON-NLS-1$  //$NON-NLS-2$

        FunctionMethod method = new FunctionMethod("MYRTRIM", "", "", FakeFunctionMetadataSource.class.getName(), "myrtrim", new FunctionParameter[] {p1}, result);  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        method.setPushdown(PushDown.CAN_PUSHDOWN);
        methods.add(method);
        
        FunctionMethod method2 = new FunctionMethod("misc.namespace.func", "", "", null, null, new FunctionParameter[] {p1}, result);  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        method2.setPushdown(PushDown.MUST_PUSHDOWN);
        methods.add(method2);
        
        FunctionMethod method3 = new FunctionMethod("parsedate_", "", "", null, null, new FunctionParameter[] {p1}, new FunctionParameter("", DataTypeManager.DefaultDataTypes.DATE));  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        method3.setPushdown(PushDown.MUST_PUSHDOWN);
        methods.add(method3);

        return methods;
    }
    
    public Class getInvocationClass(String className) throws ClassNotFoundException { 
        return Class.forName(className); 
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
}
