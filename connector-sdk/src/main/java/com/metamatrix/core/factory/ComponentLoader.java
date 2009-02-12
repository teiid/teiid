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

package com.metamatrix.core.factory;

import java.io.InputStream;
import java.io.InputStreamReader;

import bsh.EvalError;
import bsh.Interpreter;

import com.metamatrix.cdk.CdkPlugin;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.FileUtil;

public class ComponentLoader {
    private Interpreter interpreter;
    
    public ComponentLoader(ClassLoader classLoader, String scriptName) {
        init(classLoader, scriptName);
    }
    
    private void init(ClassLoader classLoader, String scriptName) {
         InputStream scriptStream = classLoader.getResourceAsStream(scriptName);
         if (scriptStream == null) {
             throw new MetaMatrixRuntimeException(CdkPlugin.Util.getString("ComponentLoader.Resource_not_found__{0}_1", scriptName)); //$NON-NLS-1$
         }
         InputStreamReader scriptReader = new InputStreamReader(scriptStream);
         String script = FileUtil.read(scriptReader);
        
         interpreter = new bsh.Interpreter();
         interpreter.setClassLoader(classLoader);
         execute(script);            
    }
        
    public void set(String name, Object value) {
    	try {
			interpreter.set(name, value);
		} catch (EvalError e) {
			throw new MetaMatrixRuntimeException(e);
		}
    }
    
    public Object execute(String command) {
    	try {
			return interpreter.eval(command);
		} catch (EvalError e) {
			throw new RuntimeException(e);
		}
    }

    public Object load(String name) {
        return execute("load" + name + "()"); //$NON-NLS-1$ //$NON-NLS-2$
    }
}
