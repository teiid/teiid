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

package com.metamatrix.script.shell;

import bsh.EvalError;
import bsh.Interpreter;


/** 
 * A parser class used in conjunction with Bean Shell
 */
public class SimpleParser implements CustomParser {

    final String ls = System.getProperty("line.separator"); //$NON-NLS-1$
    Interpreter interpreter = null;
    
    public String convert(String str) {        
        if (interpreter == null) {
            return str+ls;
        }

        try {
            interpreter.set("mmstr", str); //$NON-NLS-1$
            interpreter.eval("mparse();"+ls); //$NON-NLS-1$ //); 
            return (String) interpreter.get("mmstr"); //$NON-NLS-1$
        } catch (EvalError err) {
        }
        return ";"; //$NON-NLS-1$
    }     
    
    public void setInterpreter(Interpreter i) {
        this.interpreter = i;
    }
}
