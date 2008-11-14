/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.connector.jdbc.oracle;

import com.metamatrix.connector.jdbc.extension.FunctionModifier;
import com.metamatrix.connector.jdbc.extension.impl.BasicFunctionModifier;
import com.metamatrix.data.language.*;
import com.metamatrix.data.language.IExpression;
import com.metamatrix.data.language.IFunction;

/**
 * Modify the locate function to use the Oracle instr function.
 * 
 * locate(sub, str) -> instr(str, sub)
 * 
 * locate(sub, str, start) -> instr(str, sub, start+1)
 */
public class LocateFunctionModifier extends BasicFunctionModifier implements FunctionModifier {

    private ILanguageFactory langFactory;
    
    public LocateFunctionModifier(ILanguageFactory langFactory) {
        this.langFactory = langFactory;
    }

    /* 
     * @see com.metamatrix.connector.jdbc.extension.FunctionModifier#modify(com.metamatrix.data.language.IFunction)
     */
    public IExpression modify(IFunction function) {
        IExpression[] args = function.getParameters();     
        IExpression[] instrArgs = new IExpression[args.length];
        instrArgs[0] = args[1];
        instrArgs[1] = args[0];
        
        if(args.length == 3) {
            if(args[2] instanceof ILiteral) {
                ILiteral indexConst = (ILiteral)args[2];
                if(indexConst.getValue() == null) {
                    instrArgs[2] = args[2];
                } else {
                    // Just modify the constant
                    Integer index = (Integer) ((ILiteral)args[2]).getValue();
                    instrArgs[2] = langFactory.createLiteral(new Integer(index.intValue()+1), Integer.class);
                }
            } else {
                // Make plus function since this involves an element or function
                IFunction plusFunction = langFactory.createFunction("+",  //$NON-NLS-1$
                    new IExpression[] { args[2], langFactory.createLiteral(new Integer(1), Integer.class) },
                    Integer.class);
                instrArgs[2] = plusFunction;
            }   
        }
                
        IFunction instrFunction = langFactory.createFunction("instr", instrArgs, Integer.class); //$NON-NLS-1$
        
        return instrFunction;           
    }

}
