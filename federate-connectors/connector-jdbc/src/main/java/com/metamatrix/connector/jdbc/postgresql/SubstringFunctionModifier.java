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

package com.metamatrix.connector.jdbc.postgresql;

import com.metamatrix.connector.jdbc.extension.FunctionModifier;
import com.metamatrix.connector.jdbc.extension.impl.BasicFunctionModifier;
import com.metamatrix.data.language.*;
import com.metamatrix.data.language.IExpression;
import com.metamatrix.data.language.IFunction;

/**
 * Convert left(string, count) --> substr(string, 0, count)
 * or right(string, count) --> substr(string, length(string) - count)
 */
class SubstringFunctionModifier extends BasicFunctionModifier implements FunctionModifier {
    private ILanguageFactory langFactory;
    private boolean isLeft;
    
    SubstringFunctionModifier(ILanguageFactory langFactory, boolean isLeft) {
        this.langFactory = langFactory;
        this.isLeft = isLeft;
    }
    
    /* 
     * @see com.metamatrix.connector.jdbc.extension.FunctionModifier#modify(com.metamatrix.data.language.IFunction)
     */
    public IExpression modify(IFunction function) {
        IExpression[] args = function.getParameters();
        IFunction func = null;
        
        if (isLeft) {
            func = langFactory.createFunction("substr",  //$NON-NLS-1$
                new IExpression[] {
                    args[0], 
                    langFactory.createLiteral(new Integer(1), Integer.class),
                    args[1]},
                    String.class);   
        } else {
            IFunction inner = langFactory.createFunction("LENGTH",  //$NON-NLS-1$
                new IExpression[] {args[0]},
                Integer.class);
            
            IExpression addOne = langFactory.createFunction("+", new IExpression[] {inner, langFactory.createLiteral(new Integer(1), Integer.class)}, Integer.class); //$NON-NLS-1$
            IExpression substrArgs = langFactory.createFunction("-",  //$NON-NLS-1$
                new IExpression[] {addOne, args[1] }, 
                    Integer.class);    
                
            func = langFactory.createFunction("substr",  //$NON-NLS-1$
                new IExpression[] {
                    args[0], 
                    substrArgs},
                    String.class);      
        }

        return func;    
    }
}
