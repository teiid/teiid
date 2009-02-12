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

package com.metamatrix.connector.jdbc.oracle;

import com.metamatrix.connector.jdbc.extension.FunctionModifier;
import com.metamatrix.connector.jdbc.extension.impl.BasicFunctionModifier;
import com.metamatrix.connector.language.*;

/**
 * Convert left(string, count) --> substr(string, 0, count)
 * or right(string, count) --> substr(string, length(string) - count)
 */
public class LeftOrRightFunctionModifier extends BasicFunctionModifier implements FunctionModifier {
    private ILanguageFactory langFactory;
    private String target;
    
    public LeftOrRightFunctionModifier(ILanguageFactory langFactory, String target) {
        this.langFactory = langFactory;
        this.target = target;
    }
    
    /* 
     * @see com.metamatrix.connector.jdbc.extension.FunctionModifier#modify(com.metamatrix.data.language.IFunction)
     */
    public IExpression modify(IFunction function) {
        IExpression[] args = function.getParameters();
        IFunction func = null;
        
        if (target.equalsIgnoreCase("left")) { //$NON-NLS-1$
            func = langFactory.createFunction("SUBSTR",  //$NON-NLS-1$
                new IExpression[] {
                    args[0], 
                    langFactory.createLiteral(new Integer(0), Integer.class),
                    args[1]},
                    String.class);   
        } else if (target.equalsIgnoreCase("right")) { //$NON-NLS-1$
            IFunction inner = langFactory.createFunction("LENGTH",  //$NON-NLS-1$
                new IExpression[] {args[0]},
                Integer.class);
            
            IExpression substrArgs = langFactory.createFunction("-",  //$NON-NLS-1$
                new IExpression[] {inner, args[1] }, 
                    Integer.class);    
                
            func = langFactory.createFunction("SUBSTR",  //$NON-NLS-1$
                new IExpression[] {
                    args[0], 
                    substrArgs},
                    String.class);      
        }

        return func;    
    }
}
