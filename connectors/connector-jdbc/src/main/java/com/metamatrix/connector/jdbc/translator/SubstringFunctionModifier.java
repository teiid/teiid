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

package com.metamatrix.connector.jdbc.translator;

import com.metamatrix.connector.language.IExpression;
import com.metamatrix.connector.language.IFunction;
import com.metamatrix.connector.language.ILanguageFactory;

/**
 * Common logic for Substring modifiers requiring 3 parameters
 */
public class SubstringFunctionModifier extends BasicFunctionModifier {

    private ILanguageFactory languageFactory;
    private String length_function;
    
    public SubstringFunctionModifier(ILanguageFactory languageFactory, String substring_function, String length_function) {
    	this.languageFactory = languageFactory; 
        this.length_function = length_function;
    }

    /**
     * @see com.metamatrix.connector.jdbc.translator.FunctionModifier#modify(com.metamatrix.query.sql.symbol.Function)
     */
    public IExpression modify(IFunction function) {
        IExpression[] args = function.getParameters();
        IExpression[] newArgs = new IExpression[3];
        function.setParameters(newArgs);

        newArgs[0] = args[0];
        newArgs[1] = args[1];
        
        if(args.length == 2) {
            newArgs[2] = languageFactory.createFunction(length_function, new IExpression[] { args[0] }, Integer.class); 
        } else {
            newArgs[2] = args[2];
        }
        
        function.setParameters(newArgs);
                        
        return function;
    }
}