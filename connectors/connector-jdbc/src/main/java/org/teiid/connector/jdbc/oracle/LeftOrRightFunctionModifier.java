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

package org.teiid.connector.jdbc.oracle;

import java.util.Arrays;
import java.util.List;

import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.jdbc.translator.BasicFunctionModifier;
import org.teiid.connector.jdbc.translator.FunctionModifier;
import org.teiid.connector.language.*;


/**
 * Convert left(string, count) --> substr(string, 1, count)
 * or right(string, count) --> substr(string, -1 * count) - we lack a way to express a unary negation
 */
public class LeftOrRightFunctionModifier extends BasicFunctionModifier implements FunctionModifier {
    private ILanguageFactory langFactory;
    
    public LeftOrRightFunctionModifier(ILanguageFactory langFactory) {
        this.langFactory = langFactory;
    }
    
    /* 
     * @see com.metamatrix.connector.jdbc.extension.FunctionModifier#modify(com.metamatrix.data.language.IFunction)
     */
    public IExpression modify(IFunction function) {
        List<IExpression> args = function.getParameters();
        IFunction func = null;
        
        if (function.getName().equalsIgnoreCase("left")) { //$NON-NLS-1$
            func = langFactory.createFunction("SUBSTR",  //$NON-NLS-1$
                Arrays.asList(
                    args.get(0), 
                    langFactory.createLiteral(Integer.valueOf(1), TypeFacility.RUNTIME_TYPES.INTEGER),
                    args.get(1)),
                    String.class);   
        } else if (function.getName().equalsIgnoreCase("right")) { //$NON-NLS-1$
            IFunction negIndex = langFactory.createFunction("*",  //$NON-NLS-1$
                Arrays.asList(langFactory.createLiteral(Integer.valueOf(-1), TypeFacility.RUNTIME_TYPES.INTEGER), args.get(1)),
                Integer.class);
                            
            func = langFactory.createFunction("SUBSTR",  //$NON-NLS-1$
                Arrays.asList(
                    args.get(0), 
                    negIndex),
                    String.class);      
        }

        return func;    
    }
}
