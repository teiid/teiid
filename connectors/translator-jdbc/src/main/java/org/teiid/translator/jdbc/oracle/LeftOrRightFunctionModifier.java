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

package org.teiid.translator.jdbc.oracle;

import java.util.Arrays;
import java.util.List;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.FunctionModifier;


/**
 * Convert left(string, count) --> substr(string, 1, count)
 * or right(string, count) --> substr(string, -1 * count) - we lack a way to express a unary negation
 */
public class LeftOrRightFunctionModifier extends FunctionModifier {
    private LanguageFactory langFactory;
    
    public LeftOrRightFunctionModifier(LanguageFactory langFactory) {
        this.langFactory = langFactory;
    }
    
    @Override
    public List<?> translate(Function function) {
        List<Expression> args = function.getParameters();
        Function func = null;
        
        if (function.getName().equalsIgnoreCase("left")) { //$NON-NLS-1$
            func = langFactory.createFunction("SUBSTR",  //$NON-NLS-1$
                Arrays.asList(
                    args.get(0), 
                    langFactory.createLiteral(Integer.valueOf(1), TypeFacility.RUNTIME_TYPES.INTEGER),
                    args.get(1)),
                    String.class);   
        } else if (function.getName().equalsIgnoreCase("right")) { //$NON-NLS-1$
            Function negIndex = langFactory.createFunction("*",  //$NON-NLS-1$
                Arrays.asList(langFactory.createLiteral(Integer.valueOf(-1), TypeFacility.RUNTIME_TYPES.INTEGER), args.get(1)),
                Integer.class);
                            
            func = langFactory.createFunction("SUBSTR",  //$NON-NLS-1$
                Arrays.asList(
                    args.get(0), 
                    negIndex),
                    String.class);      
        }

        return Arrays.asList(func);    
    }
}
