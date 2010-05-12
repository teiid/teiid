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
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.FunctionModifier;


/**
 * Convert the MONTHNAME etc. function into an equivalent Oracle function.  
 * Format: to_char(timestampvalue/dayvalue, 'Month'/'Day') 
 */
public class MonthOrDayNameFunctionModifier extends FunctionModifier {
    private LanguageFactory langFactory;
    private String format;
    
    public MonthOrDayNameFunctionModifier(LanguageFactory langFactory, String format) {
        this.langFactory = langFactory;
        this.format = format;
    }
    
    @Override
    public List<?> translate(Function function) {
        List<Expression> args = function.getParameters();
    
        Function func = langFactory.createFunction("TO_CHAR",  //$NON-NLS-1$
            Arrays.asList( 
                args.get(0), 
                langFactory.createLiteral(format, TypeFacility.RUNTIME_TYPES.STRING)),  
            TypeFacility.RUNTIME_TYPES.STRING);
        
        // For some reason, these values have trailing spaces
        Function trimFunc = langFactory.createFunction(SourceSystemFunctions.RTRIM,
            Arrays.asList( func ), TypeFacility.RUNTIME_TYPES.STRING);
        
        return Arrays.asList(trimFunc);    
    }
}
