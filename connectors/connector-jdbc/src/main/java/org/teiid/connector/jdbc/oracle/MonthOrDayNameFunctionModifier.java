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

import org.teiid.connector.api.SourceSystemFunctions;
import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.jdbc.translator.FunctionModifier;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.ILanguageFactory;


/**
 * Convert the MONTHNAME etc. function into an equivalent Oracle function.  
 * Format: to_char(timestampvalue/dayvalue, 'Month'/'Day') 
 */
public class MonthOrDayNameFunctionModifier extends FunctionModifier {
    private ILanguageFactory langFactory;
    private String format;
    
    public MonthOrDayNameFunctionModifier(ILanguageFactory langFactory, String format) {
        this.langFactory = langFactory;
        this.format = format;
    }
    
    @Override
    public List<?> translate(IFunction function) {
        List<IExpression> args = function.getParameters();
    
        IFunction func = langFactory.createFunction("TO_CHAR",  //$NON-NLS-1$
            Arrays.asList( 
                args.get(0), 
                langFactory.createLiteral(format, TypeFacility.RUNTIME_TYPES.STRING)),  
            TypeFacility.RUNTIME_TYPES.STRING);
        
        // For some reason, these values have trailing spaces
        IFunction trimFunc = langFactory.createFunction(SourceSystemFunctions.RTRIM,
            Arrays.asList( func ), TypeFacility.RUNTIME_TYPES.STRING);
        
        return Arrays.asList(trimFunc);    
    }
}
