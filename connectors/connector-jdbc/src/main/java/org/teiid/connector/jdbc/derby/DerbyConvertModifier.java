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

package org.teiid.connector.jdbc.derby;

import java.util.Arrays;

import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.jdbc.db2.DB2ConvertModifier;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.ILanguageFactory;


/**
 */
public class DerbyConvertModifier extends DB2ConvertModifier {

    private ILanguageFactory langFactory;
    
    public DerbyConvertModifier(ILanguageFactory langFactory) {
    	super(langFactory);
        this.langFactory = langFactory;
    }
    
    @Override
    protected IExpression convertToReal(IExpression expression, Class sourceType) {
        
        if(sourceType.equals(TypeFacility.RUNTIME_TYPES.STRING)){

            // BEFORE: convert(string_expr, float)
            // AFTER:  cast(cast(string_expr as decimal) as float)
            IFunction inner = langFactory.createFunction("cast",  //$NON-NLS-1$
                Arrays.asList( expression, langFactory.createLiteral("decimal", TypeFacility.RUNTIME_TYPES.STRING) ),  //$NON-NLS-1$
                TypeFacility.RUNTIME_TYPES.BIG_DECIMAL);

            IFunction outer = langFactory.createFunction("cast",  //$NON-NLS-1$
                Arrays.asList( inner, langFactory.createLiteral("float", TypeFacility.RUNTIME_TYPES.STRING) ),  //$NON-NLS-1$
                TypeFacility.RUNTIME_TYPES.FLOAT);

            return outer; 

        } else if(sourceType.equals(TypeFacility.RUNTIME_TYPES.DOUBLE) || 
                        sourceType.equals(TypeFacility.RUNTIME_TYPES.BIG_DECIMAL)) {
        
            // BEFORE: convert(num_expr, float)
            // AFTER:  cast(num_expr as float)
            return langFactory.createFunction("cast",  //$NON-NLS-1$
                Arrays.asList( expression, langFactory.createLiteral("float", TypeFacility.RUNTIME_TYPES.STRING) ),  //$NON-NLS-1$
                TypeFacility.RUNTIME_TYPES.FLOAT);
        }

        // Just drop anything else
        return null;
    }

    @Override
    protected IExpression convertToDouble(IExpression expression, Class sourceType) {

        if(sourceType.equals(TypeFacility.RUNTIME_TYPES.STRING)){
            // BEFORE: convert(string_expr, double)
            // AFTER:  cast(cast(string_expr as decimal) as double)
            IFunction inner = langFactory.createFunction("cast",  //$NON-NLS-1$
                Arrays.asList( expression, langFactory.createLiteral("decimal", TypeFacility.RUNTIME_TYPES.STRING) ),  //$NON-NLS-1$
                TypeFacility.RUNTIME_TYPES.BIG_DECIMAL);

            return langFactory.createFunction("cast",  //$NON-NLS-1$
                Arrays.asList( inner, langFactory.createLiteral("double", TypeFacility.RUNTIME_TYPES.STRING) ),  //$NON-NLS-1$
                TypeFacility.RUNTIME_TYPES.DOUBLE);
        }

        // Just drop anything else
        return null;
    }

    @Override
    protected IExpression convertToBigDecimal(IExpression expression, Class sourceType) {
        
        if(sourceType.equals(TypeFacility.RUNTIME_TYPES.STRING)){
            // BEFORE: convert(string_expr, bigdecimal)
            // AFTER:  cast(string_expr as decimal)
            return langFactory.createFunction("cast",  //$NON-NLS-1$
                Arrays.asList( expression, langFactory.createLiteral("decimal", TypeFacility.RUNTIME_TYPES.STRING) ),  //$NON-NLS-1$
                TypeFacility.RUNTIME_TYPES.BIG_DECIMAL);
        }

        // Just drop anything else
        return null;
    }
    
}
