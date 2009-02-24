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

package org.teiid.connector.jdbc.postgresql;

import java.util.Arrays;

import com.metamatrix.connector.language.IExpression;
import com.metamatrix.connector.language.IFunction;
import com.metamatrix.connector.language.ILanguageFactory;


/** 
 * @since 4.3
 */
class ModifiedDatePartFunctionModifier extends DatePartFunctionModifier {
    
    private String modifier;
    private Object arg;

    ModifiedDatePartFunctionModifier(ILanguageFactory factory, String partName, String modifierFunctionName, Object modifierArgument) {
        super(factory, partName);
        this.modifier = modifierFunctionName;
        this.arg = modifierArgument;
    }

    public IExpression modify(IFunction function) {
        IExpression expr = super.modify(function);
        return factory.createFunction(modifier,
                                       Arrays.asList(expr, factory.createLiteral(arg, arg.getClass())),
                                       arg.getClass());
    }
    
    
}
