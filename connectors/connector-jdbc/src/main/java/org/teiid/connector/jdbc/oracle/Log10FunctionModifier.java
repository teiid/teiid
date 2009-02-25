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

import java.util.List;

import org.teiid.connector.jdbc.translator.BasicFunctionModifier;
import org.teiid.connector.jdbc.translator.FunctionModifier;
import org.teiid.connector.language.*;


public class Log10FunctionModifier extends BasicFunctionModifier implements FunctionModifier {
    
    private ILanguageFactory languageFactory;

    public Log10FunctionModifier(ILanguageFactory languageFactory) {
        this.languageFactory = languageFactory;
    }

    public IExpression modify(IFunction function) {
        function.setName("log"); //$NON-NLS-1$
        
        List<IExpression> args = function.getParameters();
        args.add(args.get(0));
        args.set(0, languageFactory.createLiteral(new Integer(10), Integer.class));
        return function;
    }

}
