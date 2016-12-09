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

import java.util.List;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.FunctionModifier;


public class Log10FunctionModifier extends FunctionModifier {
    
    private LanguageFactory languageFactory;

    public Log10FunctionModifier(LanguageFactory languageFactory) {
        this.languageFactory = languageFactory;
    }

    @Override
    public List<?> translate(Function function) {
        function.setName("log"); //$NON-NLS-1$
        
        List<Expression> args = function.getParameters();
        args.add(args.get(0));
        args.set(0, languageFactory.createLiteral(new Integer(10), TypeFacility.RUNTIME_TYPES.INTEGER));
        return null;
    }

}
