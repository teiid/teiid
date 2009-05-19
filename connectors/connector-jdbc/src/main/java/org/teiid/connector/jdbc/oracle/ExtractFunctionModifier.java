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

import java.util.ArrayList;
import java.util.List;

import org.teiid.connector.jdbc.translator.BasicFunctionModifier;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;


/**
 * Convert the YEAR/MONTH/DAY etc. function into an equivalent Oracle function.  
 * Format: EXTRACT(YEAR from Element) or EXTRACT(YEAR from DATE '2004-03-03')
 */
public class ExtractFunctionModifier extends BasicFunctionModifier {
    public static final String SPACE = " ";  //$NON-NLS-1$
    
    private String target;
    
    public ExtractFunctionModifier(String target) {
        this.target = target;
    }
    
    public List<?> translate(IFunction function) {
        List<IExpression> args = function.getParameters();
        
        List<Object> objs = new ArrayList<Object>();
        objs.add("EXTRACT("); //$NON-NLS-1$
        objs.add(target);
        objs.add(SPACE);
        objs.add("FROM"); //$NON-NLS-1$
        objs.add(SPACE);               
        objs.add(args.get(0));
        objs.add(")"); //$NON-NLS-1$
        return objs;
    }    
}
