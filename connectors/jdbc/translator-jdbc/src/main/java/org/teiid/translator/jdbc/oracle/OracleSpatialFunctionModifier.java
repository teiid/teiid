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

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.Literal;
import org.teiid.translator.jdbc.FunctionModifier;


public class OracleSpatialFunctionModifier extends FunctionModifier {

    /**
     * If either of the first two parameters are a Literal String, then we need to put the literal itself in the SQL
     * to be passed to Oracle, without the tick marks
     */
    public List<?> translate(Function function) {
        List<Expression> params = function.getParameters();
    	List<Object> objs = new ArrayList<Object>();
        objs.add(function.getName());
        objs.add("("); //$NON-NLS-1$
        addParamWithConversion(objs, params.get(0));
        objs.add(", "); //$NON-NLS-1$

        addParamWithConversion(objs, params.get(1));
        for (int i = 2; i < params.size(); i++) {
            objs.add(", "); //$NON-NLS-1$
            objs.add(params.get(i));
        }
        objs.add(")"); //$NON-NLS-1$
        return objs;
    }
	
	protected void addParamWithConversion(List<Object> objs,
                                          Expression expression) {
		if ((expression instanceof Literal) 
				&& (((Literal) expression).getValue() instanceof String)) {
			objs.add(((Literal) expression).getValue());
		} else {
			objs.add(expression);
		}
    }
    
}