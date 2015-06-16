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

package org.teiid.translator.jdbc.hana;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.Literal;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.jdbc.FunctionModifier;


public class HanaSpatialFunctionModifier extends FunctionModifier {

    /**
     * Most geospatial functions in HANA are called from the geometry object or an equivalent expression.
     * For example, <geometry-expression>.ST_SRID() or <geometry-expression>.ST_Relate(<geo2>). This method
     * will take the argument(s) to the Teiid spatial function and move the first argument to precede
     * the function name.
     */
    public List<?> translate(Function function) {
        List<Expression> params = function.getParameters();
    	List<Object> objs = new ArrayList<Object>();
    	
    	Expression exp1 = params.get(0);
    	
    	objs.add(exp1+"."+function.getName());
        objs.add("("); //$NON-NLS-1$
        if (params.size()>1){
        	objs.add(params.get(1));
        }
        objs.add(")"); //$NON-NLS-1$
        return objs;
    }
	
}