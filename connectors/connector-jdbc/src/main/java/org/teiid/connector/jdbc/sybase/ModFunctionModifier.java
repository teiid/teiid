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

package org.teiid.connector.jdbc.sybase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.connector.api.TypeFacility.RUNTIME_TYPES;
import org.teiid.connector.jdbc.translator.AliasModifier;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.ILanguageFactory;


public class ModFunctionModifier extends AliasModifier {

	private ILanguageFactory langFactory;
    
    public ModFunctionModifier(ILanguageFactory langFactory) {
    	super("%"); //$NON-NLS-1$
        this.langFactory = langFactory;
    }
	
	@Override
	public IExpression modify(IFunction function) {
		List<IExpression> expressions = function.getParameters();
		if (RUNTIME_TYPES.INTEGER.equals(expressions.get(0).getType())) {
			return super.modify(function);
		}
		//x % y => x - floor(x / y) * y
		IFunction divide = langFactory.createFunction("/", new ArrayList<IExpression>(expressions), expressions.get(0).getType()); //$NON-NLS-1$
		
		IFunction floor = langFactory.createFunction("floor", Arrays.asList(divide), divide.getType()); //$NON-NLS-1$
		
		List<IExpression> multArgs = Arrays.asList(floor, expressions.get(1));
		IFunction mult = langFactory.createFunction("*", multArgs, multArgs.get(1).getType()); //$NON-NLS-1$

		List<IExpression> minusArgs = Arrays.asList(expressions.get(0), mult);
		
		return langFactory.createFunction("-", minusArgs, minusArgs.get(0).getType()); //$NON-NLS-1$
	}
	
}
