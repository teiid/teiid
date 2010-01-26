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

package org.teiid.connector.jdbc.translator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.teiid.connector.api.SourceSystemFunctions;
import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.ILanguageFactory;

/**
 * Adds mod (remainder) support for non-integral types
 */
public class ModFunctionModifier extends AliasModifier {

	private Set<Class> supportedTypes = new HashSet<Class>(Arrays.asList(TypeFacility.RUNTIME_TYPES.INTEGER, TypeFacility.RUNTIME_TYPES.LONG));

	private ILanguageFactory langFactory;

    public ModFunctionModifier(String modFunction, ILanguageFactory langFactory) {
    	this(modFunction, langFactory, null);
    }

    public ModFunctionModifier(String modFunction, ILanguageFactory langFactory, Collection<Class> supportedTypes) {
    	super(modFunction);
    	this.langFactory = langFactory;
    	if (supportedTypes != null) {
    		this.supportedTypes.addAll(supportedTypes);
    	}
    }
    
    @Override
    public List<?> translate(IFunction function) {
    	List<IExpression> expressions = function.getParameters();
		Class<?> type = function.getType();
		if (supportedTypes.contains(type)) {
			modify(function);
			return null;
		}
		//x % y => x - sign(x) * floor(abs(x / y)) * y
		IFunction divide = langFactory.createFunction(SourceSystemFunctions.DIVIDE_OP, new ArrayList<IExpression>(expressions), type); 

		IFunction abs = langFactory.createFunction(SourceSystemFunctions.ABS, Arrays.asList(divide), type);
		
		IFunction floor = langFactory.createFunction(SourceSystemFunctions.FLOOR, Arrays.asList(abs), type); 
		
		IFunction sign = langFactory.createFunction(SourceSystemFunctions.SIGN, Arrays.asList(expressions.get(0)), type);
		
		List<? extends IExpression> multArgs = Arrays.asList(sign, floor, langFactory.createFunction(SourceSystemFunctions.ABS, Arrays.asList(expressions.get(1)), type));
		IFunction mult = langFactory.createFunction(SourceSystemFunctions.MULTIPLY_OP, multArgs, type); 

		List<IExpression> minusArgs = Arrays.asList(expressions.get(0), mult);
		
		return Arrays.asList(langFactory.createFunction(SourceSystemFunctions.SUBTRACT_OP, minusArgs, type)); 
	}
	
}
