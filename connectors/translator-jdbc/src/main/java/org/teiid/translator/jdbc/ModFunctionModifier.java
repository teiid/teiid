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

package org.teiid.translator.jdbc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.TypeFacility;

/**
 * Adds mod (remainder) support for non-integral types
 */
public class ModFunctionModifier extends AliasModifier {

	private Set<Class<?>> supportedTypes = new HashSet<Class<?>>(Arrays.asList(TypeFacility.RUNTIME_TYPES.INTEGER, TypeFacility.RUNTIME_TYPES.LONG));

	private LanguageFactory langFactory;

    public ModFunctionModifier(String modFunction, LanguageFactory langFactory) {
    	this(modFunction, langFactory, null);
    }

    public ModFunctionModifier(String modFunction, LanguageFactory langFactory, Collection<? extends Class<?>> supportedTypes) {
    	super(modFunction);
    	this.langFactory = langFactory;
    	if (supportedTypes != null) {
    		this.supportedTypes.addAll(supportedTypes);
    	}
    }
    
    @Override
    public List<?> translate(Function function) {
    	List<Expression> expressions = function.getParameters();
		Class<?> type = function.getType();
		if (supportedTypes.contains(type)) {
			modify(function);
			return null;
		}
		//x % y => x - sign(x) * floor(abs(x / y)) * y
		Function divide = langFactory.createFunction(SourceSystemFunctions.DIVIDE_OP, new ArrayList<Expression>(expressions), type); 

		Function abs = langFactory.createFunction(SourceSystemFunctions.ABS, Arrays.asList(divide), type);
		
		Function floor = langFactory.createFunction(SourceSystemFunctions.FLOOR, Arrays.asList(abs), type); 
		
		Function sign = langFactory.createFunction(SourceSystemFunctions.SIGN, Arrays.asList(expressions.get(0)), type);
		
		List<? extends Expression> multArgs = Arrays.asList(sign, floor, langFactory.createFunction(SourceSystemFunctions.ABS, Arrays.asList(expressions.get(1)), type));
		Function mult = langFactory.createFunction(SourceSystemFunctions.MULTIPLY_OP, multArgs, type); 

		List<Expression> minusArgs = Arrays.asList(expressions.get(0), mult);
		
		return Arrays.asList(langFactory.createFunction(SourceSystemFunctions.SUBTRACT_OP, minusArgs, type)); 
	}
	
}
