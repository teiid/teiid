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

package com.metamatrix.connector.jdbc.sybase;

import java.util.List;

import com.metamatrix.connector.api.TypeFacility.RUNTIME_TYPES;
import com.metamatrix.connector.jdbc.extension.impl.AliasModifier;
import com.metamatrix.connector.language.IExpression;
import com.metamatrix.connector.language.IFunction;
import com.metamatrix.connector.language.ILanguageFactory;

public class ModFunctionModifier extends AliasModifier {

	private ILanguageFactory langFactory;
    
    public ModFunctionModifier(ILanguageFactory langFactory) {
    	super("%"); //$NON-NLS-1$
        this.langFactory = langFactory;
    }
	
	@Override
	public IExpression modify(IFunction function) {
		IExpression[] expressions = function.getParameters();
		if (RUNTIME_TYPES.INTEGER.equals(expressions[0].getType())) {
			return super.modify(function);
		}
		//x % y => x - floor(x / y) * y
		IExpression[] divideArgs = new IExpression[2];
		System.arraycopy(expressions, 0, divideArgs, 0, 2);
		IFunction divide = langFactory.createFunction("/", divideArgs, divideArgs[0].getType()); //$NON-NLS-1$
		
		IFunction floor = langFactory.createFunction("floor", new IExpression[] {divide}, divide.getType()); //$NON-NLS-1$
		
		IExpression[] multArgs = new IExpression[] {
				floor, expressions[1]
		};
		IFunction mult = langFactory.createFunction("*", multArgs, multArgs[1].getType()); //$NON-NLS-1$
		
		IExpression[] minusArgs = new IExpression[] {
				expressions[0], mult
		};
		return langFactory.createFunction("-", minusArgs, minusArgs[0].getType()); //$NON-NLS-1$
	}
	
	@Override
	public List translate(IFunction function) {
		return null;
	}

}
