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

import java.util.ArrayList;
import java.util.List;

import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.ILanguageFactory;

public class LocateFunctionModifier extends org.teiid.connector.jdbc.translator.LocateFunctionModifier {
	
	public LocateFunctionModifier(ILanguageFactory factory) {
		super(factory);
	}

	@Override
	public List<?> translate(IFunction function) {
		modify(function);
		List<Object> parts = new ArrayList<Object>();
		List<IExpression> params = function.getParameters();
		parts.add("position("); //$NON-NLS-1$
		parts.add(params.get(0));		
		parts.add(" in "); //$NON-NLS-1$
		if (params.size() == 3) {
			parts.add(this.getLanguageFactory().createFunction("substr", params.subList(1, 3), TypeFacility.RUNTIME_TYPES.STRING)); //$NON-NLS-1$
		} else {
			parts.add(params.get(1));
		}
		parts.add(")"); //$NON-NLS-1$
		return parts;
	}
	
}
