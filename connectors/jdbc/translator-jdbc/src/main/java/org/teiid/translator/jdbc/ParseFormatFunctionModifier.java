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
import java.util.List;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.Literal;

public abstract class ParseFormatFunctionModifier extends FunctionModifier {
	
	protected String prefix;
	
	public ParseFormatFunctionModifier(String prefix) {
		this.prefix = prefix;
	}
	
	@Override
	public List<?> translate(Function function) {
		if (!(function.getParameters().get(1) instanceof Literal)) {
			return null; //shouldn't happen
		}
		Literal l = (Literal)function.getParameters().get(1);
		List<Object> result = new ArrayList<Object>();
		result.add(prefix);
		translateFormat(result, function.getParameters().get(0), (String)l.getValue());
		result.add(")"); //$NON-NLS-1$
		return result; 
	}

	protected void translateFormat(List<Object> result, Expression expression,
			String value) {
		result.add(expression);
		result.add(", "); //$NON-NLS-1$
		result.add(translateFormat(value));
	}

	abstract protected Object translateFormat(String format);
}