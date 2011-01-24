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

package org.teiid.translator.jdbc.db2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.Comparison;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.Literal;
import org.teiid.language.SearchedCase;
import org.teiid.language.SearchedWhenClause;
import org.teiid.language.Comparison.Operator;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;

public class SubstringFunctionModifier extends AliasModifier {
	
	public SubstringFunctionModifier() {
		super("substr"); //$NON-NLS-1$
	}
	
	@Override
	public List<?> translate(Function function) {
		this.modify(function);
		if (function.getParameters().size() != 3) {
			return null;
		}
		//case when length < 0 then null when length < LENGTH(string) - start + 1 then exp else LENGTH(string) - start + 1
		Expression length = function.getParameters().get(2);
		List<SearchedWhenClause> clauses = new ArrayList<SearchedWhenClause>(2);
		Boolean isNegative = null;
		if (length instanceof Literal) {
			Literal l = (Literal)length;
			if (!l.isMultiValued()) {
				int value = (Integer)l.getValue();
				isNegative = value < 0;
			}
		}
		if (isNegative == null) {
			clauses.add(new SearchedWhenClause(new Comparison(length, new Literal(0, TypeFacility.RUNTIME_TYPES.INTEGER), Operator.LT), new Literal(null, TypeFacility.RUNTIME_TYPES.INTEGER)));
		} else if (isNegative) {
			//TODO: could be done in the rewriter
			function.getParameters().set(2, null);
			return null;
		} 
		Expression maxLength = new Function(
				SourceSystemFunctions.SUBTRACT_OP,
				Arrays.asList(new Function(
								SourceSystemFunctions.LENGTH,
								Arrays.asList(function.getParameters().get(0)),
								TypeFacility.RUNTIME_TYPES.INTEGER),
							new Function(
								SourceSystemFunctions.ADD_OP,
								Arrays.asList(
										function.getParameters().get(1),
										new Literal(1, TypeFacility.RUNTIME_TYPES.INTEGER)),
							    TypeFacility.RUNTIME_TYPES.INTEGER)),
				TypeFacility.RUNTIME_TYPES.INTEGER);
		clauses.add(new SearchedWhenClause(new Comparison(length, maxLength, Operator.LE), length));
		SearchedCase sc = new SearchedCase(clauses, 
				maxLength, TypeFacility.RUNTIME_TYPES.INTEGER);
		function.getParameters().set(2, sc);
		return null;
	}
	
}
