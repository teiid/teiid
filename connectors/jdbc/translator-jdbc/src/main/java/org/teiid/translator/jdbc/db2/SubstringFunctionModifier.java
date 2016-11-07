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
import org.teiid.language.Comparison.Operator;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.Literal;
import org.teiid.language.SearchedCase;
import org.teiid.language.SearchedWhenClause;
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
		//case when length > LENGTH(string) - start + 1 then LENGTH(string) - start + 1 case when length > 0 then length end
		Expression forLength = function.getParameters().get(2);
		List<SearchedWhenClause> clauses = new ArrayList<SearchedWhenClause>(2);
		Boolean isNegative = null;
		if (forLength instanceof Literal) {
			Literal l = (Literal)forLength;
			int value = (Integer)l.getValue();
			isNegative = value < 0;
		}
		Function length = new Function(
				SourceSystemFunctions.LENGTH,
				Arrays.asList(function.getParameters().get(0)),
				TypeFacility.RUNTIME_TYPES.INTEGER);

		Expression from = function.getParameters().get(1);
		SearchedCase adjustedFrom = new SearchedCase(Arrays.asList(new SearchedWhenClause(new Comparison(from, length, Operator.GT), new Function(
				SourceSystemFunctions.ADD_OP,
				Arrays.asList(
						length,
						new Literal(1, TypeFacility.RUNTIME_TYPES.INTEGER)),
			    TypeFacility.RUNTIME_TYPES.INTEGER))), 
				from, TypeFacility.RUNTIME_TYPES.INTEGER);
		function.getParameters().set(1, adjustedFrom);

		Expression maxLength = new Function(
				SourceSystemFunctions.SUBTRACT_OP,
				Arrays.asList(length,
							new Function(
								SourceSystemFunctions.SUBTRACT_OP,
								Arrays.asList(
										adjustedFrom,
										new Literal(1, TypeFacility.RUNTIME_TYPES.INTEGER)),
							    TypeFacility.RUNTIME_TYPES.INTEGER)),
				TypeFacility.RUNTIME_TYPES.INTEGER);
		clauses.add(new SearchedWhenClause(new Comparison(forLength, maxLength, Operator.GT), maxLength));
		Expression defaultExpr = null;
		if (isNegative == null) {
			clauses.add(new SearchedWhenClause(new Comparison(forLength, new Literal(0, TypeFacility.RUNTIME_TYPES.INTEGER), Operator.GT), forLength));
		} else if (isNegative) {
			//TODO: could be done in the rewriter
			return Arrays.asList(new Literal(null, TypeFacility.RUNTIME_TYPES.STRING));
		} else {
			defaultExpr = forLength;
		}
		SearchedCase sc = new SearchedCase(clauses, 
				defaultExpr, TypeFacility.RUNTIME_TYPES.INTEGER);
		function.getParameters().set(2, sc);
		return null;
	}
	
}
