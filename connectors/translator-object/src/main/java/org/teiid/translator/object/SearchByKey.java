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

package org.teiid.translator.object;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.core.util.Assertion;
import org.teiid.language.Comparison;
import org.teiid.language.Condition;
import org.teiid.language.Expression;
import org.teiid.language.In;
import org.teiid.language.Literal;
import org.teiid.language.Comparison.Operator;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.TranslatorException;

/**
 * SearchByKey is a simple ObjectConnection that enables querying the cache by
 * the key, using EQUI and IN clauses on the SELECT statement.
 */
public final class SearchByKey  {

	public static List<Object> get(Condition criterion,
			Map<?, ?> cache, Class<?> rootClass)
			throws TranslatorException {
		List<Object> results = null;

		if (criterion == null) {
			Map<?, ?> map = cache;
			Set<?> keys = map.keySet();
			results = new ArrayList<Object>();
			for (Iterator<?> it = keys.iterator(); it.hasNext();) {
				Object v = cache.get(it.next());
				addValue(v, results, rootClass);
			}
			return results;
			
		}

		results = new ArrayList<Object>();
	
		if (criterion instanceof Comparison && ((Comparison)criterion).getOperator() == Operator.EQ) {
			
			Comparison obj = (Comparison)criterion;
			LogManager.logTrace(LogConstants.CTX_CONNECTOR,
			"Parsing Comparison criteria."); //$NON-NLS-1$
			Comparison.Operator op = obj.getOperator();
		
			Expression rhs = obj.getRightExpression();
		
			Literal literal = (Literal)rhs;

			Object v = cache.get(literal.getValue());
			
			if (v != null) {
				addValue(v, results, rootClass);
			}
		} else {

			Assertion.assertTrue(criterion instanceof In, "unexpected condition " + criterion); //$NON-NLS-1$
			In obj = (In)criterion;
			Assertion.assertTrue(!obj.isNegated());
			LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Parsing IN criteria."); //$NON-NLS-1$

			List<Expression> rhsList = obj.getRightExpressions();

			for (Expression expr : rhsList) {
				Literal literal = (Literal) expr;

				Object v = cache.get(literal.getValue());
				if (v != null) {
					addValue(v, results, rootClass);
				}
			}
		} 
		return results;
	}

	private static void addValue(Object value, List<Object> results, Class<?> rootNodeType) throws TranslatorException {
		if (value == null) {
			return;
		}
		if (!rootNodeType.isAssignableFrom(value.getClass())) {
			// the object obtained from the cache has to be of the same root
			// class type, otherwise, the modeling
			// structure won't correspond correctly
			String msg = ObjectPlugin.Util.getString(
					"MapCacheConnection.unexpectedObjectTypeInCache", //$NON-NLS-1$
					new Object[] { value.getClass().getName(),
							rootNodeType.getName() });

			throw new TranslatorException(msg);			
		}
		results.add(value);
	}

}
