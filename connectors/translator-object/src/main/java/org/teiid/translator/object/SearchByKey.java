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
import java.util.List;

import org.teiid.core.util.Assertion;
import org.teiid.language.Comparison;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.Condition;
import org.teiid.language.Delete;
import org.teiid.language.Expression;
import org.teiid.language.In;
import org.teiid.language.Literal;
import org.teiid.language.Select;
import org.teiid.language.Update;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.TranslatorException;

/**
 * SearchByKey is simple search logic that enables querying the cache by
 * the key, using EQUI and IN clauses on the SELECT statement.
 */
public  class SearchByKey implements SearchType  {

	protected  void addAll(CacheContainerWrapper cache, String cacheName, Class<?> rootClass, List<Object> results) throws TranslatorException {
		results.addAll(cache.getAll(cacheName));
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.SearchType#performKeySearch(java.lang.String, java.lang.String, java.lang.Object, org.teiid.translator.object.ObjectConnection)
	 */
	@Override
	public Object performKeySearch(String cacheName, String columnNameInSource,
			Object value, ObjectConnection conn) throws TranslatorException {
		return null;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.SearchType#performSearch(org.teiid.language.Update, java.lang.String, org.teiid.translator.object.ObjectConnection)
	 */
	@Override
	public List<Object> performSearch(Update command, String cacheName,
			ObjectConnection conn) throws TranslatorException {
		throw new UnsupportedOperationException("Update is not supported at this time");
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.SearchType#performSearch(org.teiid.language.Delete, java.lang.String, org.teiid.translator.object.ObjectConnection)
	 */
	@Override
	public List<Object> performSearch(Delete command, String cacheName,
			ObjectConnection conn) throws TranslatorException {
		throw new UnsupportedOperationException("Delete is not supported at this time");
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.SearchType#performSearch(org.teiid.language.Select, java.lang.String, org.teiid.translator.object.ObjectConnection)
	 */
	@Override
	public List<Object> performSearch(Select command, String cacheName,
			ObjectConnection conn) throws TranslatorException {

		CacheContainerWrapper cache = conn.getCacheContainer();
		Class<?> rootClass = conn.getType(cacheName);

		LogManager.logTrace(LogConstants.CTX_CONNECTOR,
				"Perform search by key."); //$NON-NLS-1$
		
		Condition criterion = command.getWhere();
		List<Object> results = new ArrayList<Object>();

		if (criterion == null) {
			addAll(cache, cacheName, rootClass, results);
			return results;
		}
	
		if (criterion instanceof Comparison && ((Comparison)criterion).getOperator() == Operator.EQ) {
			
			Comparison obj = (Comparison)criterion;
			LogManager.logTrace(LogConstants.CTX_CONNECTOR,
			"Parsing Comparison criteria."); //$NON-NLS-1$
		
			Expression rhs = obj.getRightExpression();
		
			Literal literal = (Literal)rhs;

			Object v = cache.get(cacheName, literal.getValue());
			
			if (v != null) {
				results.add(v);
			}
		} else {

			Assertion.assertTrue(criterion instanceof In, "unexpected condition " + criterion); //$NON-NLS-1$
			In obj = (In)criterion;
			Assertion.assertTrue(!obj.isNegated());
			LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Parsing IN criteria."); //$NON-NLS-1$

			List<Expression> rhsList = obj.getRightExpressions();

			for (Expression expr : rhsList) {
				Literal literal = (Literal) expr;

				Object v = cache.get(cacheName, literal.getValue());
				if (v != null) {
					results.add(v);
				}
			}
		} 
		return results;

		
	}

}
