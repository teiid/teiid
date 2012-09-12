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

package org.teiid.translator.object.infinispan.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.api.BasicCache;
import org.teiid.language.Select;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectExecutionFactory;
import org.teiid.translator.object.SearchStrategy;
import org.teiid.translator.object.SelectProjections;
import org.teiid.translator.object.infinispan.InfinispanExecutionFactory;
import org.teiid.translator.object.search.BasicKeySearchCriteria;
import org.teiid.translator.object.search.SearchCriterion;

/**
 * SearchByKey is a simple SearchStrategy that enables querying the cache by
 * the key, using EQUI and IN clauses on the SELECT statement.
 */
public class SearchByKey implements SearchStrategy {

	public List<Object> performSearch(Select command,
			SelectProjections projections,
			ObjectExecutionFactory objectFactory, Object connection)
			throws TranslatorException {

		InfinispanExecutionFactory factory = (InfinispanExecutionFactory) objectFactory;
		BasicCache<String, Object> cache = factory.getCache(connection);
		BasicKeySearchCriteria bksc = BasicKeySearchCriteria.getInstance(
				factory, projections, command);

		return get(bksc.getCriterion(), cache, factory);

	}

	private List<Object> get(SearchCriterion criterion,
			BasicCache<String, Object> cache, InfinispanExecutionFactory factory)
			throws TranslatorException {
		List<Object> results = null;

		if (!criterion.isRootTableInSelect()) {
			return Collections.EMPTY_LIST;
		}

		if (criterion.getOperator() == SearchCriterion.Operator.ALL) {
			Set keys = cache.keySet();
			results = new ArrayList<Object>();
			for (Iterator it = keys.iterator(); it.hasNext();) {
				Object v = cache.get(it.next());
				addValue(v, results, factory.getRootClass());
			}

			return results;
		}

		if (criterion.getCriterion() != null) {
			results = get(criterion.getCriterion(), cache, factory);
		}

		if (results == null) {
			results = new ArrayList<Object>();
		}

		if (criterion.getOperator().equals(SearchCriterion.Operator.EQUALS)) {

			Object value = criterion.getValue();

			Object v = cache.get(value instanceof String ? value : value
					.toString());
			if (v != null) {
				addValue(v, results, factory.getRootClass());
			}
		} else if (criterion.getOperator().equals(SearchCriterion.Operator.IN)) {

			List<Object> parms = (List) criterion.getValue();
			for (Iterator<Object> it = parms.iterator(); it.hasNext();) {
				Object arg = it.next();
				// the key is only supported in string format
				Object v = cache.get(arg instanceof String ? arg : arg
						.toString());
				if (v != null) {
					addValue(v, results, factory.getRootClass());
				}
			}

		}

		return results;

	}

	private void addValue(Object value, List<Object> results, Class rootNodeType) {
		if (value != null && value.getClass().equals(rootNodeType)) {

			if (value.getClass().isArray()) {
				List<Object> listRows = Arrays.asList((Object[]) value);
				results.addAll(listRows);
				return;
			}

			if (value instanceof Collection) {
				results.addAll((Collection) value);
				return;
			}

			if (value instanceof Map) {
				Map<?, Object> mapRows = (Map) value;
				results.addAll(mapRows.values());
				return;
			}

			results.add(value);
		}

	}

}
