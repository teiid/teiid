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
package org.teiid.translator.object.mapcache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.resource.ResourceException;

import org.teiid.language.Select;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectPlugin;
import org.teiid.translator.object.search.BasicKeySearchCriteria;
import org.teiid.translator.object.search.SearchCriterion;

/**
 * The MapCacheConnection provides simple key searches of the cache.
 * 
 * @author vhalbert
 * 
 */
public class MapCacheConnection extends BasicConnection  implements ObjectConnection {

	private MapCacheExecutionFactory factory = null;
	private BasicKeySearchCriteria visitor = null;
	
	public MapCacheConnection(MapCacheExecutionFactory factory) {
		super();
		this.factory = factory;
	}
	
	@Override
	public boolean isAlive() {
		return true;
	}


	@Override
	public void cleanUp() {
		factory = null;
		visitor = null;
	}
	

	@Override
	public void close() throws ResourceException {
		cleanUp();
		
	}

	public List<Object> performSearch(Select command) throws TranslatorException {

		visitor = BasicKeySearchCriteria.getInstance(factory,command);

		return get(visitor.getCriterion(), factory.getCache(), factory.getRootClass());
	}

	private List<Object> get(SearchCriterion criterion, Map<?, ?> cache,
			Class<?> rootClass) throws TranslatorException {
		List<Object> results = null;
		if (criterion.getOperator() == SearchCriterion.Operator.ALL) {
			results = new ArrayList<Object>();
			for (Iterator<?> it = cache.keySet().iterator(); it.hasNext();) {
				Object v = cache.get(it.next());
				addValue(v, results, rootClass);

			}

			return results;
		}

		if (criterion.getCriterion() != null) {
			results = get(criterion.getCriterion(), cache, rootClass);
		}

		if (results == null) {
			results = new ArrayList<Object>();
		}

		if (criterion.getOperator().equals(SearchCriterion.Operator.EQUALS)) {

			Object v = cache.get(criterion.getValue());
			if (v != null) {
				addValue(v, results, rootClass);
			}
		} else if (criterion.getOperator().equals(SearchCriterion.Operator.IN)) {

			List<?> parms = (List<?>) criterion.getValue();
			for (Iterator<?> it = parms.iterator(); it.hasNext();) {
				Object arg = it.next();
				Object v = cache.get(arg);
				if (v != null) {
					addValue(v, results, rootClass);
				}
			}

		}

		return results;

	}

	private void addValue(Object value, List<Object> results, Class<?> rootClass)
			throws TranslatorException {
		// can only add objects of the same root class in the cache
		if (value != null) {
			if (value.getClass().equals(rootClass)) {

				if (value.getClass().isArray()) {
					List<Object> listRows = Arrays.asList((Object[]) value);
					results.addAll(listRows);
					return;
				}

				if (value instanceof Collection) {
					results.addAll((Collection<?>) value);
					return;
				}

				if (value instanceof Map) {
					Map<?, ?> mapRows = (Map<?, ?>) value;
					results.addAll(mapRows.values());
					return;
				}

				results.add(value);
			} else {
				// the object obtained from the cache has to be of the same root
				// class type, otherwise, the modeling
				// structure won't correspond correctly
				String msg = ObjectPlugin.Util.getString(
						"MapCacheConnection.unexpectedObjectTypeInCache",
						new Object[] { value.getClass().getName(),
								rootClass.getName() });

				throw new TranslatorException(msg);
			}
		}

	}

}
