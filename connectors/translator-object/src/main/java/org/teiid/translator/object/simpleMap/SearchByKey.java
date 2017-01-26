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

package org.teiid.translator.object.simpleMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectVisitor;
import org.teiid.translator.object.SearchType;

/**
 * SearchByKey is simple search logic that enables querying the cache by
 * the key, using EQUI and IN clauses on the SELECT statement.
 */
public  class SearchByKey implements SearchType  {
	private ObjectConnection conn;
	
	public SearchByKey(ObjectConnection connection) {
		this.conn  = connection;
	}
	
	@Override
	public Object performKeySearch(String columnNameInSource,
			Object value,   ExecutionContext executionContext) throws TranslatorException  {
		try {
			return conn.get(value);
		} finally {
			conn = null;
		}
	}

	@Override
	public List<Object> performSearch(ObjectVisitor visitor,
		  ExecutionContext executionContext) throws TranslatorException  {
		
		try {
			return performSearch(visitor);
		} finally {
			conn = null;
		}
		
	}
	
	private List<Object> performSearch(ObjectVisitor visitor) throws TranslatorException {

		LogManager.logTrace(LogConstants.CTX_CONNECTOR,
				"Perform search by key."); //$NON-NLS-1$
		
		List<Object> values =visitor.getCriteriaValues();
		List<Object> results = new ArrayList<Object>();

		boolean hasLimit = (visitor.getLimit() > 0);
		
		if (values == null || values.isEmpty()) {
			if (hasLimit) {
				
				return getFirst(conn, visitor.getLimit());
			} 
			
			results.addAll(conn.getAll());
			
			return results;
		}
		for (Object value:values) {
			Object cv = conn.get(value);
			if (cv != null) {
				results.add(cv);
			}		
			if (hasLimit && results.size() >= visitor.getLimit()) {
				break;
			}
		}

		return results;

	}
	
	private List<Object> getFirst(ObjectConnection conn, int limit) throws TranslatorException {
		List<Object> objs = new ArrayList<Object>();
		@SuppressWarnings("unchecked")
		Map<Object, Object> c = (Map<Object, Object>) conn.getCache();
		int i = 0;
		
		for (Object k : c.keySet()) {
			objs.add(c.get(k));
			++i;
			if (i >= limit) break;
		}
		return objs;
	}
}
