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

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectSelectVisitor;
import org.teiid.translator.object.SearchType;

/**
 * SearchByKey is simple search logic that enables querying the cache by
 * the key, using EQUI and IN clauses on the SELECT statement.
 */
public  class SearchByKey implements SearchType  {
	
	@Override
	public Object performKeySearch(String columnNameInSource,
			Object value, ObjectConnection conn) throws TranslatorException  {

		return conn.get(value);
	}

	@Override
	public List<Object> performSearch(ObjectSelectVisitor visitor,
			ObjectConnection conn) throws TranslatorException  {

		LogManager.logTrace(LogConstants.CTX_CONNECTOR,
				"Perform search by key."); //$NON-NLS-1$
		
		List<Object> values = visitor.getCriteriaValues();
		List<Object> results = new ArrayList<Object>();

		if (values == null || values.isEmpty()) {
			if (visitor.getLimit() > 0) {
				results.addAll(conn.getFirst(visitor.getLimit()));
			} else {
				results.addAll(conn.getAll());
			}
			return results;
		}
		
		for (Object value:values) {
			Object cv = conn.get(value);
			results.add(cv);
		
		}

		return results;

	}
}
