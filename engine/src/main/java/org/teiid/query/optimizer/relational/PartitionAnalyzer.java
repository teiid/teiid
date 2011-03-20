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

package org.teiid.query.optimizer.relational;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.core.types.DataTypeManager;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SetCriteria;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.util.SymbolMap;

/**
 * TODO: support recursive detection of partitions
 * 
 * Extracts a map of partitioning information from a union
 */
public class PartitionAnalyzer {
	
	public static Map<ElementSymbol, List<Set<Constant>>> extractPartionInfo(SetQuery setQuery, List<ElementSymbol> projectedSymbols) {
		List<Query> queries = new LinkedList<Query>();
    	if (!extractQueries(setQuery, queries)) {
    		return Collections.emptyMap();
    	}
		Map<ElementSymbol, List<Set<Constant>>> partitions = new LinkedHashMap<ElementSymbol, List<Set<Constant>>>();
		boolean first = true;
		for (Query query : queries) {
			Map<ElementSymbol, Set<Constant>> info = extractPartitionInfo(query, projectedSymbols);
			
			partitions.keySet().retainAll(info.keySet());
			
			if (first) {
    			first = false;
    			for (Map.Entry<ElementSymbol, Set<Constant>> entry : info.entrySet()) {
    				ArrayList<Set<Constant>> values = new ArrayList<Set<Constant>>(queries.size());
					partitions.put(entry.getKey(), values);
					values.add(entry.getValue());
    			}
    			continue;
			} 
			Set<ElementSymbol> keys = partitions.keySet();
			
			for (Iterator<ElementSymbol> iter = keys.iterator(); iter.hasNext();) {
				ElementSymbol elementSymbol = iter.next();
				List<Set<Constant>> values = partitions.get(elementSymbol);
				Set<Constant> value = info.get(elementSymbol);
				for (Set<Constant> set : values) {
					if (!Collections.disjoint(set, value)) {
						iter.remove();
						continue;
					}
				}
				values.add(value);
			}
    	}
		return partitions;
	}
	
	public static boolean extractQueries(QueryCommand queryCommand, List<Query> result) {
		if (queryCommand instanceof SetQuery) {
			SetQuery sq = (SetQuery)queryCommand;
			if (sq.isAll() && sq.getOperation() == Operation.UNION && sq.getOrderBy() == null && sq.getLimit() == null && sq.getWith() == null) {
				if (!extractQueries(sq.getLeftQuery(), result)) {
					return false;
				}
				if (!extractQueries(sq.getRightQuery(), result)) {
					return false;
				}
				return true;
	    	}
			return false;
		}
		result.add((Query)queryCommand);
		return true;
	}
	
	private static Map<ElementSymbol, Set<Constant>> extractPartitionInfo(Query query, List<ElementSymbol> projectedSymbols) {
		List<SingleElementSymbol> projected = query.getSelect().getProjectedSymbols();
		List<Criteria> crits = Criteria.separateCriteriaByAnd(query.getCriteria());
		Map<Expression, Set<Constant>> inMap = new HashMap<Expression, Set<Constant>>();
		for (Criteria criteria : crits) {
			if (criteria instanceof CompareCriteria) {
				CompareCriteria cc = (CompareCriteria)criteria;
				if (cc.getOperator() != CompareCriteria.EQ) {
					continue;
				}
				if (cc.getLeftExpression() instanceof Constant) {
					inMap.put(cc.getRightExpression(), new HashSet<Constant>(Arrays.asList((Constant)cc.getLeftExpression())));
				} else if (cc.getRightExpression() instanceof Constant) {
					inMap.put(cc.getLeftExpression(), new HashSet<Constant>(Arrays.asList((Constant)cc.getRightExpression())));
				}
				continue;
			}
			if (!(criteria instanceof SetCriteria)) {
				continue;
			}
			SetCriteria sc = (SetCriteria)criteria;
			HashSet<Constant> values = new HashSet<Constant>();
			boolean allConstants = true;
			for (Expression exp : (Collection<Expression>)sc.getValues()) {
				if (exp instanceof Constant) {
					values.add((Constant)exp);
				} else {
					allConstants = false;
					break;
				}
			}
			if (allConstants) {
				inMap.put(sc.getExpression(), values);
			}
		}
		Map<ElementSymbol, Set<Constant>> result = new HashMap<ElementSymbol, Set<Constant>>();
		for (int i = 0; i < projected.size(); i++) {
			Expression ex = SymbolMap.getExpression(projected.get(i));
			if (DataTypeManager.isNonComparable(DataTypeManager.getDataTypeName(ex.getType()))) {
				continue;
			}
			if (ex instanceof Constant) {
				result.put(projectedSymbols.get(i), Collections.singleton((Constant)ex));
			} else {
				Set<Constant> values = inMap.get(ex);
				if (values != null) {
					result.put(projectedSymbols.get(i), values);
				}
			}
		}
		return result;
	}
	
}
