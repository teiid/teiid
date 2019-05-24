/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.optimizer.relational;

import java.util.*;

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
        List<Expression> projected = query.getSelect().getProjectedSymbols();
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
