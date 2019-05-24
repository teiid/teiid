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

package org.teiid.query.tempdata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.teiid.core.types.ArrayImpl;
import org.teiid.language.Like.MatchMode;
import org.teiid.language.SortSpecification.NullOrdering;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.processor.relational.ListNestedSortComparator;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.MatchCriteria;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.lang.SetCriteria;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.translator.ExecutionFactory.NullOrder;

/**
 * Accumulates information about index usage.
 */
public class BaseIndexInfo<T extends SearchableTable> {

    List<Object> lower = null;
    List<Object> upper = null;
    ArrayList<List<Object>> valueSet = new ArrayList<List<Object>>();
    T table;
    Boolean ordering;
    boolean covering;
    CompoundCriteria nonCoveredCriteria = null;
    CompoundCriteria coveredCriteria = null;

    public BaseIndexInfo(T table, final List<? extends Expression> projectedCols, final Criteria condition, OrderBy orderBy, boolean primary) {
        this.table = table;
        if (primary || this.table.getColumnMap().keySet().containsAll(projectedCols)) {
            covering = true;
        }
        if (table.getPkLength() > 0) {
            processCriteria(condition, primary);
            if (orderBy != null) {
                ordering = useIndexForOrderBy(orderBy);
            }
        }
    }

    private void processCriteria(Criteria condition, boolean primary) {
        List<Criteria> crits = Criteria.separateCriteriaByAnd(condition);
        if (!primary) {
            for (Iterator<Criteria> critIter = crits.iterator(); critIter.hasNext();) {
                Criteria criteria = critIter.next();
                if (table.getColumnMap().keySet().containsAll(ElementCollectorVisitor.getElements(criteria, false))) {
                    if (coveredCriteria == null) {
                        coveredCriteria = new CompoundCriteria();
                    }
                    coveredCriteria.addCriteria(criteria);
                } else {
                    covering = false;
                    if (nonCoveredCriteria == null) {
                        nonCoveredCriteria = new CompoundCriteria();
                    }
                    nonCoveredCriteria.addCriteria(criteria);
                    critIter.remove();
                }
            }
        }
        for (int i = 0; i < table.getPkLength(); i++) {
            for (Iterator<Criteria> critIter = crits.iterator(); critIter.hasNext();) {
                Criteria criteria = critIter.next();
                if (criteria instanceof CompareCriteria) {
                    CompareCriteria cc = (CompareCriteria)criteria;
                    Object matchResult = table.matchesPkColumn(i, cc.getLeftExpression());
                    if (Boolean.FALSE.equals(matchResult)) {
                        continue;
                    }
                    if (cc.getOperator() != CompareCriteria.EQ && !table.supportsOrdering(i, cc.getLeftExpression())) {
                        critIter.remove();
                        continue;
                    }
                    this.addCondition(i, matchResult, (Constant)cc.getRightExpression(), cc.getOperator());
                    critIter.remove();
                } else if (criteria instanceof IsNullCriteria) {
                    IsNullCriteria inc = (IsNullCriteria)criteria;
                    Object matchResult = table.matchesPkColumn(i, inc.getExpression());
                    if (Boolean.FALSE.equals(matchResult)) {
                        continue;
                    }

                    this.addCondition(i, matchResult, new Constant(null), CompareCriteria.EQ);
                    critIter.remove();
                } else if (criteria instanceof MatchCriteria) {
                    MatchCriteria matchCriteria = (MatchCriteria)criteria;
                    Object matchResult = table.matchesPkColumn(i, matchCriteria.getLeftExpression());
                    if (Boolean.FALSE.equals(matchResult)) {
                        continue;
                    }
                    Constant value = (Constant)matchCriteria.getRightExpression();
                    String pattern = (String)value.getValue();
                    boolean escaped = false;
                    char escapeChar = matchCriteria.getEscapeChar();
                    if (matchCriteria.getMode() == MatchMode.REGEX) {
                        escapeChar = '\\';
                    }
                    StringBuilder prefix = new StringBuilder();

                    if (pattern.length() > 0 && matchCriteria.getMode() == MatchMode.REGEX && pattern.charAt(0) != '^') {
                        //make the assumption that we require an anchor
                        continue;
                    }

                    for (int j = matchCriteria.getMode() == MatchMode.REGEX?1:0; j < pattern.length(); j++) {
                        char character = pattern.charAt(j);

                        if (character == escapeChar && character != MatchCriteria.NULL_ESCAPE_CHAR) {
                            if (escaped) {
                                prefix.append(character);
                                escaped = false;
                            } else {
                                escaped = true;
                            }
                            continue;
                        }
                        if (!escaped) {
                            if (matchCriteria.getMode() == MatchMode.LIKE) {
                                if (character == MatchCriteria.WILDCARD_CHAR || character == MatchCriteria.MATCH_CHAR) {
                                    break;
                                }
                            } else {
                                int index = Arrays.binarySearch(Evaluator.REGEX_RESERVED, character);
                                if (index >= 0 && pattern.length() > 0) {
                                    getRegexPrefix(pattern, escapeChar, prefix, j, character);
                                    break;
                                }
                            }
                        } else {
                            escaped = false;
                        }
                        prefix.append(character);
                    }
                    if (prefix.length() > 0) {
                        this.addCondition(i, matchResult, new Constant(prefix.toString()), CompareCriteria.GE);
                        if (matchCriteria.getLeftExpression() instanceof Function && table.supportsOrdering(i, matchCriteria.getLeftExpression())) {
                            //this comparison needs to be aware of case
                            this.addCondition(i, matchResult, new Constant(prefix.substring(0, prefix.length() -1) + (char) (Character.toLowerCase(prefix.charAt(prefix.length()-1))+1)), CompareCriteria.LE);
                        } else {
                            this.addCondition(i, matchResult, new Constant(prefix.substring(0, prefix.length() -1) + (char) (prefix.charAt(prefix.length()-1)+1)), CompareCriteria.LE);
                        }
                    } else {
                        critIter.remove();
                    }
                } else if (criteria instanceof SetCriteria) {
                    SetCriteria setCriteria = (SetCriteria)criteria;
                    if (setCriteria.isNegated()) {
                        continue;
                    }
                    Object matchResult = table.matchesPkColumn(i, setCriteria.getExpression());
                    if (Boolean.FALSE.equals(matchResult)) {
                        continue;
                    }
                    Collection<Constant> values = (Collection<Constant>) setCriteria.getValues();
                    this.addSet(i, matchResult, values);
                    critIter.remove();
                }
            }
        }
    }

    private void getRegexPrefix(String pattern, char escapeChar,
            StringBuilder prefix, int j, char character) {
        boolean escaped = false;
        //check the rest of the expression for |
        int level = 0;
        for (int k = j; k < pattern.length(); k++) {
            character = pattern.charAt(k);
            if (character == escapeChar && character != MatchCriteria.NULL_ESCAPE_CHAR) {
                escaped = !escaped;
            } else if (!escaped) {
                if (character == '(') {
                    level++;
                } else if (character == ')') {
                    level--;
                } else if (character == '|' && level == 0) {
                    prefix.setLength(0); //TODO: turn this into an IN condition
                    return;
                }
            }
        }

        if (character == '{' || character == '?' || character == '*') {
            prefix.setLength(prefix.length() - 1);
        }
    }

    void addCondition(int i, Object match, Constant value, int comparisionMode) {
        Object value2 = value.getValue();
        switch (comparisionMode) {
        case CompareCriteria.EQ:
            if (i == 0) {
                valueSet.clear();
                valueSet.add(new ArrayList<Object>(table.getPkLength()));
            }
            if (valueSet.size() == 1) {
                List<Object> toSearch = valueSet.get(0);
                buildSearchRow(i, match, value2, toSearch);
                lower = null;
                upper = null;
            }
            break;
        case CompareCriteria.GE:
        case CompareCriteria.GT:
            if (valueSet.isEmpty()) {
                if (i == 0) {
                    lower = new ArrayList<Object>(table.getPkLength());
                }
                if (lower != null) {
                    buildSearchRow(i, match, value2, lower);
                }
            }
            break;
        case CompareCriteria.LE:
        case CompareCriteria.LT:
            if (valueSet.isEmpty()) {
                if (i == 0) {
                    upper = new ArrayList<Object>(table.getPkLength());
                }
                if (upper != null) {
                    buildSearchRow(i, match, value2, upper);
                }
            }
            break;
        }
    }

    private void buildSearchRow(int i, Object match, Object value2,
            List<Object> toSearch) {
        if (toSearch.size() != i) {
            return;
        }
        if (value2 instanceof ArrayImpl && match instanceof int[]) {
            int[] indexes = (int[])match;
            ArrayImpl array = (ArrayImpl)value2;
            Object[] arrayVals = array.getValues();
            for (int j = 0; j < indexes.length; j++) {
                int index = indexes[j];
                if (index == -1) {
                    break;
                }
                toSearch.add(arrayVals[index]);
            }
        } else {
            toSearch.add(value2);
        }
    }

    void addSet(int i, Object match, Collection<Constant> values) {
        if (!valueSet.isEmpty()) {
            return;
        }
        if (i == 0) {
            for (Constant constant : values) {
                List<Object> value = new ArrayList<Object>(table.getPkLength());
                Object value2 = constant.getValue();
                buildSearchRow(i, match, value2, value);
                valueSet.add(value);
            }
            lower = null;
            upper = null;
        }
    }

    /**
     * Return a non-null direction if the index can be used, otherwise null.
     * @param orderBy
     * @return
     */
    private Boolean useIndexForOrderBy(OrderBy orderBy) {
        Boolean direction = null;
        int size = orderBy.getOrderByItems().size();
        if (size > table.getPkLength()) {
            return null;
        }
        for (int i = 0; i < size; i++) {
            OrderByItem item = orderBy.getOrderByItems().get(i);

            if (!Boolean.TRUE.equals(table.matchesPkColumn(i, item.getSymbol())) || !table.supportsOrdering(i, item.getSymbol())) {
                return null;
            }

            if (item.getNullOrdering() != null && ((item.isAscending() && item.getNullOrdering() == NullOrdering.LAST)
                    || (!item.isAscending() && item.getNullOrdering() == NullOrdering.FIRST))) {
                //assumes nulls low
                return null;
            }
            if (item.isAscending()) {
                if (direction == null) {
                    direction = OrderBy.ASC;
                } else if (direction != OrderBy.ASC) {
                    return null;
                }
            } else if (direction == null) {
                direction = OrderBy.DESC;
            } else if (direction != OrderBy.DESC) {
                return null;
            }
        }
        return direction;
    }

    public List<Object> getLower() {
        return lower;
    }

    public List<Object> getUpper() {
        return upper;
    }

    public ArrayList<List<Object>> getValueSet() {
        return valueSet;
    }

    public void sortValueSet(boolean direction, NullOrder nullOrder) {
        int size = getValueSet().get(0).size();
        int[] sortOn = new int[size];
        for (int i = 0; i <sortOn.length; i++) {
            sortOn[i] = i;
        }
        Collections.sort(getValueSet(), new ListNestedSortComparator(sortOn, direction).defaultNullOrder(nullOrder));
    }

    public Criteria getCoveredCriteria() {
        return coveredCriteria;
    }

    public Criteria getNonCoveredCriteria() {
        return nonCoveredCriteria;
    }

    public BaseIndexInfo<?> next;

}