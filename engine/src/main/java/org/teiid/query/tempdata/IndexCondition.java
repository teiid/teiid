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

package org.teiid.query.tempdata;

import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.MatchCriteria;
import org.teiid.query.sql.lang.SetCriteria;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;

/**
 * Accumulates information from criteria about a specific index column.
 */
class IndexCondition {
	
	static IndexCondition[] getIndexConditions(Criteria condition, List<ElementSymbol> keyColumns) {
		List<Criteria> crits = Criteria.separateCriteriaByAnd(condition);
		IndexCondition[] conditions = new IndexCondition[keyColumns.size()];
		for (int i = 0; i < conditions.length; i++) {
			if (i > 0 && conditions[i - 1].valueSet.size() != 1) {
				break; //don't yet support any other types of composite key lookups
			}
			conditions[i] = new IndexCondition();
			ElementSymbol keyColumn = keyColumns.get(i);
			for (Iterator<Criteria> critIter = crits.iterator(); critIter.hasNext();) {
				Criteria criteria = critIter.next();
				if (criteria instanceof CompareCriteria) {
					CompareCriteria cc = (CompareCriteria)criteria;
					if (cc.getOperator() == CompareCriteria.NE 
							|| !(cc.getRightExpression() instanceof Constant) || (cc.getOperator() != CompareCriteria.EQ && i > 0)) {
						critIter.remove();
						continue;
					}
					if (!cc.getLeftExpression().equals(keyColumn)) {
						continue;
					}
					conditions[i].addCondition((Constant)cc.getRightExpression(), cc.getOperator());
					critIter.remove();
				} else if (criteria instanceof IsNullCriteria) {
					IsNullCriteria inc = (IsNullCriteria)criteria;
					if (inc.isNegated() || !inc.getExpression().equals(keyColumn)) {
						continue;
					}
					conditions[i].addCondition(new Constant(null), CompareCriteria.EQ);
					critIter.remove();
				} else {
					if (i > 0) {
						critIter.remove();
						continue;
					}
					if (criteria instanceof MatchCriteria) {
						MatchCriteria matchCriteria = (MatchCriteria)criteria;
						if (matchCriteria.isNegated() || !matchCriteria.getLeftExpression().equals(keyColumn) || !(matchCriteria.getRightExpression() instanceof Constant)) {
							continue;
						}
						Constant value = (Constant)matchCriteria.getRightExpression();
						String pattern = (String)value.getValue();
						boolean escaped = false;
						StringBuilder prefix = new StringBuilder();
						for (int j = 0; i < pattern.length(); j++) {
				            char character = pattern.charAt(j);
				            
				            if (character == matchCriteria.getEscapeChar() && character != MatchCriteria.NULL_ESCAPE_CHAR) {
				                if (escaped) {
				                    prefix.append(character);
				                    escaped = false;
				                } else {
				                    escaped = true;
				                }
				            } else if (character == MatchCriteria.WILDCARD_CHAR || character == MatchCriteria.MATCH_CHAR) {
				            	break;
				            } else {
				            	prefix.append(character);
				            }
						}
						if (prefix.length() > 0) {
							conditions[i].addCondition(new Constant(prefix.toString()), CompareCriteria.GE);
						}
					} else if (criteria instanceof SetCriteria) {
						SetCriteria setCriteria = (SetCriteria)criteria;
						if (!setCriteria.getExpression().equals(keyColumn)) {
							continue;
						}
						TreeSet<Constant> values = new TreeSet<Constant>();
						for (Expression expr : (List<? extends Expression>)setCriteria.getValues()) {
							if (!(expr instanceof Constant)) {
								continue;
							}
							values.add((Constant)expr);
						}
						conditions[i].addSet(values);
					}
				}
			}
		}
		return conditions;
	}
	
	Constant lower = null;
	Constant upper = null;
	TreeSet<Constant> valueSet = new TreeSet<Constant>();
	
	void addCondition(Constant value, int comparisionMode) {
		switch (comparisionMode) {
		case CompareCriteria.EQ:
			valueSet.clear();
			valueSet.add(value);
			lower = null;
			upper = null;
			break;
		case CompareCriteria.GE:
		case CompareCriteria.GT:
			if (valueSet.isEmpty()) {
				lower = value;
			} 
			break;
		case CompareCriteria.LE:
		case CompareCriteria.LT:
			if (valueSet.isEmpty()) {
				upper = value;
			}
			break;
		}
	}
	
	void addSet(TreeSet<Constant> values) {
		if (!valueSet.isEmpty()) {
			return;
		}
		lower = null;
		upper = null;
		valueSet.addAll(values);
	}
	
}