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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.teiid.common.buffer.TupleBrowser;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.language.Like.MatchMode;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.processor.CollectionTupleSource;
import org.teiid.query.processor.relational.ListNestedSortComparator;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.MatchCriteria;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.lang.SetCriteria;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;

/**
 * Accumulates information about index usage.
 */
class IndexInfo {
	
	List<Object> lower = null;
	List<Object> upper = null;
	ArrayList<List<Object>> valueSet = new ArrayList<List<Object>>();
	TempTable table;
	Boolean ordering;
	boolean covering;
	TupleSource valueTs;
	List<Criteria> nonCoveredCriteria = new LinkedList<Criteria>();
	List<Criteria> coveredCriteria = new LinkedList<Criteria>();
	
	public IndexInfo(TempTable table, final List<? extends SingleElementSymbol> projectedCols, final Criteria condition, OrderBy orderBy, boolean primary) {
		this.table = table;
		if (primary || this.table.getColumnMap().keySet().containsAll(projectedCols)) {
			covering = true;
		}
		if (table.getPkLength() > 0) {
			processCriteria(condition, primary);
			if (orderBy != null && (covering || this.table.getColumnMap().keySet().containsAll(orderBy.getSortKeys()))) {
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
					coveredCriteria.add(criteria);
				} else {
					covering = false;
					nonCoveredCriteria.add(criteria);
					critIter.remove();
				}
			}
		}
		for (int i = 0; i < table.getPkLength(); i++) {
			ElementSymbol keyColumn = table.getColumns().get(i);
			for (Iterator<Criteria> critIter = crits.iterator(); critIter.hasNext();) {
				Criteria criteria = critIter.next();
				if (criteria instanceof CompareCriteria) {
					CompareCriteria cc = (CompareCriteria)criteria;
					if (cc.getOperator() == CompareCriteria.NE 
							|| !(cc.getRightExpression() instanceof Constant)) {
						critIter.remove();
						continue;
					}
					if (!cc.getLeftExpression().equals(keyColumn)) {
						continue;
					}
					this.addCondition(i, (Constant)cc.getRightExpression(), cc.getOperator());
					critIter.remove();
				} else if (criteria instanceof IsNullCriteria) {
					IsNullCriteria inc = (IsNullCriteria)criteria;
					if (inc.isNegated() || !inc.getExpression().equals(keyColumn)) {
						continue;
					}
					this.addCondition(i, new Constant(null), CompareCriteria.EQ);
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
						char escapeChar = matchCriteria.getEscapeChar();
						if (matchCriteria.getMode() == MatchMode.REGEX) {
							escapeChar = '\\';
						}
						StringBuilder prefix = new StringBuilder();
						
			            if (pattern.length() > 0 && matchCriteria.getMode() == MatchMode.REGEX && pattern.charAt(0) != '^') {
			            	//make the assumption that we require an anchor
			            	continue;
			            }

						for (int j = matchCriteria.getMode() == MatchMode.REGEX?1:0; i < pattern.length(); j++) {
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
							this.addCondition(i, new Constant(prefix.toString()), CompareCriteria.GE);
						}
					} else if (criteria instanceof SetCriteria) {
						SetCriteria setCriteria = (SetCriteria)criteria;
						if (!setCriteria.getExpression().equals(keyColumn) || !setCriteria.isAllConstants()) {
							continue;
						}
						Collection<Constant> values = (Collection<Constant>) setCriteria.getValues();
						this.addSet(i, values);
					}
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
	
	void addCondition(int i, Constant value, int comparisionMode) {
		switch (comparisionMode) {
		case CompareCriteria.EQ:
			if (i == 0) {
				valueSet.clear();
				valueSet.add(new ArrayList<Object>(table.getPkLength()));
			} 
			if (valueSet.size() == 1) {
				valueSet.get(0).add(value.getValue());
			}
			lower = null;
			upper = null;
			break;
		case CompareCriteria.GE:
		case CompareCriteria.GT:
			if (valueSet.isEmpty()) {
				if (i == 0) {
					lower = new ArrayList<Object>(table.getPkLength());
					lower.add(value.getValue());
				} if (lower != null && lower.size() == i) {
					lower.add(value.getValue());
				}
			} 
			break;
		case CompareCriteria.LE:
		case CompareCriteria.LT:
			if (valueSet.isEmpty()) {
				if (i == 0) {
					upper = new ArrayList<Object>(table.getPkLength());
					upper.add(value.getValue());
				} else if (upper != null && upper.size() == i) {
					upper.add(value.getValue());
				}
			}
			break;
		}
	}
	
	void addSet(int i, Collection<Constant> values) {
		if (!valueSet.isEmpty()) {
			return;
		}
		if (i == 0) {
			for (Constant constant : values) {
				List<Object> value = new ArrayList<Object>(table.getPkLength());
				value.add(constant.getValue());
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
		int[] orderByIndexes = RelationalNode.getProjectionIndexes(this.table.getColumnMap(), orderBy.getSortKeys());
		for (int i = 0; i < table.getPkLength(); i++) {
			if (orderByIndexes.length <= i) {
				break;
			}
			if (orderByIndexes[i] != i) {
				return null;
			}
		}
		for (OrderByItem item : orderBy.getOrderByItems()) {
			if (item.getNullOrdering() != null) {
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
	
	TupleBrowser createTupleBrowser() throws TeiidComponentException {
		boolean direction = OrderBy.ASC;
		if (ordering != null) {
			LogManager.logDetail(LogConstants.CTX_DQP, "Using index for ordering"); //$NON-NLS-1$
			direction = ordering;
		}
		if (valueTs != null) {
			LogManager.logDetail(LogConstants.CTX_DQP, "Using index value set"); //$NON-NLS-1$
			return new TupleBrowser(this.table.getTree(), valueTs, direction);
		}
		if (!valueSet.isEmpty()) {
			LogManager.logDetail(LogConstants.CTX_DQP, "Using index value set"); //$NON-NLS-1$
			sortValueSet(direction);
			CollectionTupleSource cts = new CollectionTupleSource(valueSet.iterator());
			return new TupleBrowser(this.table.getTree(), cts, direction);
		}
		if (lower != null || upper != null) {
			LogManager.logDetail(LogConstants.CTX_DQP, "Using index for range query", lower, upper); //$NON-NLS-1$
		} 
		return new TupleBrowser(this.table.getTree(), lower, upper, direction);
	}
	
	public void sortValueSet(boolean direction) {
		int size = valueSet.get(0).size();
		int[] sortOn = new int[size];
		for (int i = 0; i <sortOn.length; i++) {
			sortOn[i] = i;
		}
		Collections.sort(valueSet, new ListNestedSortComparator(sortOn, direction));
	}
	
}