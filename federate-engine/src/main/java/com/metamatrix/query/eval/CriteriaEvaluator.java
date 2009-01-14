/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.query.eval;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.CriteriaEvaluationException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.types.Sequencable;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.sql.lang.AbstractSetCriteria;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.CompoundCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.ExistsCriteria;
import com.metamatrix.query.sql.lang.IsNullCriteria;
import com.metamatrix.query.sql.lang.MatchCriteria;
import com.metamatrix.query.sql.lang.NotCriteria;
import com.metamatrix.query.sql.lang.SubqueryCompareCriteria;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.util.ValueIterator;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.ErrorMessageKeys;

public class CriteriaEvaluator {

    private final static char[] REGEX_RESERVED = new char[] {'$', '(', ')', '*', '.', '?', '[', '\\', ']', '^', '{', '|', '}'}; //in sorted order
    private final static MatchCriteria.PatternTranslator LIKE_TO_REGEX = new MatchCriteria.PatternTranslator(".*", ".", REGEX_RESERVED, '\\');  //$NON-NLS-1$ //$NON-NLS-2$

    private Map elements;
    private LookupEvaluator dataMgr;
    private CommandContext context;
    
    public CriteriaEvaluator(Map elements, LookupEvaluator dataMgr, CommandContext context) {
		this.context = context;
		this.dataMgr = dataMgr;
		this.elements = elements;
	}

	public boolean evaluate(Criteria criteria, List tuple)
        throws CriteriaEvaluationException, BlockedException, MetaMatrixComponentException {

        return Boolean.TRUE.equals(evaluateTVL(criteria, tuple));
    }

    public Boolean evaluateTVL(Criteria criteria, List tuple)
        throws CriteriaEvaluationException, BlockedException, MetaMatrixComponentException {
    	
		if(criteria instanceof CompoundCriteria) {
			return evaluate((CompoundCriteria)criteria, tuple);
		} else if(criteria instanceof NotCriteria) {
			return evaluate((NotCriteria)criteria, tuple);
		} else if(criteria instanceof CompareCriteria) {
			return evaluate((CompareCriteria)criteria, tuple);
		} else if(criteria instanceof MatchCriteria) {
			return evaluate((MatchCriteria)criteria, tuple);
		} else if(criteria instanceof AbstractSetCriteria) {
			return evaluate((AbstractSetCriteria)criteria, tuple);
		} else if(criteria instanceof IsNullCriteria) {
			return Boolean.valueOf(evaluate((IsNullCriteria)criteria, tuple));
        } else if(criteria instanceof SubqueryCompareCriteria) {
            return evaluate((SubqueryCompareCriteria)criteria, tuple);
        } else if(criteria instanceof ExistsCriteria) {
            return Boolean.valueOf(evaluate((ExistsCriteria)criteria, tuple));
		} else {
            throw new CriteriaEvaluationException(ErrorMessageKeys.PROCESSOR_0010, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0010, criteria));
		}
	}

	public Boolean evaluate(CompoundCriteria criteria, List tuple)
		throws CriteriaEvaluationException, BlockedException, MetaMatrixComponentException {

		List subCrits = criteria.getCriteria();
		Iterator subCritIter = subCrits.iterator();

		if(criteria.getOperator() == CompoundCriteria.AND) {
            Boolean result = Boolean.TRUE;
			while(subCritIter.hasNext()) {
				Criteria subCrit = (Criteria) subCritIter.next();
				Boolean value = evaluateTVL(subCrit, tuple);
                if (value == null) {
					result = null;
				} else if (!value.booleanValue()) {
                    return Boolean.FALSE;
                }
			}
			return result;

		}
		// CompoundCriteria.OR
        Boolean result = Boolean.FALSE;
		while(subCritIter.hasNext()) {
			Criteria subCrit = (Criteria) subCritIter.next();
			Boolean value = evaluateTVL(subCrit, tuple);
			if (value == null) {
                result = null;
                continue;
            } 
            if (value.booleanValue()) {
                return Boolean.TRUE;
            }
		}
        
		return result;
	}

	public Boolean evaluate(NotCriteria criteria, List tuple)
		throws CriteriaEvaluationException, BlockedException, MetaMatrixComponentException {

		Criteria subCrit = criteria.getCriteria();
		Boolean result = evaluateTVL(subCrit, tuple);
        if (result == null) {
            return null;
        }
        if (result.booleanValue()) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
	}

	public Boolean evaluate(CompareCriteria criteria, List tuple)
		throws CriteriaEvaluationException, BlockedException, MetaMatrixComponentException {

		// Evaluate left expression
		Object leftValue = null;
		try {
			leftValue = ExpressionEvaluator.evaluate(criteria.getLeftExpression(), elements, tuple, dataMgr, context);
		} catch(ExpressionEvaluationException e) {
            throw new CriteriaEvaluationException(e, ErrorMessageKeys.PROCESSOR_0011, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0011, new Object[] {"left", criteria})); //$NON-NLS-1$
		}

		// Shortcut if null
		if(leftValue == null) {
			return null;
		}

		// Evaluate right expression
		Object rightValue = null;
		try {
			rightValue = ExpressionEvaluator.evaluate(criteria.getRightExpression(), elements, tuple, dataMgr, context);
		} catch(ExpressionEvaluationException e) {
            throw new CriteriaEvaluationException(e, ErrorMessageKeys.PROCESSOR_0011, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0011, new Object[]{"right", criteria})); //$NON-NLS-1$
		}

		// Shortcut if null
		if(rightValue == null) {
			return null;
		}

		// Compare two non-null values using specified operator
		switch(criteria.getOperator()) {
			case CompareCriteria.EQ:
				return Boolean.valueOf(leftValue.equals(rightValue));
			case CompareCriteria.NE:
				return Boolean.valueOf(! leftValue.equals(rightValue));
			case CompareCriteria.LT:
				return Boolean.valueOf((compareValues(leftValue, rightValue) < 0));
			case CompareCriteria.LE:
				return Boolean.valueOf((compareValues(leftValue, rightValue) <= 0));
			case CompareCriteria.GT:
				return Boolean.valueOf((compareValues(leftValue, rightValue) > 0));
			case CompareCriteria.GE:
				return Boolean.valueOf((compareValues(leftValue, rightValue) >= 0));
			default:
                throw new CriteriaEvaluationException(ErrorMessageKeys.PROCESSOR_0012, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0012, criteria.getOperator()));
		}
	}

    private final int compareValues(Object leftValue, Object rightValue) {
    	ArgCheck.isInstanceOf(Comparable.class, leftValue);
    	ArgCheck.isInstanceOf(Comparable.class, rightValue);
        return ((Comparable)leftValue).compareTo(rightValue);
    }

	public Boolean evaluate(MatchCriteria criteria, List tuple)
		throws CriteriaEvaluationException, BlockedException, MetaMatrixComponentException {

        boolean result = false;
		// Evaluate left expression
        Object value = null;
		try {
			value = ExpressionEvaluator.evaluate(criteria.getLeftExpression(), elements, tuple, dataMgr, context);
		} catch(ExpressionEvaluationException e) {
            throw new CriteriaEvaluationException(e, ErrorMessageKeys.PROCESSOR_0011, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0011, new Object[]{"left", criteria})); //$NON-NLS-1$
		}

		// Shortcut if null
		if(value == null) {
            return null;
        }
        
        CharSequence leftValue = null;
        
        if (value instanceof CharSequence) {
            leftValue = (CharSequence)value;
        } else {
            try {
                leftValue = ((Sequencable)value).getCharSequence();
            } catch (SQLException err) {
                throw new CriteriaEvaluationException(err, err.getMessage()); 
            }
        }

		// Evaluate right expression
		String rightValue = null;
		try {
			rightValue = (String) ExpressionEvaluator.evaluate(criteria.getRightExpression(), elements, tuple, dataMgr, context);
		} catch(ExpressionEvaluationException e) {
            throw new CriteriaEvaluationException(e, ErrorMessageKeys.PROCESSOR_0011, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0011, new Object[]{"right", criteria})); //$NON-NLS-1$
		}

		// Shortcut if null
		if(rightValue == null) {
            return null;
        }
        
        result = match(rightValue, criteria.getEscapeChar(), leftValue);
        
        return Boolean.valueOf(result ^ criteria.isNegated());
	}

	private boolean match(String pattern, char escape, CharSequence search)
		throws CriteriaEvaluationException {

		StringBuffer rePattern = LIKE_TO_REGEX.translate(pattern, escape);
		
		// Insert leading and trailing characters to ensure match of full string
		rePattern.insert(0, '^');
		rePattern.append('$');

		try {
            Pattern patternRegex = Pattern.compile(rePattern.toString(), Pattern.DOTALL);
            Matcher matcher = patternRegex.matcher(search);
            return matcher.matches();
		} catch(PatternSyntaxException e) {
            throw new CriteriaEvaluationException(e, ErrorMessageKeys.PROCESSOR_0014, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0014, new Object[]{pattern, e.getMessage()}));
		}
	}

	public Boolean evaluate(AbstractSetCriteria criteria, List tuple)
		throws CriteriaEvaluationException, BlockedException, MetaMatrixComponentException {

		// Evaluate expression
		Object leftValue = null;
		try {
			leftValue = ExpressionEvaluator.evaluate(criteria.getExpression(), elements, tuple, dataMgr, context);
		} catch(ExpressionEvaluationException e) {
            throw new CriteriaEvaluationException(e, ErrorMessageKeys.PROCESSOR_0015, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0015, criteria));
		}

		// Shortcut if null
		if(leftValue == null) {
            return null;
        }
        Boolean result = Boolean.FALSE;
		ValueIterator valueIter = criteria.getValueIterator();
        while(valueIter.hasNext()) {
            Object possibleValue = valueIter.next();
            Object value = null;
            if(possibleValue instanceof Expression) {
    			try {
    				value = ExpressionEvaluator.evaluate((Expression) possibleValue, elements, tuple, dataMgr, context);
    			} catch(ExpressionEvaluationException e) {
                    throw new CriteriaEvaluationException(e, ErrorMessageKeys.PROCESSOR_0015, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0015, possibleValue));
    			}
            } else {
                value = possibleValue;
            }

			if(value != null) {
				if(leftValue.equals(value)) {
					return Boolean.valueOf(!criteria.isNegated());
				} // else try next value
			} else {
			    result = null;
            }
		}
        
        if (result == null) {
            return null;
        }
        
        return Boolean.valueOf(criteria.isNegated());
	}

	public boolean evaluate(IsNullCriteria criteria, List tuple)
		throws CriteriaEvaluationException, BlockedException, MetaMatrixComponentException {

		// Evaluate expression
		Object value = null;
		try {
			value = ExpressionEvaluator.evaluate(criteria.getExpression(), elements, tuple, dataMgr, context);
		} catch(ExpressionEvaluationException e) {
            throw new CriteriaEvaluationException(e, ErrorMessageKeys.PROCESSOR_0015, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0015, criteria));
		}

		return (value == null ^ criteria.isNegated());
	}

    public Boolean evaluate(SubqueryCompareCriteria criteria, List tuple)
        throws CriteriaEvaluationException, BlockedException, MetaMatrixComponentException {

        // Evaluate expression
        Object leftValue = null;
        try {
            leftValue = ExpressionEvaluator.evaluate(criteria.getLeftExpression(), elements, tuple, dataMgr, context);
        } catch(ExpressionEvaluationException e) {
            throw new CriteriaEvaluationException(e, ErrorMessageKeys.PROCESSOR_0015, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0015, criteria));
        }

        // Shortcut if null
        if(leftValue == null) {
            return null;
        }

        // Need to be careful to initialize this variable carefully for the case
        // where valueIterator has no values, and the block below is not entered.
        // If there are no rows, and ALL is the predicate quantifier, the result
        // should be true.  If SOME is the predicate quantifier, or no quantifier
        // is used, the result should be false.
        Boolean result = Boolean.FALSE;
        if (criteria.getPredicateQuantifier() == SubqueryCompareCriteria.ALL){
            result = Boolean.TRUE;
        }

        ValueIterator valueIter = criteria.getValueIterator();
        while(valueIter.hasNext()) {
            Object value = valueIter.next();

            if(value != null) {

                // Compare two non-null values using specified operator
                switch(criteria.getOperator()) {
                    case SubqueryCompareCriteria.EQ:
                        result = Boolean.valueOf(leftValue.equals(value));
                        break;
                    case SubqueryCompareCriteria.NE:
                        result = Boolean.valueOf(!leftValue.equals(value));
                        break;
                    case SubqueryCompareCriteria.LT:
                        result = Boolean.valueOf((compareValues(leftValue, value) < 0));
                        break;
                    case SubqueryCompareCriteria.LE:
                        result = Boolean.valueOf((compareValues(leftValue, value) <= 0));
                        break;
                    case SubqueryCompareCriteria.GT:
                        result = Boolean.valueOf((compareValues(leftValue, value) > 0));
                        break;
                    case SubqueryCompareCriteria.GE:
                        result = Boolean.valueOf((compareValues(leftValue, value) >= 0));
                        break;
                    default:
                        throw new CriteriaEvaluationException(ErrorMessageKeys.PROCESSOR_0012, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0012, criteria.getOperator()));
                }

                switch(criteria.getPredicateQuantifier()) {
                    case SubqueryCompareCriteria.ALL:
                        if (Boolean.FALSE.equals(result)){
                            return Boolean.FALSE;
                        }
                        break;
                    case SubqueryCompareCriteria.SOME:
                        if (Boolean.TRUE.equals(result)){
                            return Boolean.TRUE;
                        }
                        break;
                    case SubqueryCompareCriteria.NO_QUANTIFIER:
                        if (valueIter.hasNext()){
                            // The subquery should be scalar, but has produced
                            // more than one result value - this is an exception case
                            throw new CriteriaEvaluationException(ErrorMessageKeys.PROCESSOR_0056, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0056, criteria));
                        }
                        return result;
                    default:
                        throw new CriteriaEvaluationException(ErrorMessageKeys.PROCESSOR_0057, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0057, criteria.getPredicateQuantifier()));
                }

            } else { // value is null

                switch(criteria.getPredicateQuantifier()) {
                    case SubqueryCompareCriteria.ALL:
                        // null counts as unknown; one unknown means the whole thing is unknown
                        return null;
                    case SubqueryCompareCriteria.SOME:
                        result = null;
                        break;
                    case SubqueryCompareCriteria.NO_QUANTIFIER:
                        if (valueIter.hasNext()){
                            // The subquery should be scalar, but has produced
                            // more than one result value - this is an exception case
                            throw new CriteriaEvaluationException(ErrorMessageKeys.PROCESSOR_0056, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0056, criteria));
                        }
                        // null value means unknown for the single-value comparison
                        return null;
                    default:
                        throw new CriteriaEvaluationException(ErrorMessageKeys.PROCESSOR_0057, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0057, criteria.getPredicateQuantifier()));
                }
            }


        } //end value iteration

        return result;
    }

    public boolean evaluate(ExistsCriteria criteria, List tuple)
        throws BlockedException, MetaMatrixComponentException {

        ValueIterator valueIter = criteria.getValueIterator();
        if(valueIter.hasNext()) {
            return true;
        }
        return false;
    }
    
    public static boolean evaluate(Criteria criteria) throws CriteriaEvaluationException, BlockedException, MetaMatrixComponentException {
    	return new CriteriaEvaluator(Collections.emptyMap(), null, null).evaluate(criteria, Collections.emptyList());
    }

}
