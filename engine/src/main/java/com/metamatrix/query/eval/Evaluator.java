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

package com.metamatrix.query.eval;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.CriteriaEvaluationException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.types.Sequencable;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.function.FunctionDescriptor;
import com.metamatrix.query.function.FunctionLibrary;
import com.metamatrix.query.function.FunctionLibraryManager;
import com.metamatrix.query.function.metadata.FunctionMethod;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.lang.AbstractSetCriteria;
import com.metamatrix.query.sql.lang.CollectionValueIterator;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.CompoundCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.DependentSetCriteria;
import com.metamatrix.query.sql.lang.ExistsCriteria;
import com.metamatrix.query.sql.lang.IsNullCriteria;
import com.metamatrix.query.sql.lang.MatchCriteria;
import com.metamatrix.query.sql.lang.NotCriteria;
import com.metamatrix.query.sql.lang.SetCriteria;
import com.metamatrix.query.sql.lang.SubqueryCompareCriteria;
import com.metamatrix.query.sql.lang.SubqueryContainer;
import com.metamatrix.query.sql.lang.SubquerySetCriteria;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.CaseExpression;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ContextReference;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.ExpressionSymbol;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.symbol.ScalarSubquery;
import com.metamatrix.query.sql.symbol.SearchedCaseExpression;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.util.ValueIterator;
import com.metamatrix.query.sql.util.ValueIteratorSource;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.ErrorMessageKeys;

public class Evaluator {

    private final static char[] REGEX_RESERVED = new char[] {'$', '(', ')', '*', '.', '?', '[', '\\', ']', '^', '{', '|', '}'}; //in sorted order
    private final static MatchCriteria.PatternTranslator LIKE_TO_REGEX = new MatchCriteria.PatternTranslator(".*", ".", REGEX_RESERVED, '\\');  //$NON-NLS-1$ //$NON-NLS-2$

    private Map elements;
    
    protected ProcessorDataManager dataMgr;
    protected CommandContext context;
    
    public static boolean evaluate(Criteria criteria) throws CriteriaEvaluationException, BlockedException, MetaMatrixComponentException {
    	return new Evaluator(Collections.emptyMap(), null, null).evaluate(criteria, Collections.emptyList());
    }
    
    public static Object evaluate(Expression expression) throws ExpressionEvaluationException, BlockedException, MetaMatrixComponentException  {
    	return new Evaluator(Collections.emptyMap(), null, null).evaluate(expression, Collections.emptyList());
    }
    
    public Evaluator(Map elements, ProcessorDataManager dataMgr, CommandContext context) {
		this.context = context;
		this.dataMgr = dataMgr;
		this.elements = elements;
	}
    
    public void setContext(CommandContext context) {
		this.context = context;
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

		boolean and = criteria.getOperator() == CompoundCriteria.AND;
        Boolean result = and?Boolean.TRUE:Boolean.FALSE;
		while(subCritIter.hasNext()) {
			Criteria subCrit = (Criteria) subCritIter.next();
			Boolean value = evaluateTVL(subCrit, tuple);
            if (value == null) {
				result = null;
			} else if (!value.booleanValue()) {
				if (and) {
					return Boolean.FALSE;
				}
            } else if (!and) {
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
			leftValue = evaluate(criteria.getLeftExpression(), tuple);
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
			rightValue = evaluate(criteria.getRightExpression(), tuple);
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
				return Boolean.valueOf(compareValues(leftValue, rightValue) == 0);
			case CompareCriteria.NE:
				return Boolean.valueOf(compareValues(leftValue, rightValue) != 0);
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
			value = evaluate(criteria.getLeftExpression(), tuple);
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
			rightValue = (String) evaluate(criteria.getRightExpression(), tuple);
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

	private Boolean evaluate(AbstractSetCriteria criteria, List tuple)
		throws CriteriaEvaluationException, BlockedException, MetaMatrixComponentException {

		// Evaluate expression
		Object leftValue = null;
		try {
			leftValue = evaluate(criteria.getExpression(), tuple);
		} catch(ExpressionEvaluationException e) {
            throw new CriteriaEvaluationException(e, ErrorMessageKeys.PROCESSOR_0015, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0015, criteria));
		}

		// Shortcut if null
		if(leftValue == null) {
            return null;
        }
        Boolean result = Boolean.FALSE;

        ValueIterator valueIter = null;
        if (criteria instanceof SetCriteria) {
        	valueIter = new CollectionValueIterator(((SetCriteria)criteria).getValues());
        } else if (criteria instanceof DependentSetCriteria){
        	ContextReference ref = (ContextReference)criteria;
        	ValueIteratorSource vis = (ValueIteratorSource)getContext(criteria).getVariableContext().getGlobalValue(ref.getContextSymbol());
        	HashSet<Object> values;
			try {
				values = vis.getCachedSet(ref.getValueExpression());
			} catch (MetaMatrixProcessingException e) {
				throw new CriteriaEvaluationException(e, e.getMessage());
			}
        	if (values != null) {
        		return values.contains(leftValue);
        	}
        	//there are too many values to justify a linear search or holding
        	//them in memory
        	return true;
        } else if (criteria instanceof SubquerySetCriteria) {
        	try {
				valueIter = evaluateSubquery((SubquerySetCriteria)criteria, tuple);
			} catch (MetaMatrixProcessingException e) {
				throw new CriteriaEvaluationException(e, e.getMessage());
			}
        } else {
        	Assertion.failed("unknown set criteria type"); //$NON-NLS-1$
        }
        while(valueIter.hasNext()) {
            Object possibleValue = valueIter.next();
            Object value = null;
            if(possibleValue instanceof Expression) {
    			try {
    				value = evaluate((Expression) possibleValue, tuple);
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
			value = evaluate(criteria.getExpression(), tuple);
		} catch(ExpressionEvaluationException e) {
            throw new CriteriaEvaluationException(e, ErrorMessageKeys.PROCESSOR_0015, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0015, criteria));
		}

		return (value == null ^ criteria.isNegated());
	}

    private Boolean evaluate(SubqueryCompareCriteria criteria, List tuple)
        throws CriteriaEvaluationException, BlockedException, MetaMatrixComponentException {

        // Evaluate expression
        Object leftValue = null;
        try {
            leftValue = evaluate(criteria.getLeftExpression(), tuple);
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

        ValueIterator valueIter;
		try {
			valueIter = evaluateSubquery(criteria, tuple);
		} catch (MetaMatrixProcessingException e) {
			throw new CriteriaEvaluationException(e, e.getMessage());
		}
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
        throws BlockedException, MetaMatrixComponentException, CriteriaEvaluationException {

        ValueIterator valueIter;
		try {
			valueIter = evaluateSubquery(criteria, tuple);
		} catch (MetaMatrixProcessingException e) {
			throw new CriteriaEvaluationException(e, e.getMessage());
		}
        if(valueIter.hasNext()) {
            return true;
        }
        return false;
    }
    
	public Object evaluate(Expression expression, List tuple)
		throws ExpressionEvaluationException, BlockedException, MetaMatrixComponentException {
	
	    try {
			return internalEvaluate(expression, tuple);
	    } catch (ExpressionEvaluationException e) {
	        throw new ExpressionEvaluationException(e, QueryPlugin.Util.getString("ExpressionEvaluator.Eval_failed", new Object[] {expression, e.getMessage()})); //$NON-NLS-1$
	    }
	}
	
	private Object internalEvaluate(Expression expression, List tuple)
	   throws ExpressionEvaluationException, BlockedException, MetaMatrixComponentException {
	
	   if(expression instanceof SingleElementSymbol) {
		   if (elements != null) {
		       // Try to evaluate by lookup in the elements map (may work for both ElementSymbol and ExpressionSymbol
		       Integer index = (Integer) elements.get(expression);
		       if(index != null) {
		           return tuple.get(index.intValue());
		       }
		   }
		   
	       // Otherwise this should be an ExpressionSymbol and we just need to dive in and evaluate the expression itself
	       if (expression instanceof ExpressionSymbol && !(expression instanceof AggregateSymbol)) {            
	           ExpressionSymbol exprSyb = (ExpressionSymbol) expression;
	           Expression expr = exprSyb.getExpression();
	           return internalEvaluate(expr, tuple);
	       } 
	       
	       return getContext(expression).getFromContext(expression);
	   } 
	   if(expression instanceof Constant) {
	       return ((Constant) expression).getValue();
	   } else if(expression instanceof Function) {
	       return evaluate((Function) expression, tuple);
	   } else if(expression instanceof CaseExpression) {
	       return evaluate((CaseExpression) expression, tuple);
	   } else if(expression instanceof SearchedCaseExpression) {
	       return evaluate((SearchedCaseExpression) expression, tuple);
	   } else if(expression instanceof Reference) {
		   Reference ref = (Reference)expression;
		   if (ref.isPositional() && ref.getExpression() == null) {
			   return getContext(ref).getVariableContext().getGlobalValue(ref.getContextSymbol());
		   }
		   return internalEvaluate(ref.getExpression(), tuple);
	   } else if(expression instanceof ScalarSubquery) {
	       return evaluate((ScalarSubquery) expression, tuple);
	   } else {
	       throw new MetaMatrixComponentException(ErrorMessageKeys.PROCESSOR_0016, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0016, expression.getClass().getName()));
	   }
	}
	
	private Object evaluate(CaseExpression expr, List tuple)
	throws ExpressionEvaluationException, BlockedException, MetaMatrixComponentException {
	    Object exprVal = internalEvaluate(expr.getExpression(), tuple);
	    for (int i = 0; i < expr.getWhenCount(); i++) {
	        if (EquivalenceUtil.areEqual(exprVal, internalEvaluate(expr.getWhenExpression(i), tuple))) {
	            return internalEvaluate(expr.getThenExpression(i), tuple);
	        }
	    }
	    if (expr.getElseExpression() != null) {
	        return internalEvaluate(expr.getElseExpression(), tuple);
	    }
	    return null;
	}
	
	private Object evaluate(SearchedCaseExpression expr, List tuple)
	throws ExpressionEvaluationException, BlockedException, MetaMatrixComponentException {
	    for (int i = 0; i < expr.getWhenCount(); i++) {
	        try {
	            if (evaluate(expr.getWhenCriteria(i), tuple)) {
	                return internalEvaluate(expr.getThenExpression(i), tuple);
	            }
	        } catch (CriteriaEvaluationException e) {
	            throw new ExpressionEvaluationException(e, ErrorMessageKeys.PROCESSOR_0033, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0033, "CASE", expr.getWhenCriteria(i))); //$NON-NLS-1$
	        }
	    }
	    if (expr.getElseExpression() != null) {
	        return internalEvaluate(expr.getElseExpression(), tuple);
	    }
	    return null;
	}
	
	private Object evaluate(Function function, List tuple)
		throws ExpressionEvaluationException, BlockedException, MetaMatrixComponentException {
	
	    // Get function based on resolved function info
	    FunctionDescriptor fd = function.getFunctionDescriptor();
	    
		// Evaluate args
		Expression[] args = function.getArgs();
	    Object[] values = null;
	    int start = 0;
	    
	    if (fd.requiresContext()) {
			values = new Object[args.length+1];
	        values[0] = context;
	        start = 1;
	    }
	    else {
	        values = new Object[args.length];
	    }
	    
	    for(int i=0; i < args.length; i++) {
	        values[i+start] = internalEvaluate(args[i], tuple);
	    }            
	    
	    // Check for function we can't evaluate
	    if(fd.getPushdown() == FunctionMethod.MUST_PUSHDOWN) {
	        throw new MetaMatrixComponentException(QueryPlugin.Util.getString("ExpressionEvaluator.Must_push", fd.getName())); //$NON-NLS-1$
	    }
	
	    // Check for special lookup function
	    if(fd.getName().equalsIgnoreCase(FunctionLibrary.LOOKUP)) {
	        if(dataMgr == null) {
	            throw new ComponentNotFoundException(ErrorMessageKeys.PROCESSOR_0055, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0055));
	        }
	
	        String codeTableName = (String) values[0];
	        String returnElementName = (String) values[1];
	        String keyElementName = (String) values[2];
	        
	        try {
				return dataMgr.lookupCodeValue(context, codeTableName, returnElementName, keyElementName, values[3]);
			} catch (MetaMatrixProcessingException e) {
				throw new ExpressionEvaluationException(e, e.getMessage());
			}
	    } 
	    
		// Execute function
		FunctionLibrary library = FunctionLibraryManager.getFunctionLibrary();
		Object result = library.invokeFunction(fd, values);
		return result;        
	}
	
	private Object evaluate(ScalarSubquery scalarSubquery, List tuple)
	    throws ExpressionEvaluationException, BlockedException, MetaMatrixComponentException {
		
	    Object result = null;
        ValueIterator valueIter;
		try {
			valueIter = evaluateSubquery(scalarSubquery, tuple);
		} catch (MetaMatrixProcessingException e) {
			throw new ExpressionEvaluationException(e, e.getMessage());
		}
	    if(valueIter.hasNext()) {
	        result = valueIter.next();
	        if(valueIter.hasNext()) {
	            // The subquery should be scalar, but has produced
	            // more than one result value - this is an exception case
	            throw new ExpressionEvaluationException(ErrorMessageKeys.PROCESSOR_0058, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0058, scalarSubquery.getCommand()));
	        }
	    }
	    return result;
	}
	
	protected ValueIterator evaluateSubquery(SubqueryContainer container, List tuple) 
	throws MetaMatrixProcessingException, BlockedException, MetaMatrixComponentException {
		throw new UnsupportedOperationException("Subquery evaluation not possible with a base Evaluator"); //$NON-NLS-1$
	}

	private CommandContext getContext(LanguageObject expression) throws MetaMatrixComponentException {
		if (context == null) {
			throw new MetaMatrixComponentException(ErrorMessageKeys.PROCESSOR_0033, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0033, expression, "No value was available")); //$NON-NLS-1$
		}
		return context;
	}   
	    
}
