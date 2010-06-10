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

package org.teiid.query.eval;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import net.sf.saxon.om.SequenceIterator;
import net.sf.saxon.trans.XPathException;

import org.teiid.api.exception.query.CriteriaEvaluationException;
import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.ComponentNotFoundException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.Sequencable;
import org.teiid.core.types.TransformationException;
import org.teiid.core.types.XMLType;
import org.teiid.core.types.XMLType.Type;
import org.teiid.core.types.basic.StringToSQLXMLTransform;
import org.teiid.core.util.Assertion;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.function.metadata.FunctionMethod;
import org.teiid.query.function.source.XMLSystemFunctions;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.AbstractSetCriteria;
import org.teiid.query.sql.lang.CollectionValueIterator;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.DependentSetCriteria;
import org.teiid.query.sql.lang.ExistsCriteria;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.MatchCriteria;
import org.teiid.query.sql.lang.NotCriteria;
import org.teiid.query.sql.lang.SetCriteria;
import org.teiid.query.sql.lang.SubqueryCompareCriteria;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.lang.SubquerySetCriteria;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.CaseExpression;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ContextReference;
import org.teiid.query.sql.symbol.DerivedColumn;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.QueryString;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.symbol.SearchedCaseExpression;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.symbol.XMLElement;
import org.teiid.query.sql.symbol.XMLForest;
import org.teiid.query.sql.symbol.XMLNamespaces;
import org.teiid.query.sql.symbol.XMLQuery;
import org.teiid.query.sql.symbol.XMLSerialize;
import org.teiid.query.sql.symbol.XMLNamespaces.NamespaceItem;
import org.teiid.query.sql.util.ValueIterator;
import org.teiid.query.sql.util.ValueIteratorSource;
import org.teiid.query.util.CommandContext;
import org.teiid.query.util.ErrorMessageKeys;
import org.teiid.query.xquery.saxon.SaxonXQueryExpression;
import org.teiid.translator.WSConnection.Util;


public class Evaluator {

    public static class NameValuePair<T> {
		public String name;
		public T value;
		
		public NameValuePair(String name, T value) {
			this.name = name;
			this.value = value;
		}
	}

	private final static char[] REGEX_RESERVED = new char[] {'$', '(', ')', '*', '.', '?', '[', '\\', ']', '^', '{', '|', '}'}; //in sorted order
    private final static MatchCriteria.PatternTranslator LIKE_TO_REGEX = new MatchCriteria.PatternTranslator(".*", ".", REGEX_RESERVED, '\\');  //$NON-NLS-1$ //$NON-NLS-2$

    private Map elements;
    
    protected ProcessorDataManager dataMgr;
    protected CommandContext context;
    
    public static boolean evaluate(Criteria criteria) throws CriteriaEvaluationException, BlockedException, TeiidComponentException {
    	return new Evaluator(Collections.emptyMap(), null, null).evaluate(criteria, Collections.emptyList());
    }
    
    public static Object evaluate(Expression expression) throws ExpressionEvaluationException, BlockedException, TeiidComponentException  {
    	return new Evaluator(Collections.emptyMap(), null, null).evaluate(expression, Collections.emptyList());
    }
    
    public Evaluator(Map elements, ProcessorDataManager dataMgr, CommandContext context) {
		this.context = context;
		this.dataMgr = dataMgr;
		this.elements = elements;
	}
    
    public void initialize(CommandContext context, ProcessorDataManager dataMgr) {
		this.context = context;
		this.dataMgr = dataMgr;
	}

	public boolean evaluate(Criteria criteria, List tuple)
        throws CriteriaEvaluationException, BlockedException, TeiidComponentException {

        return Boolean.TRUE.equals(evaluateTVL(criteria, tuple));
    }

    public Boolean evaluateTVL(Criteria criteria, List tuple)
        throws CriteriaEvaluationException, BlockedException, TeiidComponentException {
    	
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
		throws CriteriaEvaluationException, BlockedException, TeiidComponentException {

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
		throws CriteriaEvaluationException, BlockedException, TeiidComponentException {

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
		throws CriteriaEvaluationException, BlockedException, TeiidComponentException {

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
    	assert leftValue instanceof Comparable<?>;
    	assert rightValue instanceof Comparable<?>;
    	if (leftValue == rightValue) {
    		return 0;
    	}
        return ((Comparable<Object>)leftValue).compareTo(rightValue);
    }

	public Boolean evaluate(MatchCriteria criteria, List tuple)
		throws CriteriaEvaluationException, BlockedException, TeiidComponentException {

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
		throws CriteriaEvaluationException, BlockedException, TeiidComponentException {

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
    		Set<Object> values;
    		try {
    			values = vis.getCachedSet(ref.getValueExpression());
    		} catch (TeiidProcessingException e) {
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
			} catch (TeiidProcessingException e) {
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
		throws CriteriaEvaluationException, BlockedException, TeiidComponentException {

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
        throws CriteriaEvaluationException, BlockedException, TeiidComponentException {

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
		} catch (TeiidProcessingException e) {
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
        throws BlockedException, TeiidComponentException, CriteriaEvaluationException {

        ValueIterator valueIter;
		try {
			valueIter = evaluateSubquery(criteria, tuple);
		} catch (TeiidProcessingException e) {
			throw new CriteriaEvaluationException(e, e.getMessage());
		}
        if(valueIter.hasNext()) {
            return true;
        }
        return false;
    }
    
	public Object evaluate(Expression expression, List tuple)
		throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
	
	    try {
			return internalEvaluate(expression, tuple);
	    } catch (ExpressionEvaluationException e) {
	        throw new ExpressionEvaluationException(e, QueryPlugin.Util.getString("ExpressionEvaluator.Eval_failed", new Object[] {expression, e.getMessage()})); //$NON-NLS-1$
	    }
	}
	
	private Object internalEvaluate(Expression expression, List tuple)
	   throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
	
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
	   } else if (expression instanceof XMLElement){
		   return evaluateXMLElement(tuple, (XMLElement)expression);
	   } else if (expression instanceof XMLForest){
		   return evaluateXMLForest(tuple, (XMLForest)expression);
	   } else if (expression instanceof XMLSerialize){
		   return evaluateXMLSerialize(tuple, (XMLSerialize)expression);
	   } else if (expression instanceof XMLQuery) {
		   return evaluateXMLQuery(tuple, (XMLQuery)expression);
	   } else if (expression instanceof QueryString) {
		   return evaluateQueryString(tuple, (QueryString)expression);
	   } else {
	       throw new TeiidComponentException(ErrorMessageKeys.PROCESSOR_0016, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0016, expression.getClass().getName()));
	   }
	}

	//TODO: exception if length is too long?
	private Object evaluateQueryString(List tuple, QueryString queryString)
			throws ExpressionEvaluationException, BlockedException,
			TeiidComponentException {
		Evaluator.NameValuePair<Object>[] pairs = getNameValuePairs(tuple, queryString.getArgs(), false);
		String path = (String)internalEvaluate(queryString.getPath(), tuple);
		if (path == null) {
			path = ""; //$NON-NLS-1$
		} 
		boolean appendedAny = false;
		StringBuilder result = new StringBuilder();
		for (Evaluator.NameValuePair<Object> nameValuePair : pairs) {
			if (nameValuePair.value == null) {
				continue;
			}
			if (appendedAny) {
				result.append('&');
			}
			appendedAny = true;
			result.append(Util.httpURLEncode(nameValuePair.name)).append('=').append(Util.httpURLEncode((String)nameValuePair.value));
		}
		if (!appendedAny) {
			return path;
		}
		result.insert(0, '?');
		result.insert(0, path);
		return result.toString();
	}

	private Object evaluateXMLQuery(List tuple, XMLQuery xmlQuery)
			throws BlockedException, TeiidComponentException,
			FunctionExecutionException {
		boolean emptyOnEmpty = true;
		if (xmlQuery.getEmptyOnEmpty() != null)  {
			emptyOnEmpty = xmlQuery.getEmptyOnEmpty();
		}   
		try {
			SequenceIterator iter = evaluateXQuery(xmlQuery.getXQueryExpression(), xmlQuery.getPassing(), tuple);
			return xmlQuery.getXQueryExpression().createXMLType(iter, emptyOnEmpty);
		} catch (TeiidProcessingException e) {
			throw new FunctionExecutionException(e, QueryPlugin.Util.getString("Evaluator.xmlquery", e.getMessage())); //$NON-NLS-1$
		} catch (XPathException e) {
			throw new FunctionExecutionException(e, QueryPlugin.Util.getString("Evaluator.xmlquery", e.getMessage())); //$NON-NLS-1$
		}
	}
	
	private Object evaluateXMLSerialize(List tuple, XMLSerialize xs)
			throws ExpressionEvaluationException, BlockedException,
			TeiidComponentException, FunctionExecutionException {
		XMLType value = (XMLType) internalEvaluate(xs.getExpression(), tuple);
		if (value == null) {
			return null;
		}
		try {
			if (!xs.isDocument()) {
				return DataTypeManager.transformValue(value, xs.getType());
			}
			if (value.getType() == Type.UNKNOWN) {
				Type type = StringToSQLXMLTransform.isXml(value.getCharacterStream());
				value.setType(type);
			}
			if (value.getType() == Type.DOCUMENT || value.getType() == Type.ELEMENT) {
				return DataTypeManager.transformValue(value, xs.getType());
			}
		} catch (SQLException e) {
			throw new FunctionExecutionException(e, e.getMessage());
		} catch (TransformationException e) {
			throw new FunctionExecutionException(e, e.getMessage());
		}
		throw new FunctionExecutionException(QueryPlugin.Util.getString("Evaluator.xmlserialize")); //$NON-NLS-1$
	}

	private Object evaluateXMLForest(List tuple, XMLForest function)
			throws ExpressionEvaluationException, BlockedException,
			TeiidComponentException, FunctionExecutionException {
		List<DerivedColumn> args = function.getArgs();
		Evaluator.NameValuePair<Object>[] nameValuePairs = getNameValuePairs(tuple, args, true); 
			
		try {
			return XMLSystemFunctions.xmlForest(context, namespaces(function.getNamespaces()), nameValuePairs);
		} catch (TeiidProcessingException e) {
			throw new FunctionExecutionException(e, e.getMessage());
		}
	}

	private Object evaluateXMLElement(List tuple, XMLElement function)
			throws ExpressionEvaluationException, BlockedException,
			TeiidComponentException, FunctionExecutionException {
		List<Expression> content = function.getContent();
		   List<Object> values = new ArrayList<Object>(content.size());
		   for (Expression exp : content) {
			   values.add(internalEvaluate(exp, tuple));
		   }
		   try {
			   Evaluator.NameValuePair<Object>[] attributes = null;
			   if (function.getAttributes() != null) {
				   attributes = getNameValuePairs(tuple, function.getAttributes().getArgs(), true);
			   }
			   return XMLSystemFunctions.xmlElement(context, function.getName(), namespaces(function.getNamespaces()), attributes, values);
		   } catch (TeiidProcessingException e) {
			   throw new FunctionExecutionException(e, e.getMessage());
		   }
	}
	
	public SequenceIterator evaluateXQuery(SaxonXQueryExpression xquery, List<DerivedColumn> cols, List<?> tuple) 
	throws BlockedException, TeiidComponentException, TeiidProcessingException {
		HashMap<String, Object> parameters = new HashMap<String, Object>();
		Object contextItem = null;
		for (DerivedColumn passing : cols) {
			Object value = this.evaluate(passing.getExpression(), tuple);
			if (passing.getAlias() == null) {
				contextItem = value;
			} else {
				parameters.put(passing.getAlias(), value);
			}
		}
		return xquery.evaluateXQuery(contextItem, parameters);
	}

	private Evaluator.NameValuePair<Object>[] getNameValuePairs(List tuple, List<DerivedColumn> args, boolean xmlNames)
			throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
		Evaluator.NameValuePair<Object>[] nameValuePairs = new Evaluator.NameValuePair[args.size()];
		for (int i = 0; i < args.size(); i++) {
			DerivedColumn symbol = args.get(i);
			String name = symbol.getAlias();
			Expression ex = symbol.getExpression();
			if (name == null && ex instanceof ElementSymbol) {
				name = ((ElementSymbol)ex).getShortName();
				if (xmlNames) {
					name = XMLSystemFunctions.escapeName(name, true);
				}
			}
			nameValuePairs[i] = new Evaluator.NameValuePair<Object>(name, internalEvaluate(ex, tuple));
		}
		return nameValuePairs;
	}
	
	private Evaluator.NameValuePair<String>[] namespaces(XMLNamespaces namespaces) {
		if (namespaces == null) {
			return null;
		}
	    List<NamespaceItem> args = namespaces.getNamespaceItems();
	    Evaluator.NameValuePair<String>[] nameValuePairs = new Evaluator.NameValuePair[args.size()];
	    for(int i=0; i < args.size(); i++) {
	    	NamespaceItem item = args.get(i);
	    	nameValuePairs[i] = new Evaluator.NameValuePair<String>(item.getPrefix(), item.getUri());
	    } 
	    return nameValuePairs;
	}
	
	private Object evaluate(CaseExpression expr, List tuple)
	throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
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
	throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
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
		throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
	
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
	        throw new TeiidComponentException(QueryPlugin.Util.getString("ExpressionEvaluator.Must_push", fd.getName())); //$NON-NLS-1$
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
			} catch (TeiidProcessingException e) {
				throw new ExpressionEvaluationException(e, e.getMessage());
			}
	    }
	    
		// Execute function
		Object result = fd.invokeFunction(values);
		return result;        
	}
	
	private Object evaluate(ScalarSubquery scalarSubquery, List tuple)
	    throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
		
	    Object result = null;
        ValueIterator valueIter;
		try {
			valueIter = evaluateSubquery(scalarSubquery, tuple);
		} catch (TeiidProcessingException e) {
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
	throws TeiidProcessingException, BlockedException, TeiidComponentException {
		throw new UnsupportedOperationException("Subquery evaluation not possible with a base Evaluator"); //$NON-NLS-1$
	}

	private CommandContext getContext(LanguageObject expression) throws TeiidComponentException {
		if (context == null) {
			throw new TeiidComponentException(ErrorMessageKeys.PROCESSOR_0033, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0033, expression, "No value was available")); //$NON-NLS-1$
		}
		return context;
	}   
	    
}
