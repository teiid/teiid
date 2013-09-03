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

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.transform.stream.StreamResult;

import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.query.QueryResult;
import net.sf.saxon.trans.XPathException;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.client.SourceWarning;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.ComponentNotFoundException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.*;
import org.teiid.core.types.XMLType.Type;
import org.teiid.core.types.basic.StringToSQLXMLTransform;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.language.Like.MatchMode;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.function.JSONFunctionMethods.JSONBuilder;
import org.teiid.query.function.source.XMLSystemFunctions;
import org.teiid.query.function.source.XMLSystemFunctions.XmlConcat;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.proc.ExceptionExpression;
import org.teiid.query.sql.symbol.*;
import org.teiid.query.sql.symbol.XMLNamespaces.NamespaceItem;
import org.teiid.query.sql.util.ValueIterator;
import org.teiid.query.sql.util.ValueIteratorSource;
import org.teiid.query.sql.util.VariableContext;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.util.CommandContext;
import org.teiid.query.xquery.saxon.SaxonXQueryExpression;
import org.teiid.query.xquery.saxon.SaxonXQueryExpression.Result;
import org.teiid.query.xquery.saxon.SaxonXQueryExpression.RowProcessor;
import org.teiid.query.xquery.saxon.XQueryEvaluator;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.WSConnection.Util;

public class Evaluator {

    private final class XMLQueryRowProcessor implements RowProcessor {
		XmlConcat concat; //just used to get a writer
		Type type;
		private javax.xml.transform.Result result;
		
		private XMLQueryRowProcessor() throws TeiidProcessingException {
			concat = new XmlConcat(context.getBufferManager());
			result = new StreamResult(concat.getWriter());
		}

		@Override
		public void processRow(NodeInfo row) {
			if (type == null) {
				type = SaxonXQueryExpression.getType(row);
			} else {
				type = Type.CONTENT;
			}
			try {
				QueryResult.serialize(row, result, SaxonXQueryExpression.DEFAULT_OUTPUT_PROPERTIES);
			} catch (XPathException e) {
				 throw new TeiidRuntimeException(e);
			}
		}
	}

	private final class SequenceReader extends Reader {
		private LinkedList<Reader> readers;
		private Reader current = null;
		
		public SequenceReader(LinkedList<Reader> readers) {
			this.readers = readers;
		}

		@Override
		public void close() throws IOException {
			for (Reader reader : readers) {
				try {
					reader.close();
				} catch (IOException e) {
					
				}
			}
		}

		@Override
		public int read(char[] cbuf, int off, int len)
				throws IOException {
			if (current == null && !readers.isEmpty()) {
				current = readers.removeFirst();
			}
			if (current == null) {
				return -1;
			}
			int read = current.read(cbuf, off, len);
			if (read == -1) {
				current.close();
				current = null;
				read = 0;
			} 
			if (read < len) {
				int nextRead = read(cbuf, off + read, len - read);
				if (nextRead > 0) {
					read += nextRead;
				}
			}
			return read;
		}
	}

	public static class NameValuePair<T> {
		public String name;
		public T value;
		
		public NameValuePair(String name, T value) {
			this.name = name;
			this.value = value;
		}
	}

	public final static char[] REGEX_RESERVED = new char[] {'$', '(', ')', '*', '+', '.', '?', '[', '\\', ']', '^', '{', '|', '}'}; //in sorted order
    private final static MatchCriteria.PatternTranslator LIKE_TO_REGEX = new MatchCriteria.PatternTranslator(new char[] {'%', '_'}, new String[] {".*", "."},  REGEX_RESERVED, '\\', Pattern.DOTALL);  //$NON-NLS-1$ //$NON-NLS-2$
    
    private final static char[] SIMILAR_REGEX_RESERVED = new char[] {'$', '.', '\\', '^'}; //in sorted order
    public final static MatchCriteria.PatternTranslator SIMILAR_TO_REGEX = new MatchCriteria.PatternTranslator(
    		new char[] {'%', '(', ')', '*', '?', '+', '[', ']', '_', '{', '|', '}'}, 
    		new String[] {"([a]|[^a])*", "(", ")", "*", "?", "+", //$NON-NLS-1$ //$NON-NLS-2$  //$NON-NLS-3$ //$NON-NLS-4$  //$NON-NLS-5$ //$NON-NLS-6$
    				"[", "]", "([a]|[^a])", "{", "|", "}"},  SIMILAR_REGEX_RESERVED, '\\', 0);  //$NON-NLS-1$ //$NON-NLS-2$  //$NON-NLS-3$ //$NON-NLS-4$  //$NON-NLS-5$ //$NON-NLS-6$  
    
    private Map elements;
    
    protected ProcessorDataManager dataMgr;
    protected CommandContext context;
    
    public static boolean evaluate(Criteria criteria) throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
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

	public boolean evaluate(Criteria criteria, List<?> tuple)
        throws ExpressionEvaluationException, BlockedException, TeiidComponentException {

        return Boolean.TRUE.equals(evaluateTVL(criteria, tuple));
    }

    public Boolean evaluateTVL(Criteria criteria, List<?> tuple)
        throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
    	
		return internalEvaluateTVL(criteria, tuple);
	}

	private Boolean internalEvaluateTVL(Criteria criteria, List<?> tuple)
			throws ExpressionEvaluationException, BlockedException,
			TeiidComponentException {
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
        } else if (criteria instanceof ExpressionCriteria) {
        	return (Boolean)evaluate(((ExpressionCriteria)criteria).getExpression(), tuple);
		} else {
             throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30311, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30311, criteria));
		}
	}

	private Boolean evaluate(CompoundCriteria criteria, List<?> tuple)
		throws ExpressionEvaluationException, BlockedException, TeiidComponentException {

		List<Criteria> subCrits = criteria.getCriteria();
		boolean and = criteria.getOperator() == CompoundCriteria.AND;
        Boolean result = and?Boolean.TRUE:Boolean.FALSE;
		for (int i = 0; i < subCrits.size(); i++) {
			Criteria subCrit = subCrits.get(i);
			Boolean value = internalEvaluateTVL(subCrit, tuple);
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

	private Boolean evaluate(NotCriteria criteria, List<?> tuple)
		throws ExpressionEvaluationException, BlockedException, TeiidComponentException {

		Criteria subCrit = criteria.getCriteria();
		Boolean result = internalEvaluateTVL(subCrit, tuple);
        if (result == null) {
            return null;
        }
        if (result.booleanValue()) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
	}

	private Boolean evaluate(CompareCriteria criteria, List<?> tuple)
		throws ExpressionEvaluationException, BlockedException, TeiidComponentException {

		// Evaluate left expression
		Object leftValue = null;
		try {
			leftValue = evaluate(criteria.getLeftExpression(), tuple);
		} catch(ExpressionEvaluationException e) {
             throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30312, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30312, "left", criteria)); //$NON-NLS-1$
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
             throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30312, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30312, "right", criteria)); //$NON-NLS-1$
		}

		// Shortcut if null
		if(rightValue == null) {
			return null;
		}

		// Compare two non-null values using specified operator
		return compare(criteria, leftValue, rightValue);
	}

	private Boolean evaluate(MatchCriteria criteria, List<?> tuple)
		throws ExpressionEvaluationException, BlockedException, TeiidComponentException {

        boolean result = false;
		// Evaluate left expression
        Object value = null;
		try {
			value = evaluate(criteria.getLeftExpression(), tuple);
		} catch(ExpressionEvaluationException e) {
             throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30312, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30312, "left", criteria)); //$NON-NLS-1$
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
                 throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30316, err, err.getMessage());
            }
        }

		// Evaluate right expression
		String rightValue = null;
		try {
			rightValue = (String) evaluate(criteria.getRightExpression(), tuple);
		} catch(ExpressionEvaluationException e) {
             throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30312, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30312, "right", criteria)); //$NON-NLS-1$
		}

		// Shortcut if null
		if(rightValue == null) {
            return null;
        }
        
        result = match(rightValue, criteria.getEscapeChar(), leftValue, criteria.getMode());
        
        return Boolean.valueOf(result ^ criteria.isNegated());
	}

	private boolean match(String pattern, char escape, CharSequence search, MatchMode mode)
		throws ExpressionEvaluationException {

		Pattern patternRegex = null;
		switch (mode) {
		case LIKE:
			patternRegex = LIKE_TO_REGEX.translate(pattern, escape);
			break;
		case SIMILAR:
			patternRegex = SIMILAR_TO_REGEX.translate(pattern, escape);
			break;
		case REGEX:
			patternRegex = MatchCriteria.getPattern(pattern, pattern, 0);
			break;
		default:
			throw new AssertionError();
		}
		
        Matcher matcher = patternRegex.matcher(search);
        return matcher.find();
	}

	private Boolean evaluate(AbstractSetCriteria criteria, List<?> tuple)
		throws ExpressionEvaluationException, BlockedException, TeiidComponentException {

		// Evaluate expression
		Object leftValue = null;
		try {
			leftValue = evaluate(criteria.getExpression(), tuple);
		} catch(ExpressionEvaluationException e) {
             throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30323, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30323, criteria));
		}

		// Shortcut if null
		if(leftValue == null) {
            return null;
        }
        Boolean result = Boolean.FALSE;

        ValueIterator valueIter = null;
        if (criteria instanceof SetCriteria) {
        	SetCriteria set = (SetCriteria)criteria;
        	if (set.isAllConstants()) {
        		boolean exists = set.getValues().contains(new Constant(leftValue, criteria.getExpression().getType()));
        		if (!exists) {
        			if (set.getValues().contains(Constant.NULL_CONSTANT)) {
        				return null;
        			}
        			return criteria.isNegated();
        		}
        		return !criteria.isNegated();
        	}
        	valueIter = new CollectionValueIterator(((SetCriteria)criteria).getValues());
        } else if (criteria instanceof DependentSetCriteria){
        	DependentSetCriteria ref = (DependentSetCriteria)criteria;
        	VariableContext vc = getContext(criteria).getVariableContext();
    		ValueIteratorSource vis = (ValueIteratorSource)vc.getGlobalValue(ref.getContextSymbol());
    		Set<Object> values;
    		try {
    			values = vis.getCachedSet(ref.getValueExpression());
    		} catch (TeiidProcessingException e) {
    			 throw new ExpressionEvaluationException(e);
    		}
        	if (values != null) {
        		return values.contains(leftValue);
        	}
    		vis.setUnused(true);
        	//there are too many values to justify a linear search or holding
        	//them in memory
        	return true;
        } else if (criteria instanceof SubquerySetCriteria) {
        	try {
				valueIter = evaluateSubquery((SubquerySetCriteria)criteria, tuple);
			} catch (TeiidProcessingException e) {
				 throw new ExpressionEvaluationException(e);
			}
        } else {
        	throw new AssertionError("unknown set criteria type"); //$NON-NLS-1$
        }
        while(valueIter.hasNext()) {
            Object possibleValue = valueIter.next();
            Object value = null;
            if(possibleValue instanceof Expression) {
    			try {
    				value = evaluate((Expression) possibleValue, tuple);
    			} catch(ExpressionEvaluationException e) {
                     throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30323, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30323, possibleValue));
    			}
            } else {
                value = possibleValue;
            }

			if(value != null) {
				if(Constant.COMPARATOR.compare(leftValue, value) == 0) {
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

	private boolean evaluate(IsNullCriteria criteria, List<?> tuple)
		throws ExpressionEvaluationException, BlockedException, TeiidComponentException {

		// Evaluate expression
		Object value = null;
		try {
			value = evaluate(criteria.getExpression(), tuple);
		} catch(ExpressionEvaluationException e) {
             throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30323, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30323, criteria));
		}

		return (value == null ^ criteria.isNegated());
	}

    private Boolean evaluate(SubqueryCompareCriteria criteria, List<?> tuple)
        throws ExpressionEvaluationException, BlockedException, TeiidComponentException {

        // Evaluate expression
        Object leftValue = null;
        try {
            leftValue = evaluate(criteria.getLeftExpression(), tuple);
        } catch(ExpressionEvaluationException e) {
             throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30323, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30323, criteria));
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
			 throw new ExpressionEvaluationException(e);
		}
        while(valueIter.hasNext()) {
            Object value = valueIter.next();

            if(value != null) {
            	result = compare(criteria, leftValue, value);

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
                    default:
                         throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30326, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30326, criteria.getPredicateQuantifier()));
                }

            } else { // value is null
                result = null;
            }


        } //end value iteration

        return result;
    }

	public static Boolean compare(AbstractCompareCriteria criteria, Object leftValue,
			Object value) throws AssertionError {
		int compare = 0;
		//TODO: we follow oracle style array comparison
		//semantics.  each element is treated as an individual comparison,
		//so null implies unknown. h2 (and likely other dbms) allow for null
		//array element equality
		if (leftValue instanceof ArrayImpl) {
			ArrayImpl av = (ArrayImpl)leftValue;
			try {
				compare = av.compareTo((ArrayImpl)value, true, Constant.COMPARATOR);
			} catch (ArrayImpl.NullException e) {
				return null;
			}
		} else {
			compare = Constant.COMPARATOR.compare(leftValue, value);
		}
		// Compare two non-null values using specified operator
		Boolean result = null;
		switch(criteria.getOperator()) {
		    case CompareCriteria.EQ:
		        result = Boolean.valueOf(compare == 0);
		        break;
		    case CompareCriteria.NE:
		        result = Boolean.valueOf(compare != 0);
		        break;
		    case CompareCriteria.LT:
		        result = Boolean.valueOf(compare < 0);
		        break;
		    case CompareCriteria.LE:
		        result = Boolean.valueOf(compare <= 0);
		        break;
		    case CompareCriteria.GT:
		        result = Boolean.valueOf(compare > 0);
		        break;
		    case CompareCriteria.GE:
		        result = Boolean.valueOf(compare >= 0);
		        break;
		    default:
		        throw new AssertionError();
		}
		return result;
	}

    private boolean evaluate(ExistsCriteria criteria, List<?> tuple)
        throws BlockedException, TeiidComponentException, ExpressionEvaluationException {

        ValueIterator valueIter;
		try {
			valueIter = evaluateSubquery(criteria, tuple);
		} catch (TeiidProcessingException e) {
			 throw new ExpressionEvaluationException(e);
		}
        if(valueIter.hasNext()) {
            return !criteria.isNegated();
        }
        return criteria.isNegated();
    }
    
	public Object evaluate(Expression expression, List<?> tuple)
		throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
	
	    try {
			return internalEvaluate(expression, tuple);
	    } catch (ExpressionEvaluationException e) {
	         throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30328, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30328, new Object[] {expression, e.getMessage()}));
	    }
	}
	
	protected Object internalEvaluate(Expression expression, List<?> tuple)
	   throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
	
	   if(expression instanceof DerivedExpression) {
		   if (elements != null) {
		       // Try to evaluate by lookup in the elements map (may work for both ElementSymbol and ExpressionSymbol
		       Integer index = (Integer) elements.get(expression);
		       if(index != null) {
		           return tuple.get(index.intValue());
		       }
		   }
		   
	       // Otherwise this should be an ExpressionSymbol and we just need to dive in and evaluate the expression itself
	       if (expression instanceof ExpressionSymbol) {            
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
		   Object result = internalEvaluate(ref.getExpression(), tuple);
		   if (ref.getConstraint() != null) {
			   try {
				   ref.getConstraint().validate(result);
			   } catch (QueryValidatorException e) {
				   throw new ExpressionEvaluationException(e);
			   }
		   }
		   return result;
	   } else if(expression instanceof Criteria) {
	       return evaluate((Criteria) expression, tuple);
	   } else if(expression instanceof ScalarSubquery) {
	       return evaluate((ScalarSubquery) expression, tuple);
	   } else if (expression instanceof Criteria) {
		   return evaluate((Criteria)expression, tuple);
	   } else if (expression instanceof TextLine){
		   return evaluateTextLine(tuple, (TextLine)expression);
	   } else if (expression instanceof XMLElement){
		   return evaluateXMLElement(tuple, (XMLElement)expression);
	   } else if (expression instanceof XMLForest){
		   return evaluateXMLForest(tuple, (XMLForest)expression);
	   } else if (expression instanceof JSONObject){
		   return evaluateJSONObject(tuple, (JSONObject)expression, null);
	   } else if (expression instanceof XMLSerialize){
		   return evaluateXMLSerialize(tuple, (XMLSerialize)expression);
	   } else if (expression instanceof XMLQuery) {
		   return evaluateXMLQuery(tuple, (XMLQuery)expression);
	   } else if (expression instanceof QueryString) {
		   return evaluateQueryString(tuple, (QueryString)expression);
	   } else if (expression instanceof XMLParse){
		   return evaluateXMLParse(tuple, (XMLParse)expression);
	   } else if (expression instanceof Array) {
		   Array array = (Array)expression;
		   List<Expression> exprs = array.getExpressions();
		   Object[] result = (Object[]) java.lang.reflect.Array.newInstance(array.getComponentType(), exprs.size());
		   for (int i = 0; i < exprs.size(); i++) {
			   result[i] = internalEvaluate(exprs.get(i), tuple);
		   }
		   return new ArrayImpl(result);
	   } else if (expression instanceof ExceptionExpression) {
		   return evaluate(tuple, (ExceptionExpression)expression);
	   } else {
	        throw new TeiidComponentException(QueryPlugin.Event.TEIID30329, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30329, expression.getClass().getName()));
	   }
	}

	private Object evaluate(List<?> tuple, ExceptionExpression ee)
			throws ExpressionEvaluationException, BlockedException,
			TeiidComponentException {
		String msg = (String) internalEvaluate(ee.getMessage(), tuple);
		String sqlState = ee.getDefaultSQLState();
		if (ee.getSqlState() != null) {
			sqlState = (String) internalEvaluate(ee.getSqlState(), tuple);
		}
		Integer errorCode = null;
		if (ee.getErrorCode() != null) {
			errorCode = (Integer) internalEvaluate(ee.getErrorCode(), tuple);
		}
		Exception parent = null;
		if (ee.getParent() != null) {
			parent = (Exception) internalEvaluate(ee.getParent(), tuple);
		}
		Exception result = new TeiidSQLException(parent, msg, sqlState, errorCode!=null?errorCode:0);
		result.setStackTrace(SourceWarning.EMPTY_STACK_TRACE);
		return result;
	}

	private Object evaluateXMLParse(List<?> tuple, final XMLParse xp) throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
		Object value = internalEvaluate(xp.getExpression(), tuple);
		if (value == null) {
			return null;
		}
		XMLType.Type type = Type.DOCUMENT;
		SQLXMLImpl result = null;
		try {
			if (value instanceof String) {
				String string = (String)value;
				result = new SQLXMLImpl(string);
				result.setCharset(Streamable.CHARSET);
				if (!xp.isWellFormed()) {
					Reader r = new StringReader(string);
					type = validate(xp, r);
				}
			} else {
				InputStreamFactory isf = null;
				Streamable<?> s = (Streamable<?>)value;
				isf = getInputStreamFactory(s);
				result = new SQLXMLImpl(isf);
				if (!xp.isWellFormed()) {
					Reader r = result.getCharacterStream();
					type = validate(xp, r);
				}
			}
		} catch (TransformationException e) {
			 throw new ExpressionEvaluationException(e);
		} catch (SQLException e) {
			 throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30331, e, e.getMessage());
		}
		if (!xp.isDocument()) {
			type = Type.CONTENT;
		}
		XMLType xml = new XMLType(result);
		xml.setType(type);
		return xml;
	}

	public static InputStreamFactory getInputStreamFactory(Streamable<?> s) {
		if (s.getReference() instanceof Streamable<?>) {
			return getInputStreamFactory((Streamable<?>) s.getReference());
		}
		if (s.getReference() instanceof BaseLob) {
			BaseLob bl = (BaseLob) s.getReference();
			try {
				InputStreamFactory isf = bl.getStreamFactory();
				if (isf != null) {
					return isf;
				}
			} catch (SQLException e) {
			}
		}
		if (s instanceof ClobType) {
			return new InputStreamFactory.ClobInputStreamFactory((Clob)s.getReference());
		} else if (s instanceof BlobType){
			return new InputStreamFactory.BlobInputStreamFactory((Blob)s.getReference());
		}
		return new InputStreamFactory.SQLXMLInputStreamFactory((SQLXML)s.getReference());
	}

	private Type validate(final XMLParse xp, Reader r)
			throws TransformationException {
		if (!xp.isDocument()) {
			LinkedList<Reader> readers = new LinkedList<Reader>();
			readers.add(new StringReader("<r>")); //$NON-NLS-1$
			readers.add(r);
			readers.add(new StringReader("</r>")); //$NON-NLS-1$
			r = new SequenceReader(readers);
		}
		return StringToSQLXMLTransform.isXml(r);
	}

	//TODO: exception if length is too long?
	private Object evaluateQueryString(List<?> tuple, QueryString queryString)
			throws ExpressionEvaluationException, BlockedException,
			TeiidComponentException {
		Evaluator.NameValuePair<Object>[] pairs = getNameValuePairs(tuple, queryString.getArgs(), false, true);
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

	private Object evaluateXMLQuery(List<?> tuple, XMLQuery xmlQuery)
			throws BlockedException, TeiidComponentException,
			FunctionExecutionException {
		boolean emptyOnEmpty = xmlQuery.getEmptyOnEmpty() == null || xmlQuery.getEmptyOnEmpty();
		Result result = null;
		try {
			XMLQueryRowProcessor rp = null;
			if (xmlQuery.getXQueryExpression().isStreaming()) {
				rp = new XMLQueryRowProcessor();
			}
			try {
				result = evaluateXQuery(xmlQuery.getXQueryExpression(), xmlQuery.getPassing(), tuple, rp);
			} catch (TeiidRuntimeException e) {
				if (e.getCause() instanceof XPathException) {
					throw (XPathException)e.getCause();
				}
				throw e;
			}
			if (rp != null) {
				XMLType.Type type = rp.type;
				if (type == null) {
					if (!emptyOnEmpty) {
						return null;
					}
					type = Type.CONTENT;
				}
				XMLType val = rp.concat.close();
				val.setType(rp.type);
				return val;
			}
			return xmlQuery.getXQueryExpression().createXMLType(result.iter, this.context.getBufferManager(), emptyOnEmpty);
		} catch (TeiidProcessingException e) {
			 throw new FunctionExecutionException(QueryPlugin.Event.TEIID30333, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30333, e.getMessage()));
		} catch (XPathException e) {
			 throw new FunctionExecutionException(QueryPlugin.Event.TEIID30333, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30333, e.getMessage()));
		} finally {
			if (result != null) {
				result.close();
			}
		}
	}
	
	private Object evaluateXMLSerialize(List<?> tuple, XMLSerialize xs)
			throws ExpressionEvaluationException, BlockedException,
			TeiidComponentException, FunctionExecutionException {
		XMLType value = (XMLType) internalEvaluate(xs.getExpression(), tuple);
		if (value == null) {
			return null;
		}
		try {
			if (!xs.isDocument()) {
				return XMLSystemFunctions.serialize(xs, value);
			}
			if (value.getType() == Type.UNKNOWN) {
				Type type = StringToSQLXMLTransform.isXml(value.getCharacterStream());
				value.setType(type);
			}
			if (value.getType() == Type.DOCUMENT || value.getType() == Type.ELEMENT) {
				return XMLSystemFunctions.serialize(xs, value);
			}
		} catch (SQLException e) {
			 throw new FunctionExecutionException(QueryPlugin.Event.TEIID30334, e);
		} catch (TransformationException e) {
			 throw new FunctionExecutionException(QueryPlugin.Event.TEIID30335, e);
		}
		 throw new FunctionExecutionException(QueryPlugin.Event.TEIID30336, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30336));
	}
	
	private static TextLine.ValueExtractor<NameValuePair<Object>> defaultExtractor = new TextLine.ValueExtractor<NameValuePair<Object>>() {
		public Object getValue(NameValuePair<Object> t) {
			return t.value;
		}
	};

	private Object evaluateTextLine(List<?> tuple, TextLine function) throws ExpressionEvaluationException, BlockedException, TeiidComponentException, FunctionExecutionException {
		List<DerivedColumn> args = function.getExpressions();
		Evaluator.NameValuePair<Object>[] nameValuePairs = getNameValuePairs(tuple, args, true, true);
		
		try {
			return new ArrayImpl(TextLine.evaluate(Arrays.asList(nameValuePairs), defaultExtractor, function));
		} catch (TransformationException e) {
			 throw new ExpressionEvaluationException(e);
		}
	}

	private Object evaluateXMLForest(List<?> tuple, XMLForest function)
			throws ExpressionEvaluationException, BlockedException,
			TeiidComponentException, FunctionExecutionException {
		List<DerivedColumn> args = function.getArgs();
		Evaluator.NameValuePair<Object>[] nameValuePairs = getNameValuePairs(tuple, args, true, true); 
			
		try {
			return XMLSystemFunctions.xmlForest(context, namespaces(function.getNamespaces()), nameValuePairs);
		} catch (TeiidProcessingException e) {
			 throw new FunctionExecutionException(e);
		}
	}
	
	private Object evaluateJSONObject(List<?> tuple, JSONObject function, JSONBuilder builder)
			throws ExpressionEvaluationException, BlockedException,
			TeiidComponentException, FunctionExecutionException {
		List<DerivedColumn> args = function.getArgs();
		Evaluator.NameValuePair<Object>[] nameValuePairs = getNameValuePairs(tuple, args, false, false);
		boolean returnValue = false;
		try {
			if (builder == null) {
				returnValue = true;
				//preevaluate subqueries to prevent blocked exceptions
				for (SubqueryContainer<?> container : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(function)) {
					evaluateSubquery(container, tuple);
				}
				builder = new JSONBuilder(context.getBufferManager());
			}
			builder.start(false);
			for (NameValuePair<Object> nameValuePair : nameValuePairs) {
				addValue(tuple, builder, nameValuePair.name, nameValuePair.value);
			}
			builder.end(false);
			if (returnValue) {
				ClobType result = builder.close();
				builder = null;
				return result;
			}
			return null;
		} catch (TeiidProcessingException e) {
			throw new FunctionExecutionException(e);
		} finally {
			if (returnValue && builder != null) {
				builder.remove();
			}
		}
	}

	private void addValue(List<?> tuple, JSONBuilder builder,
			String name, Object value)
			throws TeiidProcessingException, ExpressionEvaluationException,
			BlockedException, TeiidComponentException,
			FunctionExecutionException {
		try {
			if (value instanceof JSONObject) {
				builder.startValue(name);
				evaluateJSONObject(tuple, (JSONObject)value, builder);
				return;
			}
			if (value instanceof Function) {
				Function f = (Function)value;
				if (f.getName().equalsIgnoreCase(FunctionLibrary.JSONARRAY)) {
					builder.startValue(name);
					jsonArray(context, f, f.getArgs(), builder, this, tuple);
					return;
				}
			}
			builder.addValue(name, internalEvaluate((Expression)value, tuple));
		} catch (BlockedException e) {
			throw e;
		}
	}
	
	public static ClobType jsonArray(CommandContext context, Function f, Object[] vals, JSONBuilder builder, Evaluator eval, List<?> tuple) throws TeiidProcessingException, BlockedException, TeiidComponentException {
		boolean returnValue = false;
		try {
			if (builder == null) {
				returnValue = true;
				if (eval != null) {
					//preevaluate subqueries to prevent blocked exceptions
					for (SubqueryContainer<?> container : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(f)) {
						eval.evaluateSubquery(container, tuple);
					}
				}
				builder = new JSONBuilder(context.getBufferManager());
			}
			builder.start(true);
			for (Object object : vals) {
				if (eval != null) {
					eval.addValue(tuple, builder, null, object);
				} else {
					builder.addValue(object);
				}
			}
			builder.end(true);
			if (returnValue) {
				ClobType result = builder.close();
				builder = null;
				return result;
			}
			return null;
		} finally {
			if (returnValue && builder != null) {
				builder.remove();
			}
		}
	}

	private Object evaluateXMLElement(List<?> tuple, XMLElement function)
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
				attributes = getNameValuePairs(tuple, function.getAttributes().getArgs(), true, true);
			}
			return XMLSystemFunctions.xmlElement(context, function.getName(), namespaces(function.getNamespaces()), attributes, values);
		} catch (TeiidProcessingException e) {
			throw new FunctionExecutionException(e);
		}
	}
	
	private Result evaluateXQuery(SaxonXQueryExpression xquery, List<DerivedColumn> cols, List<?> tuple, RowProcessor processor) 
	throws BlockedException, TeiidComponentException, TeiidProcessingException {
		HashMap<String, Object> parameters = new HashMap<String, Object>();
		Object contextItem = evaluateParameters(cols, tuple, parameters);
		return XQueryEvaluator.evaluateXQuery(xquery, contextItem, parameters, processor, context);
	}

	/**
	 * Evaluate the parameters and return the context item if it exists
	 */
	public Object evaluateParameters(List<DerivedColumn> cols, List<?> tuple,
			Map<String, Object> parameters)
			throws ExpressionEvaluationException, BlockedException,
			TeiidComponentException {
		Object contextItem = null;
		for (DerivedColumn passing : cols) {
			Object value = evaluateParameter(tuple, passing);
			if (passing.getAlias() == null) {
				contextItem = value;
			} else {
				parameters.put(passing.getAlias(), value);
			}
		}
		return contextItem;
	}

	private Object evaluateParameter(List<?> tuple, DerivedColumn passing)
			throws ExpressionEvaluationException, BlockedException,
			TeiidComponentException {
		if (passing.getExpression() instanceof Function) {
			Function f = (Function)passing.getExpression();
			//narrow optimization of json based documents to allow for lower overhead streaming
			if (f.getName().equalsIgnoreCase(SourceSystemFunctions.JSONTOXML)) {
				String rootName = (String)this.evaluate(f.getArg(0), tuple);
				Object lob = this.evaluate(f.getArg(1), tuple);
				if (rootName == null || lob == null) {
					return null;
				}
				try {
					if (lob instanceof Blob) {
						return XMLSystemFunctions.jsonToXml(context, rootName, (Blob)lob, true);
					}
					return XMLSystemFunctions.jsonToXml(context, rootName, (Clob)lob, true);
				} catch (IOException e) {
					throw new FunctionExecutionException(QueryPlugin.Event.TEIID30384, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30384, f.getFunctionDescriptor().getName(), e.getMessage()));
				} catch (SQLException e) {
					throw new FunctionExecutionException(QueryPlugin.Event.TEIID30384, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30384, f.getFunctionDescriptor().getName(), e.getMessage()));
				} catch (TeiidProcessingException e) {
					throw new FunctionExecutionException(QueryPlugin.Event.TEIID30384, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30384, f.getFunctionDescriptor().getName(), e.getMessage()));
				}
			}
		} else if (passing.getExpression() instanceof XMLParse) {
			XMLParse xmlParse = (XMLParse)passing.getExpression();
			xmlParse.setWellFormed(true);
		}
		Object value = this.evaluate(passing.getExpression(), tuple);
		return value;
	}

	private Evaluator.NameValuePair<Object>[] getNameValuePairs(List<?> tuple, List<DerivedColumn> args, boolean xmlNames, boolean eval)
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
			if (!xmlNames && name == null) {
				name = "expr" + (i+1); //$NON-NLS-1$
			}
			nameValuePairs[i] = new Evaluator.NameValuePair<Object>(name, eval?internalEvaluate(ex, tuple):ex);
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
	
	private Object evaluate(CaseExpression expr, List<?> tuple)
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
	
	private Object evaluate(SearchedCaseExpression expr, List<?> tuple)
	throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
	    for (int i = 0; i < expr.getWhenCount(); i++) {
            if (evaluate(expr.getWhenCriteria(i), tuple)) {
                return internalEvaluate(expr.getThenExpression(i), tuple);
            }
	    }
	    if (expr.getElseExpression() != null) {
	        return internalEvaluate(expr.getElseExpression(), tuple);
	    }
	    return null;
	}
	
	private Object evaluate(Function function, List<?> tuple)
		throws BlockedException, TeiidComponentException, ExpressionEvaluationException {
	
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
	    
	    if (fd.getPushdown() == PushDown.MUST_PUSHDOWN) {
	    	try {
				return evaluatePushdown(function, tuple, values);
			} catch (TeiidProcessingException e) {
				throw new ExpressionEvaluationException(e);
			}
	    }
	    
	    // Check for special lookup function
	    if(function.getName().equalsIgnoreCase(FunctionLibrary.LOOKUP)) {
	        if(dataMgr == null) {
	             throw new ComponentNotFoundException(QueryPlugin.Event.TEIID30342, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30342));
	        }
	
	        String codeTableName = (String) values[0];
	        String returnElementName = (String) values[1];
	        String keyElementName = (String) values[2];
	        
	        try {
				return dataMgr.lookupCodeValue(context, codeTableName, returnElementName, keyElementName, values[3]);
			} catch (TeiidProcessingException e) {
				throw new ExpressionEvaluationException(e);
			}
	    }
	    
		// Execute function
		return fd.invokeFunction(values, context, null);
	}
	
	protected Object evaluatePushdown(Function function, List<?> tuple,
			Object[] values) throws FunctionExecutionException, TeiidComponentException, TeiidProcessingException {
		throw new FunctionExecutionException(QueryPlugin.Event.TEIID30341, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30341, function.getFunctionDescriptor().getFullName()));
	}

	private Object evaluate(ScalarSubquery scalarSubquery, List<?> tuple)
	    throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
		
	    Object result = null;
        ValueIterator valueIter;
		try {
			valueIter = evaluateSubquery(scalarSubquery, tuple);
		} catch (TeiidProcessingException e) {
			 throw new ExpressionEvaluationException(e);
		}
	    if(valueIter.hasNext()) {
	        result = valueIter.next();
	        if(valueIter.hasNext()) {
	            // The subquery should be scalar, but has produced
	            // more than one result value - this is an exception case
	             throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30345, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30345, scalarSubquery.getCommand()));
	        }
	    }
	    return result;
	}
	
	/**
	 * @param container
	 * @param tuple
	 * @return
	 * @throws TeiidProcessingException
	 * @throws BlockedException
	 * @throws TeiidComponentException
	 */
	protected ValueIterator evaluateSubquery(SubqueryContainer<?> container, List<?> tuple) 
	throws TeiidProcessingException, BlockedException, TeiidComponentException {
		throw new UnsupportedOperationException("Subquery evaluation not possible with a base Evaluator"); //$NON-NLS-1$
	}

	private CommandContext getContext(LanguageObject expression) throws TeiidComponentException {
		if (context == null) {
			 throw new TeiidComponentException(QueryPlugin.Event.TEIID30328, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30328, expression, QueryPlugin.Util.getString("Evaluator.no_value"))); //$NON-NLS-1$
		}
		return context;
	}   
	    
}
