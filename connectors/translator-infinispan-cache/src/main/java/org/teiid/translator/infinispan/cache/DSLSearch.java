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
package org.teiid.translator.infinispan.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.infinispan.query.dsl.FilterConditionBeginContext;
import org.infinispan.query.dsl.FilterConditionContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.SortOrder;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.language.*;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectVisitor;
import org.teiid.translator.object.SearchType;



/**
 * DSLSearch is used to perform queries on an Infinispan Cache based on
 * the DSL query language..   This will parse the WHERE criteria and dynamically build the search query(s)
 * that's used to retrieve the results from an Infinispan cache.
 * 
 * Note:  As of JDG 6.2, DSL is supported
 * 
 * @author vhalbert
 * 
 * @since 8.7.x
 * 
 * @TODO
 * Between
 * 
 */
public final class DSLSearch implements SearchType   {

	
	@Override
	/** 
	 * Calling to make a key value search on the cache.
	 * 
	 * The assumption is the <code>value</code> has already been converted the key object type
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.SearchType#performKeySearch(java.lang.String, java.lang.Object, org.teiid.translator.object.ObjectConnection)
	 */
	public Object performKeySearch(String columnNameInSource, Object value, ObjectConnection conn) throws TranslatorException {
	    
		
		@SuppressWarnings("rawtypes")
		QueryBuilder qb = getQueryBuilder(conn);
	    
		value = escapeReservedChars(value);
		
	    FilterConditionContext fcc = qb.having(columnNameInSource).eq(value);
		
		Query query = fcc.toBuilder().build();
		List<Object> results = query.list();
		if (results.size() == 1) {
			return results.get(0);
		} else if (results.size() > 1) {
			throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID21504, value.toString()));
		}
		
		return null;
	}
	
	@Override
	public List<Object> performSearch(ObjectVisitor visitor,
			ObjectConnection conn) throws TranslatorException {
		
		Condition where = visitor.getWhereCriteria();
		OrderBy orderby = visitor.getOrderBy();		

	    QueryBuilder qb = getQueryBuilder(conn);
	    
	    if (visitor.getLimit() > 0) {
	    	qb.maxResults(visitor.getLimit());
	    }
	    	    
	    if (orderby != null) {
		    List<SortSpecification> sss = orderby.getSortSpecifications();
		    for (SortSpecification spec:sss) {
		    	Expression exp = spec.getExpression();
		    	Column mdIDElement = ((ColumnReference) exp).getMetadataObject();
		    	SortOrder so = SortOrder.ASC;
		    	if (spec.getOrdering().name().equalsIgnoreCase(SortOrder.DESC.name())) {
		    		so = SortOrder.DESC;
		    	}
		    	qb = qb.orderBy(mdIDElement.getNameInSource(), so);		    }
	    }
	    	
	    FilterConditionContext fcc = buildQueryFromWhereClause(where, qb, null);	 
		 			
		List<Object> results =  null;
				
		Query query = null;
		if (fcc != null) {
			query = fcc.toBuilder().build();
			results = query.list();

			if (results == null) {
				return Collections.emptyList();
			}
		} else if (orderby != null) {
			   query = qb.build();
	           results = query.list();
	           if (results == null) {
	                   return Collections.emptyList();
	           }			
		} else {
			results = new ArrayList<Object>();
			Collection<Object> all = conn.getAll();
			results.addAll(all);
			return results;
		}

		return results;
		
	}
	
	@SuppressWarnings("rawtypes")
	private static QueryBuilder getQueryBuilder(ObjectConnection conn) throws TranslatorException {
		
		InfinispanCacheConnection iconn = (InfinispanCacheConnection) conn;

		QueryFactory qf = iconn.getQueryFactory();
		
		Class<?> type = conn.getCacheClassType();
	    		  
	    QueryBuilder qb = qf.from(type);

	    return qb;
	}
	
	private static FilterConditionContext buildQueryFromWhereClause(Condition criteria, @SuppressWarnings("rawtypes") QueryBuilder queryBuilder, FilterConditionBeginContext fcbc)
			throws TranslatorException {

		if (criteria == null) return null;
		FilterConditionContext fcc = null;

		
		if (criteria instanceof AndOr) {
			LogManager.logTrace(LogConstants.CTX_CONNECTOR,
					"Infinispan DSL Parsing compound criteria."); //$NON-NLS-1$
			AndOr crit = (AndOr) criteria;
			AndOr.Operator op = crit.getOperator();

			switch (op) {
			case AND:

				FilterConditionContext f_and = buildQueryFromWhereClause(
						crit.getLeftCondition(), queryBuilder, fcbc);
				FilterConditionBeginContext fcbca =  null;
				if (f_and != null) {
					fcbca = f_and.and();
				}
				fcc = buildQueryFromWhereClause(
						crit.getRightCondition(), queryBuilder, fcbca);

				break;

			case OR:
				
				FilterConditionContext f_or = buildQueryFromWhereClause(
						crit.getLeftCondition(), queryBuilder, fcbc);
				FilterConditionBeginContext fcbcb = null;
				if (f_or != null) {
					fcbcb = f_or.or();
				}
				fcc = buildQueryFromWhereClause(
						crit.getRightCondition(), queryBuilder, fcbcb);
				
				
				break;

			default:
				throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID21501, new Object[] { op, "And, Or" }));
			}

		} else if (criteria instanceof Comparison) {
			fcc = visit((Comparison) criteria,  queryBuilder, fcbc);

		} else if (criteria instanceof Like) {
			fcc = visit((Like) criteria, queryBuilder, fcbc);

		} else if (criteria instanceof In) {
			fcc = visit((In) criteria, queryBuilder, fcbc);

		} else if (criteria instanceof IsNull) {
			fcc = visit( (IsNull) criteria, queryBuilder, fcbc);
			
		} else if (criteria instanceof Not) {
			Condition c = ((Not)criteria).getCriteria();
			if (fcbc == null) {
				fcc = queryBuilder.not(buildQueryFromWhereClause(c, queryBuilder, fcbc));
			} else {
				fcc = fcbc.not(buildQueryFromWhereClause(c, queryBuilder, fcbc));
			}
		} else {
			throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID21505, criteria.toString()));

		}
		return fcc;
	}

	@SuppressWarnings("rawtypes")
	public static FilterConditionContext visit(Comparison obj, QueryBuilder queryBuilder, FilterConditionBeginContext fcbc) throws TranslatorException {

		LogManager.logTrace(LogConstants.CTX_CONNECTOR,
				"Parsing Comparison criteria."); //$NON-NLS-1$
		Comparison.Operator op = obj.getOperator();

		Expression lhs = obj.getLeftExpression();
		Expression rhs = obj.getRightExpression();

		// joins between the objects in the same cache is not usable
		if ((lhs instanceof ColumnReference && rhs instanceof ColumnReference)
				|| (lhs instanceof Literal && rhs instanceof Literal)) {
			return null;
		}

		Object value = null;
		Column mdIDElement = null;
		Literal literal = null;
		if (lhs instanceof ColumnReference) {

			mdIDElement = ((ColumnReference) lhs).getMetadataObject();
			literal = (Literal) rhs;
			value = literal.getValue();

		} else if (rhs instanceof ColumnReference ){
			mdIDElement = ((ColumnReference) rhs).getMetadataObject();
			literal = (Literal) lhs;
			value = literal.getValue();
		}

		if (value == null) {
			throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID21502));
		}

		
		value = escapeReservedChars(value);
		
		value = convertValue(value, mdIDElement);
		
		switch (op) {
		case NE:
			if (fcbc == null) {
				return queryBuilder.not().having(getRecordName(mdIDElement)).eq(value);
			} 
			return fcbc.not().having(getRecordName(mdIDElement)).eq(value);
			

		case EQ:
			if (fcbc == null ) {
				return queryBuilder.having(getRecordName(mdIDElement)).eq(value);
			}
			return fcbc.having(getRecordName(mdIDElement)).eq(value);

		case GT:
			if (fcbc == null) {
				return queryBuilder.having(getRecordName(mdIDElement)).gt(value);
			}
			return fcbc.having(getRecordName(mdIDElement)).gt(value);

		case GE:
			if (fcbc == null) {
				return queryBuilder.having(getRecordName(mdIDElement)).gte(value);
			}
			return fcbc.having(getRecordName(mdIDElement)).gte(value);

		case LT:
			if (fcbc == null) {
				return queryBuilder.having(getRecordName(mdIDElement)).lt(value);
			}
			return fcbc.having(getRecordName(mdIDElement)).lt(value);
			
		case LE:
			if (fcbc == null) {
				return queryBuilder.having(getRecordName(mdIDElement)).lte(value);
			}
			return fcbc.having(getRecordName(mdIDElement)).lte(value);

		default:
			throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID21501, new Object[] { op, "NE, EQ, GT, GE, LT, LE" }));
		}

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static FilterConditionContext visit(In obj, QueryBuilder queryBuilder, FilterConditionBeginContext fcbc) throws TranslatorException {
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Parsing IN criteria."); //$NON-NLS-1$

		Expression lhs = obj.getLeftExpression();
		Column col = ((ColumnReference) lhs).getMetadataObject();

		List<Expression> rhsList = obj.getRightExpressions();
		
		List v = new ArrayList(rhsList.size()) ;
		//= Arrays.asList(1
		boolean createdQuery = false;
		for (Expression expr : rhsList) {

			if (expr instanceof Literal) {
				Literal literal = (Literal) expr;

				Object value = escapeReservedChars(literal.getValue());
				value = convertValue(value, col);
				v.add(value);
				createdQuery = true;
			} else {
				throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID21503, new Object[] { expr, "IN" }));
			}
		}
		
		if (createdQuery) {
			

			if (fcbc == null) {
				if (obj.isNegated()) {
					return  queryBuilder.not().having(getRecordName(col)).in(v);
				}
				return  queryBuilder.having(getRecordName(col)).in(v);
			}
			if (obj.isNegated()) {
				return fcbc.not().having(getRecordName(col)).in(v);
			}			
			return fcbc.having(getRecordName(col)).in(v);
		}
		return null;
	}

	@SuppressWarnings("rawtypes")
	public static FilterConditionContext visit(Like obj, QueryBuilder queryBuilder, FilterConditionBeginContext fcbc) throws TranslatorException {
		LogManager.logTrace(LogConstants.CTX_CONNECTOR,
				"Parsing LIKE criteria."); //$NON-NLS-1$

		Expression lhs = obj.getLeftExpression();
		Expression rhs = obj.getRightExpression();

		Column c = null;
		Expression literalExp = null;
		if (lhs instanceof ColumnReference) {
			c = ((ColumnReference) lhs).getMetadataObject();
			literalExp = rhs;
		} else {
			c = ((ColumnReference) rhs).getMetadataObject();
			literalExp = lhs;
		}

		String value = null;
		if (literalExp instanceof Literal) {

			value = (String) escapeReservedChars(((Literal) literalExp)
					.getValue());
			if (fcbc == null) {
				if (obj.isNegated()) {
					return queryBuilder.not().having(getRecordName(c)).like(value);
				}
				return queryBuilder.having(getRecordName(c)).like(value);
			}
			if (obj.isNegated()) {
				return fcbc.not().having(getRecordName(c)).like(value);
			}
			return fcbc.having(getRecordName(c)).like(value);
		} 
		throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID21503, new Object[] { literalExp.toString(), "LIKE" }));

	}
	
	@SuppressWarnings("rawtypes")
	public static FilterConditionContext visit(IsNull obj, QueryBuilder queryBuilder, FilterConditionBeginContext fcbc)  {
		LogManager.logTrace(LogConstants.CTX_CONNECTOR,
				"Parsing IsNull criteria."); //$NON-NLS-1$

		Expression exp = obj.getExpression();
		Column c =  ((ColumnReference) exp).getMetadataObject();

		if (fcbc == null) {
			if (obj.isNegated()) {
				return queryBuilder.not().having(getRecordName(c)).isNull();
			}
			return queryBuilder.having(getRecordName(c)).isNull();
		}
		if (obj.isNegated()) {
			return fcbc.not().having(getRecordName(c)).isNull();
		}
		return fcbc.having(getRecordName(c)).isNull();

	}	

	protected static Object escapeReservedChars(final Object value) {
		if (value instanceof String) {
		} else {
			return value;
		}

		String expr = (String) value;

		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < expr.length(); i++) {
			char curChar = expr.charAt(i);
			switch (curChar) {
			case '\\':
				sb.append("\\5c"); //$NON-NLS-1$
				break;
			case '*':
				sb.append("\\2a"); //$NON-NLS-1$
				break;
			case '(':
				sb.append("\\28"); //$NON-NLS-1$
				break;
			case ')':
				sb.append("\\29"); //$NON-NLS-1$
				break;
			case '\u0000':
				sb.append("\\00"); //$NON-NLS-1$
				break;
			default:
				sb.append(curChar);
			}
		}
		return sb.toString();
	}

	private static String getRecordName(Column col) {
		return SQLStringVisitor.getRecordName(col);
	}
	
	// convert the value based on the native type
	private static Object convertValue(Object value, Column mdIDElement) throws TranslatorException {
		try {
			value  = DataTypeManager.transformValue(value,  mdIDElement.getJavaType());
			return value;
		} catch (TransformationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new TranslatorException(e);
		}
	}
}
