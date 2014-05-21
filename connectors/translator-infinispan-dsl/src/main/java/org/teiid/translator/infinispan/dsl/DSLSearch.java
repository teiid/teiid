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
package org.teiid.translator.infinispan.dsl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.teiid.language.Select;
import org.teiid.translator.TranslatorException;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.teiid.language.*;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;



/**
 * DSLSearch will parse the WHERE criteria and build the search query(s)
 * that's used to retrieve the results from a remote Infinispan cache using Hot Rod client.
 * 
 * Note:  As of JDG 6.2, DSL is supported
 * 
 * @author vhalbert
 * 
 * @since 8.7.x
 * 
 */
public final class DSLSearch   {


	@SuppressWarnings("rawtypes")
	public static List<Object> performSearch(Select command, Class<?> type, String cacheName, InfinispanConnection conn)
			throws TranslatorException {
		
		RemoteCache rc = (RemoteCache) conn.getCache(cacheName);
		
		QueryFactory qf = Search.getQueryFactory(rc);
	    		  
	    QueryBuilder qb = qf.from(type);
	    	
		boolean createdQueries = buildQueryFromWhereClause(command.getWhere(), qb);	 
		 			
		List<Object> results =  null;
		
		Query query = null;
		if (createdQueries) {
			query = qb.build();
			results = query.list();

			if (results == null || results.isEmpty()) {
				return Collections.emptyList();
			}
			
		} else {
			results = new ArrayList();
			
			results.addAll( rc.getBulk().values() );
		}

		return results;
		
	}
	
	private static boolean buildQueryFromWhereClause(Condition criteria, QueryBuilder queryBuilder)
			throws TranslatorException {
		boolean createdQueries = false;

		if (criteria == null) return false;
	
		
		if (criteria instanceof AndOr) {
			LogManager.logTrace(LogConstants.CTX_CONNECTOR,
					"Infinispan DSL Parsing compound criteria."); //$NON-NLS-1$
			AndOr crit = (AndOr) criteria;
			AndOr.Operator op = crit.getOperator();

			switch (op) {
			case AND:

////				BooleanJunction<BooleanJunction> leftAnd = queryBuilder.
////						.bool();
//				boolean andLeftHasQueries = buildQueryFromWhereClause(
//						crit.getLeftCondition(), queryBuilder);
//
////				BooleanJunction<BooleanJunction> rightAnd = queryBuilder
////						.bool();
//				boolean andRightHasQueries = buildQueryFromWhereClause(
//						crit.getRightCondition(), queryBuilder);
//
//				if (andLeftHasQueries && andRightHasQueries) {
//					leftAnd.must(rightAnd.createQuery());
//					inUse.should(leftAnd.createQuery());
//				} else if (andLeftHasQueries) {
//
//					inUse.should(leftAnd.createQuery());
//				} else if (andRightHasQueries) {
//					inUse.should(rightAnd.createQuery());
//				}
//
//				createdQueries = (andLeftHasQueries ? andLeftHasQueries
//						: andRightHasQueries);

				break;

			case OR:

//				boolean orLeftHasQueries = buildQueryFromWhereClause(
//						crit.getLeftCondition(), inUse, queryBuilder);
//				boolean orRightHasQueries = buildQueryFromWhereClause(
//						crit.getRightCondition(), inUse, queryBuilder);
//
//				createdQueries = (orLeftHasQueries ? orLeftHasQueries
//						: orRightHasQueries);

				break;

			default:
				final String msg = InfinispanPlugin.Util
						.getString("LuceneSearch.invalidOperator", new Object[] { op, "And, Or" }); //$NON-NLS-1$ //$NON-NLS-2$
				throw new TranslatorException(msg);
			}

		} else if (criteria instanceof Comparison) {
			createdQueries = visit((Comparison) criteria,  queryBuilder);

		} else if (criteria instanceof Exists) {
			LogManager.logTrace(LogConstants.CTX_CONNECTOR,
					"Parsing EXISTS criteria: NOT IMPLEMENTED YET"); //$NON-NLS-1$
			// TODO Exists should be supported in a future release.

		} else if (criteria instanceof Like) {
			createdQueries = visit((Like) criteria, queryBuilder);

		} else if (criteria instanceof In) {
			createdQueries = visit((In) criteria, queryBuilder);

		}
		// else if (criteria instanceof Not) {
		//			LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Parsing NOT criteria."); //$NON-NLS-1$
		// isNegated = true;
		// filterList.addAll(getSearchFilterFromWhereClause(((Not)criteria).getCriteria(),
		// new LinkedList<String>()));
		// }

		return createdQueries;
	}
		

//	private static boolean buildQueryFromWhereClause(Condition criteria, QueryBuilder queryBuilder)
//			throws TranslatorException {
//		boolean createdQueries = false;
//		BooleanJunction<BooleanJunction> inUse = junction;
//
//		if (criteria instanceof AndOr) {
//			LogManager.logTrace(LogConstants.CTX_CONNECTOR,
//					"Parsing compound criteria."); //$NON-NLS-1$
//			AndOr crit = (AndOr) criteria;
//			AndOr.Operator op = crit.getOperator();
//
//			switch (op) {
//			case AND:
//
//				BooleanJunction<BooleanJunction> leftAnd = queryBuilder
//						.bool();
//				boolean andLeftHasQueries = buildQueryFromWhereClause(
//						crit.getLeftCondition(), leftAnd, queryBuilder);
//
//				BooleanJunction<BooleanJunction> rightAnd = queryBuilder
//						.bool();
//				boolean andRightHasQueries = buildQueryFromWhereClause(
//						crit.getRightCondition(), rightAnd, queryBuilder);
//
//				if (andLeftHasQueries && andRightHasQueries) {
//					leftAnd.must(rightAnd.createQuery());
//					inUse.should(leftAnd.createQuery());
//				} else if (andLeftHasQueries) {
//
//					inUse.should(leftAnd.createQuery());
//				} else if (andRightHasQueries) {
//					inUse.should(rightAnd.createQuery());
//				}
//
//				createdQueries = (andLeftHasQueries ? andLeftHasQueries
//						: andRightHasQueries);
//
//				break;
//
//			case OR:
//
//				boolean orLeftHasQueries = buildQueryFromWhereClause(
//						crit.getLeftCondition(), inUse, queryBuilder);
//				boolean orRightHasQueries = buildQueryFromWhereClause(
//						crit.getRightCondition(), inUse, queryBuilder);
//
//				createdQueries = (orLeftHasQueries ? orLeftHasQueries
//						: orRightHasQueries);
//
//				break;
//
//			default:
//				final String msg = InfinispanAPIPlugin.Util
//						.getString("LuceneSearch.invalidOperator", new Object[] { op, "And, Or" }); //$NON-NLS-1$ //$NON-NLS-2$
//				throw new TranslatorException(msg);
//			}
//
//		} else if (criteria instanceof Comparison) {
//			createdQueries = visit((Comparison) criteria, inUse, queryBuilder);
//
//		} else if (criteria instanceof Exists) {
//			LogManager.logTrace(LogConstants.CTX_CONNECTOR,
//					"Parsing EXISTS criteria: NOT IMPLEMENTED YET"); //$NON-NLS-1$
//			// TODO Exists should be supported in a future release.
//
//		} else if (criteria instanceof Like) {
//			createdQueries = visit((Like) criteria, inUse, queryBuilder);
//
//		} else if (criteria instanceof In) {
//			createdQueries = visit((In) criteria, inUse, queryBuilder);
//
//		}
//		// else if (criteria instanceof Not) {
//		//			LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Parsing NOT criteria."); //$NON-NLS-1$
//		// isNegated = true;
//		// filterList.addAll(getSearchFilterFromWhereClause(((Not)criteria).getCriteria(),
//		// new LinkedList<String>()));
//		// }
//
//		return createdQueries;
//	}

	public static boolean visit(Comparison obj, QueryBuilder queryBuilder) throws TranslatorException {

		LogManager.logTrace(LogConstants.CTX_CONNECTOR,
				"Parsing Comparison criteria."); //$NON-NLS-1$
		Comparison.Operator op = obj.getOperator();

		Expression lhs = obj.getLeftExpression();
		Expression rhs = obj.getRightExpression();

		// joins between the objects in the same cache is not usable
		if ((lhs instanceof ColumnReference && rhs instanceof ColumnReference)
				|| (lhs instanceof Literal && rhs instanceof Literal)) {
			return false;
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
			final String msg = InfinispanPlugin.Util
					.getString("LuceneSearch.unsupportedComparingByNull"); //$NON-NLS-1$
			throw new TranslatorException(msg);
		}

		value = escapeReservedChars(value);
		switch (op) {
		case NE:
			createEqualsQuery(mdIDElement, value, false, true, queryBuilder);
			break;

		case EQ:
			createEqualsQuery(mdIDElement, value, true, false, queryBuilder);
			break;

		case GT:
			createRangeAboveQuery(mdIDElement, value, queryBuilder);
			break;

		case LT:
			createRangeBelowQuery(mdIDElement, value, queryBuilder);
			break;

		default:
			final String msg = InfinispanPlugin.Util
					.getString("LuceneSearch.invalidOperator", new Object[] { op, "NE, EQ, GT, LT" }); //$NON-NLS-1$ //$NON-NLS-2$
			throw new TranslatorException(msg);
		}
		return true;

	}

	public static boolean visit(In obj, QueryBuilder queryBuilder) throws TranslatorException {
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Parsing IN criteria."); //$NON-NLS-1$

		Expression lhs = obj.getLeftExpression();

		Column mdIDElement = ((ColumnReference) lhs).getMetadataObject();

		List<Expression> rhsList = obj.getRightExpressions();
		boolean createdQuery = false;
		for (Expression expr : rhsList) {

			if (expr instanceof Literal) {
				Literal literal = (Literal) expr;

				// add these as OR queries
				createEqualsQuery(mdIDElement,
						escapeReservedChars(literal.getValue()), false, false, queryBuilder);
				createdQuery = true;
			} else {
				String msg = InfinispanPlugin.Util.getString(
						"LuceneSearch.Unsupported_expression", //$NON-NLS-1$
						new Object[] { expr, "IN" }); //$NON-NLS-1$
				throw new TranslatorException(msg);
			}
		}
		return createdQuery;
	}

	public static boolean visit(Like obj, QueryBuilder queryBuilder) throws TranslatorException {
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
			createLikeQuery(c, value.replaceAll("%", ""),  queryBuilder); // "*" //$NON-NLS-1$ //$NON-NLS-2$
		} else {
			final String msg = InfinispanPlugin.Util.getString(
					"LuceneSearch.Unsupported_expression", //$NON-NLS-1$
					new Object[] { literalExp.toString(), "LIKE" }); //$NON-NLS-1$
			throw new TranslatorException(msg);
		}

		return true;
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

	private static void createEqualsQuery(Column column, Object value, boolean and,
			boolean not, QueryBuilder queryBuilder) {
        
        queryBuilder.having(getNameInSource(column)).eq(value.toString());

//		if (not) {
//			junction.must(queryKey).not();
//		} else if (and) {
//			junction.must(queryKey);
//		} else {
//			junction.should(queryKey);
//		}
//		return queryKey;
	}

	private static void createRangeAboveQuery(Column column, Object value, QueryBuilder queryBuilder) {
//
//		Query queryKey = queryBuilder.range()
//				.onField(ObjectExecution.getNameInSource(column))
//				.above(value.toString()).excludeLimit().createQuery();
//		junction.must(queryKey);
//		return queryKey;
	}

	private static void createRangeBelowQuery(Column column, Object value, QueryBuilder queryBuilder) {

//		Query queryKey = queryBuilder.range()
//				.onField(ObjectExecution.getNameInSource(column))
//				.below(value.toString()).excludeLimit().createQuery();
//		junction.must(queryKey);
//		return queryKey;
	}

	private static void createLikeQuery(Column column, String value, QueryBuilder queryBuilder) {
		
		queryBuilder.having(getNameInSource(column)).like(value);

	}
	
	public static String getNameInSource(AbstractMetadataRecord c) {
		String name = c.getNameInSource();
		if (name == null || name.trim().isEmpty()) {
			return c.getName();
		}
		return name;
	}

}
