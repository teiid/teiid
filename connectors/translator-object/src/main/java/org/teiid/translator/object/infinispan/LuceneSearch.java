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
package org.teiid.translator.object.infinispan;

import java.util.Collections;
import java.util.List;

import org.apache.lucene.search.Query;
import org.hibernate.search.query.dsl.BooleanJunction;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.infinispan.Cache;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.teiid.language.AndOr;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Condition;
import org.teiid.language.Delete;
import org.teiid.language.Exists;
import org.teiid.language.Expression;
import org.teiid.language.In;
import org.teiid.language.Like;
import org.teiid.language.Literal;
import org.teiid.language.Select;
import org.teiid.language.Update;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectPlugin;
import org.teiid.translator.object.SearchType;



/**
 * LuceneSearch will parse the WHERE criteria and build the search query(s)
 * that's used to retrieve the results from an Infinispan cache.
 * 
 * @author vhalbert
 * 
 */
public class LuceneSearch implements SearchType  {
	
	@Override
	public Object performKeySearch(String cacheName, String columnNameInSource, Object value, ObjectConnection connection) throws TranslatorException {
	   
		LogManager.logTrace(LogConstants.CTX_CONNECTOR,
				"Perform Lucene KeySearch."); //$NON-NLS-1$
		
		Cache<?,?> c = (Cache<?, ?>) connection.getCacheContainer().getCache(cacheName);
		return c.get(String.valueOf(value));
	}

	@Override
	public List<Object> performSearch(Update command, String cacheName, ObjectConnection connection)
			throws TranslatorException {
		return performSearch(command.getWhere(), cacheName, connection);
	}
	
	@Override
	public List<Object> performSearch(Delete command, String cacheName, ObjectConnection connection)
			throws TranslatorException {	
		return performSearch(command.getWhere(), cacheName, connection);
	}

	@Override
	public List<Object> performSearch(Select command, String cacheName, ObjectConnection connection)
			throws TranslatorException {	
		return performSearch(command.getWhere(), cacheName, connection);
	}


	private static List<Object> performSearch(Condition where, String cacheName, ObjectConnection connection)
			throws TranslatorException {
		
		LogManager.logTrace(LogConstants.CTX_CONNECTOR,
				"Using Lucene Searching."); //$NON-NLS-1$
		
		Class<?> type = connection.getType(cacheName);
		
		//Map<?, ?> cache, 
		SearchManager searchManager = Search
				.getSearchManager((Cache<?, ?>) connection.getCacheContainer().getCache(cacheName) );

		QueryBuilder queryBuilder = searchManager.buildQueryBuilderForClass(type).get();

		BooleanJunction<BooleanJunction> junction = queryBuilder.bool();
		boolean createdQueries = buildQueryFromWhereClause(where,
				junction, queryBuilder);

		Query query = null;
		if (createdQueries) {
			query = junction.createQuery();
			
		} else {
			query = queryBuilder.all().createQuery();
		}

		CacheQuery cacheQuery = searchManager.getQuery(query, type); // rootNodeType

		List<Object> results = cacheQuery.list();
		if (results == null || results.isEmpty()) {
			return Collections.emptyList();
		}

		return results;
	}

	private static boolean buildQueryFromWhereClause(Condition criteria,
			BooleanJunction<BooleanJunction> junction, QueryBuilder queryBuilder)
			throws TranslatorException {
		boolean createdQueries = false;
		BooleanJunction<BooleanJunction> inUse = junction;

		if (criteria instanceof AndOr) {
			LogManager.logTrace(LogConstants.CTX_CONNECTOR,
					"Parsing compound criteria."); //$NON-NLS-1$
			AndOr crit = (AndOr) criteria;
			AndOr.Operator op = crit.getOperator();

			switch (op) {
			case AND:

				BooleanJunction<BooleanJunction> leftAnd = queryBuilder
						.bool();
				boolean andLeftHasQueries = buildQueryFromWhereClause(
						crit.getLeftCondition(), leftAnd, queryBuilder);

				BooleanJunction<BooleanJunction> rightAnd = queryBuilder
						.bool();
				boolean andRightHasQueries = buildQueryFromWhereClause(
						crit.getRightCondition(), rightAnd, queryBuilder);

				if (andLeftHasQueries && andRightHasQueries) {
					leftAnd.must(rightAnd.createQuery());
					inUse.should(leftAnd.createQuery());
				} else if (andLeftHasQueries) {

					inUse.should(leftAnd.createQuery());
				} else if (andRightHasQueries) {
					inUse.should(rightAnd.createQuery());
				}

				createdQueries = (andLeftHasQueries ? andLeftHasQueries
						: andRightHasQueries);

				break;

			case OR:

				boolean orLeftHasQueries = buildQueryFromWhereClause(
						crit.getLeftCondition(), inUse, queryBuilder);
				boolean orRightHasQueries = buildQueryFromWhereClause(
						crit.getRightCondition(), inUse, queryBuilder);

				createdQueries = (orLeftHasQueries ? orLeftHasQueries
						: orRightHasQueries);

				break;

			default:
				final String msg = ObjectPlugin.Util
						.getString("LuceneSearch.invalidOperator", new Object[] { op, "And, Or" }); //$NON-NLS-1$ //$NON-NLS-2$
				throw new TranslatorException(msg);
			}

		} else if (criteria instanceof Comparison) {
			createdQueries = visit((Comparison) criteria, inUse, queryBuilder);

		} else if (criteria instanceof Exists) {
			LogManager.logTrace(LogConstants.CTX_CONNECTOR,
					"Parsing EXISTS criteria: NOT IMPLEMENTED YET"); //$NON-NLS-1$
			// TODO Exists should be supported in a future release.

		} else if (criteria instanceof Like) {
			createdQueries = visit((Like) criteria, inUse, queryBuilder);

		} else if (criteria instanceof In) {
			createdQueries = visit((In) criteria, inUse, queryBuilder);

		}
		// else if (criteria instanceof Not) {
		//			LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Parsing NOT criteria."); //$NON-NLS-1$
		// isNegated = true;
		// filterList.addAll(getSearchFilterFromWhereClause(((Not)criteria).getCriteria(),
		// new LinkedList<String>()));
		// }

		return createdQueries;
	}

	public static boolean visit(Comparison obj,
			BooleanJunction<BooleanJunction> junction, QueryBuilder queryBuilder) throws TranslatorException {

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
			final String msg = ObjectPlugin.Util
					.getString("LuceneSearch.unsupportedComparingByNull"); //$NON-NLS-1$
			throw new TranslatorException(msg);
		}

		value = escapeReservedChars(value);
		switch (op) {
		case NE:
			createEqualsQuery(mdIDElement, value, false, true, junction, queryBuilder);
			break;

		case EQ:
			createEqualsQuery(mdIDElement, value, true, false, junction, queryBuilder);
			break;

		case GT:
			createRangeAboveQuery(mdIDElement, value, junction, queryBuilder);
			break;

		case LT:
			createRangeBelowQuery(mdIDElement, value, junction, queryBuilder);
			break;

		default:
			final String msg = ObjectPlugin.Util
					.getString("LuceneSearch.invalidOperator", new Object[] { op, "NE, EQ, GT, LT" }); //$NON-NLS-1$ //$NON-NLS-2$
			throw new TranslatorException(msg);
		}
		return true;

	}

	public static boolean visit(In obj, BooleanJunction<BooleanJunction> junction, QueryBuilder queryBuilder) throws TranslatorException {
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
						escapeReservedChars(literal.getValue()), false, false,
						junction, queryBuilder);
				createdQuery = true;
			} else {
				String msg = ObjectPlugin.Util.getString(
						"LuceneSearch.Unsupported_expression", //$NON-NLS-1$
						new Object[] { expr, "IN" }); //$NON-NLS-1$
				throw new TranslatorException(msg);
			}
		}
		return createdQuery;
	}

	public static boolean visit(Like obj, BooleanJunction<BooleanJunction> junction, QueryBuilder queryBuilder) throws TranslatorException {
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
			createLikeQuery(c, value.replaceAll("%", ""), junction, queryBuilder); // "*" //$NON-NLS-1$ //$NON-NLS-2$
		} else {
			final String msg = ObjectPlugin.Util.getString(
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

	private static Query createEqualsQuery(Column column, Object value, boolean and,
			boolean not, BooleanJunction<BooleanJunction> junction, QueryBuilder queryBuilder) {
		String nis = column.getNameInSource();
		return createEqualsQuery(   (nis != null ? nis : column.getName()), value, and, not, junction, queryBuilder);
		
//		Query queryKey = queryBuilder.keyword()
//				.onField(column.getNameInSource())
//				.matching(value).createQuery();
//
//		if (not) {
//			junction.must(queryKey).not();
//		} else if (and) {
//			junction.must(queryKey);
//		} else if (junction != null) {
//			junction.should(queryKey);
//		}
//		return queryKey;
	}
	
	private static Query createEqualsQuery(String nameInSource, Object value, boolean and,
			boolean not, BooleanJunction<BooleanJunction> junction, QueryBuilder queryBuilder) {
		Query queryKey = queryBuilder.keyword()
				.onField(nameInSource)
				.matching(value).createQuery();

		if (not) {
			junction.must(queryKey).not();
		} else if (and) {
			junction.must(queryKey);
		} else if (junction != null) {
			junction.should(queryKey);
		}
		return queryKey;
	}	

	private static Query createRangeAboveQuery(Column column, Object value,
			BooleanJunction<BooleanJunction> junction, QueryBuilder queryBuilder) {

		Query queryKey = queryBuilder.range()
				.onField(column.getNameInSource())
				.above(value).excludeLimit().createQuery();
		junction.must(queryKey);
		return queryKey;
	}

	private static Query createRangeBelowQuery(Column column, Object value,
			BooleanJunction<BooleanJunction> junction, QueryBuilder queryBuilder) {

		Query queryKey = queryBuilder.range()
				.onField(column.getNameInSource())
				.below(value).excludeLimit().createQuery();
		junction.must(queryKey);
		return queryKey;
	}

	private static Query createLikeQuery(Column column, String value,
			BooleanJunction<BooleanJunction> junction, QueryBuilder queryBuilder) {
		Query queryKey = queryBuilder.phrase()
				.onField(column.getNameInSource()).sentence(value)
				.createQuery();
		junction.should(queryKey);
		return queryKey;
	}

}
