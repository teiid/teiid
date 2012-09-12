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
package org.teiid.translator.object.infinispan.search;

import java.util.ArrayList;
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
import org.teiid.language.Exists;
import org.teiid.language.Expression;
import org.teiid.language.In;
import org.teiid.language.Like;
import org.teiid.language.Literal;
import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectExecutionFactory;
import org.teiid.translator.object.ObjectPlugin;
import org.teiid.translator.object.SearchStrategy;
import org.teiid.translator.object.SelectProjections;
import org.teiid.translator.object.infinispan.InfinispanExecutionFactory;

/**
 * LuceneSearch will parse the WHERE criteria and build the search query(s)
 * that's used to retrieve the results from an Infinispan cache.
 * 
 * @author vhalbert
 * 
 */
public class LuceneSearch implements SearchStrategy {
	protected List<String> exceptionMessages = new ArrayList<String>(2);

	private QueryBuilder queryBuilder;

	public LuceneSearch() {
	}

	public List<Object> performSearch(Select command,
			SelectProjections projections,
			ObjectExecutionFactory objectFactory, Object connection)
			throws TranslatorException {

		InfinispanExecutionFactory factory = (InfinispanExecutionFactory) objectFactory;

		SearchManager searchManager = Search
				.getSearchManager((Cache<?, ?>) factory.getCache(connection));

		queryBuilder = searchManager.buildQueryBuilderForClass(
				factory.getRootClass()).get();

		BooleanJunction<BooleanJunction> junction = queryBuilder.bool();
		boolean createdQueries = buildQueryFromWhereClause(command.getWhere(),
				junction);

		// check for errors
		this.throwExceptionIfFound();

		Query query = null;
		if (createdQueries) {
			query = junction.createQuery();
		} else {
			query = queryBuilder.all().createQuery();
		}

		CacheQuery cacheQuery = searchManager.getQuery(query,
				factory.getRootClass()); // rootNodeType

		List<Object> results = cacheQuery.list();
		if (results == null || results.isEmpty()) {
			return Collections.emptyList();
		}

		return results;
	}

	private boolean buildQueryFromWhereClause(Condition criteria,
			BooleanJunction<BooleanJunction> junction)
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

				BooleanJunction<BooleanJunction> leftAnd = this.queryBuilder
						.bool();
				boolean andLeftHasQueries = buildQueryFromWhereClause(
						crit.getLeftCondition(), leftAnd);

				BooleanJunction<BooleanJunction> rightAnd = this.queryBuilder
						.bool();
				boolean andRightHasQueries = buildQueryFromWhereClause(
						crit.getRightCondition(), rightAnd);

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
						crit.getLeftCondition(), inUse);
				boolean orRightHasQueries = buildQueryFromWhereClause(
						crit.getRightCondition(), inUse);

				createdQueries = (orLeftHasQueries ? orLeftHasQueries
						: orRightHasQueries);

				break;

			default:
				final String msg = ObjectPlugin.Util
						.getString("LuceneSearch.invalidOperator", new Object[] { op, "And, Or" }); //$NON-NLS-1$
				throw new TranslatorException(msg);
			}

		} else if (criteria instanceof Comparison) {
			createdQueries = visit((Comparison) criteria, inUse);

		} else if (criteria instanceof Exists) {
			LogManager.logTrace(LogConstants.CTX_CONNECTOR,
					"Parsing EXISTS criteria: NOT IMPLEMENTED YET"); //$NON-NLS-1$
			// TODO Exists should be supported in a future release.

		} else if (criteria instanceof Like) {
			createdQueries = visit((Like) criteria, inUse);

		} else if (criteria instanceof In) {
			createdQueries = visit((In) criteria, inUse);

		}
		// else if (criteria instanceof Not) {
		//			LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Parsing NOT criteria."); //$NON-NLS-1$
		// isNegated = true;
		// filterList.addAll(getSearchFilterFromWhereClause(((Not)criteria).getCriteria(),
		// new LinkedList<String>()));
		// }

		return createdQueries;
	}

	public boolean visit(Comparison obj,
			BooleanJunction<BooleanJunction> junction) {

		LogManager.logTrace(LogConstants.CTX_CONNECTOR,
				"Parsing Comparison criteria."); //$NON-NLS-1$
		Comparison.Operator op = ((Comparison) obj).getOperator();

		Expression lhs = ((Comparison) obj).getLeftExpression();
		Expression rhs = ((Comparison) obj).getRightExpression();

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
			addException(msg);
			return false;
		}

		value = escapeReservedChars(value);
		switch (op) {
		case NE:
			createEqualsQuery(mdIDElement, value, false, true, junction);
			break;

		case EQ:
			createEqualsQuery(mdIDElement, value, true, false, junction);
			break;

		case GT:
			createRangeAboveQuery(mdIDElement, value, junction);
			break;

		case LT:
			createRangeBelowQuery(mdIDElement, value, junction);
			break;

		default:
			final String msg = ObjectPlugin.Util
					.getString("LuceneSearch.unsupportedComparisonOperator"); //$NON-NLS-1$
			addException(msg);
			return false;
		}
		return true;

	}

	public boolean visit(In obj, BooleanJunction<BooleanJunction> junction) {
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Parsing IN criteria."); //$NON-NLS-1$

		Expression lhs = ((In) obj).getLeftExpression();

		Column mdIDElement = ((ColumnReference) lhs).getMetadataObject();

		List<Expression> rhsList = ((In) obj).getRightExpressions();
		boolean createdQuery = false;
		for (Expression expr : rhsList) {

			if (expr instanceof Literal) {
				Literal literal = (Literal) expr;

				// add these as OR queries
				createEqualsQuery(mdIDElement,
						escapeReservedChars(literal.getValue()), false, false,
						junction);
				createdQuery = true;
			} else {
				String msg = ObjectPlugin.Util.getString(
						"LuceneSearch.Unsupported_expression",
						new Object[] { expr, "IN" });
				this.addException(msg);
			}
		}
		return createdQuery;
	}

	public boolean visit(Like obj, BooleanJunction<BooleanJunction> junction) {
		LogManager.logTrace(LogConstants.CTX_CONNECTOR,
				"Parsing LIKE criteria."); //$NON-NLS-1$

		Expression lhs = ((Like) obj).getLeftExpression();
		Expression rhs = ((Like) obj).getRightExpression();

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
			createLikeQuery(c, value.replaceAll("%", ""), junction); // "*"
			return true;
		} else {
			final String msg = ObjectPlugin.Util.getString(
					"LuceneSearch.Unsupported_expression",
					new Object[] { literalExp.toString(), "LIKE" });
			this.addException(msg);
			return false;

		}

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

	private Query createEqualsQuery(Column column, Object value, boolean and,
			boolean not, BooleanJunction<BooleanJunction> junction) {
		Query queryKey = queryBuilder.keyword()
				.onField(getNameInSourceFromColumn(column))
				// .matching(value.toString() + "*")
				.matching(value.toString()).createQuery();

		if (not) {
			junction.must(queryKey).not();
		} else if (and) {
			junction.must(queryKey);
		} else {
			junction.should(queryKey);
		}
		return queryKey;
	}

	private Query createRangeAboveQuery(Column column, Object value,
			BooleanJunction<BooleanJunction> junction) {

		Query queryKey = queryBuilder.range()
				.onField(getNameInSourceFromColumn(column))
				.above(value.toString()).excludeLimit().createQuery();
		junction.must(queryKey);
		return queryKey;
	}

	private Query createRangeBelowQuery(Column column, Object value,
			BooleanJunction<BooleanJunction> junction) {

		Query queryKey = queryBuilder.range()
				.onField(getNameInSourceFromColumn(column))
				.below(value.toString()).excludeLimit().createQuery();
		junction.must(queryKey);
		return queryKey;
	}

	private Query createLikeQuery(Column column, String value,
			BooleanJunction<BooleanJunction> junction) {
		Query queryKey = queryBuilder.phrase()
				.onField(getNameInSourceFromColumn(column)).sentence(value)
				.createQuery();
		junction.should(queryKey);
		return queryKey;
	}

	private String getNameInSourceFromColumn(Column c) {
		String name = c.getNameInSource();
		if (name == null || name.trim().equals("")) { //$NON-NLS-1$
			return c.getName();
		}
		return name;
	}

	private void addException(String message) {

		exceptionMessages.add(message);

	}

	protected void throwExceptionIfFound() throws TranslatorException {
		if (!exceptionMessages.isEmpty())
			throw new TranslatorException("LuceneSearch Exception(s): "
					+ exceptionMessages.toString());
	}

}
