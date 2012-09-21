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
package org.teiid.translator.object.search;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.AggregateFunction;
import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.Comparison;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.In;
import org.teiid.language.Literal;
import org.teiid.language.ScalarSubquery;
import org.teiid.language.SearchedCase;
import org.teiid.language.Select;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectExecutionFactory;
import org.teiid.translator.object.ObjectPlugin;

/**
 * The BasicKeySearchCriteria parses the {@link Command select} and creates
 * {@link SearchCriterion searchCriteria} to support basic key searching on a
 * cache. The EQUALS or IN clauses in the SQL command are the only supported
 * options at this time, with the assumption of OR is used, not AND.
 * 
 * @author vhalbert
 * 
 */
public class BasicKeySearchCriteria extends HierarchyVisitor {

	// search criteria based on the WHERE clause
	private SearchCriterion criterion;

	private List<String> exceptionMessages = new ArrayList<String>(2);

	private BasicKeySearchCriteria(ObjectExecutionFactory factory) {
	}

	public static BasicKeySearchCriteria getInstance(
			ObjectExecutionFactory factory,
			Select command) throws TranslatorException {
		BasicKeySearchCriteria visitor = new BasicKeySearchCriteria(factory);
		visitor.visitNode(command);
		visitor.throwExceptionIfFound();
		return visitor;
	}

	/**
	 * Call to get the {@link SearchCriterion Criterion}. If the command
	 * specified no criteria, then a {@link SearchCriterion Criterion} that
	 * indicates to retrieve "ALL" will be returned.
	 * 
	 * @return
	 */
	public SearchCriterion getCriterion() {
		if (this.criterion != null) {

		} else {
			this.criterion = new SearchCriterion();

		}

		return this.criterion;
	}

	@Override
	public void visit(Comparison obj) {

		LogManager.logTrace(LogConstants.CTX_CONNECTOR,
				"Parsing Comparison criteria."); //$NON-NLS-1$
		Comparison.Operator op = ((Comparison) obj).getOperator();

		Expression lhs = ((Comparison) obj).getLeftExpression();
		Expression rhs = ((Comparison) obj).getRightExpression();

		// comparison between the ojbects is not usable, because the
		// nameInSource and its parent(s)
		// will be how the child objects are obtained
		if ((lhs instanceof ColumnReference && rhs instanceof ColumnReference)
				|| (lhs instanceof Literal && rhs instanceof Literal)) {
			return;
		}

		Object value = null;
		Column mdIDElement = null;
		Literal literal = null;
		if (lhs instanceof ColumnReference && isValidExpression(rhs)) {
			mdIDElement = ((ColumnReference) lhs).getMetadataObject();
			literal = (Literal) rhs;
			value = literal.getValue();

		} else if (rhs instanceof ColumnReference && isValidExpression(lhs)) {
			mdIDElement = ((ColumnReference) rhs).getMetadataObject();
			literal = (Literal) lhs;
			value = literal.getValue();

		}

		if (mdIDElement == null || value == null) {
			String msg = ObjectPlugin.Util
			.getString(
					"BasicKeySearchCriteria.missingComparisonExpression", new Object[] { });
			addException(msg);
			return;
		}

		addCompareCriteria(mdIDElement,
				escapeReservedChars(literal.getValue()), op);

	}

	public void visit(In obj) {
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Parsing IN criteria."); //$NON-NLS-1$

		Expression lhs = ((In) obj).getLeftExpression();
		isValidExpression(lhs);

		Column mdIDElement = ((ColumnReference) lhs).getMetadataObject();

		List<Expression> rhsList = ((In) obj).getRightExpressions();

		Class<?> type = lhs.getType();
		List<Object> parms = new ArrayList<Object>(rhsList.size());
		for (Expression expr : rhsList) {

			if (expr instanceof Literal) {
				Literal literal = (Literal) expr;

				parms.add(escapeReservedChars(literal.getValue()));

				type = literal.getType();

			} else {
				String msg = ObjectPlugin.Util
				.getString(
						"BasicKeySearchCriteria.Unsupported_expression", new Object[] {expr });
				addException(msg);
				return;
			}

		}
		addInCriteria(mdIDElement, parms, type);

	}

	private void addCompareCriteria(Column column, Object value, Operator op) {
		SearchCriterion sc = new SearchCriterion(column, value, op.toString(),
				SearchCriterion.Operator.EQUALS, column.getRuntimeType());

		addSearchCriterion(sc);
	}

	private void addInCriteria(Column column, List<Object> parms, Class<?> type) {
		SearchCriterion sc = new SearchCriterion(column, parms, "in",
				SearchCriterion.Operator.IN, column.getRuntimeType());

		addSearchCriterion(sc);

	}

	private boolean isValidExpression(Expression e) {
		if (e instanceof ColumnReference) {
			return true;
		} else if (e instanceof Literal) {
			return true;

		} else {
			String msg = null;
			if (e instanceof AggregateFunction) {
				msg = ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID12001);
			} else if (e instanceof Function) {
				msg = ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID12005);
			} else if (e instanceof ScalarSubquery) {
				msg = ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID12006);
			} else if (e instanceof SearchedCase) {
				msg = ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID12007);
			}
			LogManager.logError(LogConstants.CTX_CONNECTOR, msg + e.toString());
			addException(msg + e.toString());
		}
		return false;

	}

	private void addSearchCriterion(SearchCriterion searchCriteria) {
		// only searching on primary key is part of the criteria sent for cache
		// lookup
		// all other criteria will be used to filter the rows
		assert (searchCriteria.getTableName() != null);

		assert (searchCriteria.getField() != null);

		if (this.criterion != null) {
			searchCriteria.addOrCriterion(this.criterion);
		}

		this.criterion = searchCriteria;
	}

	protected static Object escapeReservedChars(final Object value) {
		if (value instanceof String) {
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
		return value;
	}

	private void addException(String msg) {
		exceptionMessages.add(msg);
	}

	protected void throwExceptionIfFound() throws TranslatorException {
		if (!exceptionMessages.isEmpty())
			throw new TranslatorException("ObjectProjections Exception: "
					+ exceptionMessages.toString());
	}

}
