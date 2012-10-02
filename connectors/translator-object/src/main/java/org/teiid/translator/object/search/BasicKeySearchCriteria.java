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

import org.teiid.core.TeiidRuntimeException;
import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.Comparison;
import org.teiid.language.Expression;
import org.teiid.language.In;
import org.teiid.language.Literal;
import org.teiid.language.Select;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.translator.object.ObjectExecutionFactory;

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

	private BasicKeySearchCriteria(ObjectExecutionFactory factory) {
	}

	public static BasicKeySearchCriteria getInstance(
			ObjectExecutionFactory factory,
			Select command) {
		BasicKeySearchCriteria visitor = new BasicKeySearchCriteria(factory);
		visitor.visitNode(command);
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
		if (this.criterion == null) {
			this.criterion = new SearchCriterion();
		}

		return this.criterion;
	}

	@Override
	public void visit(Comparison obj) {
		if (!(obj.getRightExpression() instanceof Literal)) {
			//the translator does not support joins, but the unit tests assume that it does
			return;
		}
		LogManager.logTrace(LogConstants.CTX_CONNECTOR,
				"Parsing Comparison criteria."); //$NON-NLS-1$
		Comparison.Operator op = obj.getOperator();

		Expression lhs = obj.getLeftExpression();
		Expression rhs = obj.getRightExpression();

		Literal literal = (Literal)rhs;
		Column mdIDElement = ((ColumnReference) lhs).getMetadataObject();

		addCompareCriteria(mdIDElement,
				literal.getValue(), op);

	}

	public void visit(In obj) {
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Parsing IN criteria."); //$NON-NLS-1$

		Expression lhs = obj.getLeftExpression();

		Column mdIDElement = ((ColumnReference) lhs).getMetadataObject();

		List<Expression> rhsList = obj.getRightExpressions();

		List<Object> parms = new ArrayList<Object>(rhsList.size());
		for (Expression expr : rhsList) {
			Literal literal = (Literal) expr;

			parms.add(literal.getValue());
		}
		addInCriteria(mdIDElement, parms);

	}

	private void addCompareCriteria(Column column, Object value, Operator op) {
		SearchCriterion sc = new SearchCriterion(column, value, SearchCriterion.Operator.EQUALS,
				column.getRuntimeType());

		addSearchCriterion(sc);
	}

	private void addInCriteria(Column column, List<Object> parms) {
		SearchCriterion sc = new SearchCriterion(column, parms, SearchCriterion.Operator.IN,
				column.getRuntimeType());

		addSearchCriterion(sc);
	}

	private void addSearchCriterion(SearchCriterion searchCriteria) {
		if (this.criterion != null) {
			throw new TeiidRuntimeException("There should not be more than one predicate against the primary key"); //$NON-NLS-1$
		}

		this.criterion = searchCriteria;
	}

}
