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
package org.teiid.translator.salesforce.execution.visitors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.language.*;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.salesforce.Constants;
import org.teiid.translator.salesforce.SalesForcePlugin;


public class SelectVisitor extends CriteriaVisitor implements IQueryProvidingVisitor {

	public static final String AGG_PREFIX = "expr"; //$NON-NLS-1$
	private Map<Integer, Expression> selectSymbolIndexToElement = new HashMap<Integer, Expression>();
	private Map<String, Integer> selectSymbolNameToIndex = new HashMap<String, Integer>();
	private int selectSymbolCount;
	private int idIndex = -1; // index of the ID select symbol.
	protected List<DerivedColumn> selectSymbols;
	protected StringBuilder limitClause = new StringBuilder();
	protected StringBuilder groupByClause = new StringBuilder();
	protected StringBuilder havingClause = new StringBuilder();
	private Boolean objectSupportsRetrieve;
	
	public SelectVisitor(RuntimeMetadata metadata) {
		super(metadata);
	}

	public void visit(Select query) {
		super.visitNodes(query.getFrom());
		super.visitNode(query.getWhere());
		super.visitNode(query.getGroupBy());
		if (query.getHaving() != null) {
			//since the base is a criteria hierarchy visitor,
			//we must separately visit the having clause
			//TODO: if further uses of criteria come up, we should not use hierarchy visitor as the base
			Condition c = query.getHaving();
			CriteriaVisitor cv = new CriteriaVisitor(this.metadata);
			cv.visitNode(c);
			cv.addCriteriaString(SQLConstants.Reserved.HAVING, this.havingClause);
			if (this.havingClause.length() > 0) {
				this.havingClause.append(SPACE);
			}
		}
		super.visitNode(query.getLimit());
		if (query.isDistinct()) {
			exceptions.add(new TranslatorException(SalesForcePlugin.Util.getString("SelectVisitor.distinct.not.supported"))); //$NON-NLS-1$
		}
		selectSymbols = query.getDerivedColumns();
		selectSymbolCount = selectSymbols.size();
		int aggCount = 0;
		for (int index = 0; index < selectSymbols.size(); index++) {
			DerivedColumn symbol = selectSymbols.get(index);
			// get the name in source
			Expression expression = symbol.getExpression();
			selectSymbolIndexToElement.put(index, expression);
			if (expression instanceof ColumnReference) {
				Column element = ((ColumnReference) expression).getMetadataObject();
				String qualifiedName = element.getParent().getNameInSource() + ':' + element.getNameInSource();
				selectSymbolNameToIndex .put(qualifiedName, index);
				String nameInSource = element.getNameInSource();
				if (null == nameInSource || nameInSource.length() == 0) {
					exceptions.add(new TranslatorException("name in source is null or empty for column "+ symbol.toString())); //$NON-NLS-1$
					continue;
				}
				if (nameInSource.equalsIgnoreCase("id")) { //$NON-NLS-1$
					idIndex = index;
				}
			} else if (expression instanceof AggregateFunction) {
				selectSymbolNameToIndex.put(AGG_PREFIX + (aggCount++), index); 
			}
		}
	}
	
	@Override
	public void visit(GroupBy obj) {
		this.groupByClause.append("GROUP BY "); //$NON-NLS-1$
		for (Iterator<Expression> iter = obj.getElements().iterator(); iter.hasNext();) {
			Expression expr = iter.next();
			this.groupByClause.append(getValue(expr, false));
			if (iter.hasNext()) {
				this.groupByClause.append(", "); //$NON-NLS-1$
			}
		}
		this.groupByClause.append(SPACE);
	}
	
	@Override
	public void visit(NamedTable obj) {
		try {
			table = obj.getMetadataObject();
	        String supportsQuery = table.getProperties().get(Constants.SUPPORTS_QUERY);
	        objectSupportsRetrieve = Boolean.valueOf(table.getProperties().get(Constants.SUPPORTS_RETRIEVE));
	        if (!Boolean.valueOf(supportsQuery)) {
	            throw new TranslatorException(table.getNameInSource() + " " + SalesForcePlugin.Util.getString("CriteriaVisitor.query.not.supported")); //$NON-NLS-1$ //$NON-NLS-2$
	        }
			loadColumnMetadata(obj);
		} catch (TranslatorException ce) {
			exceptions.add(ce);
		}
	}
	
	@Override
	public void visit(Limit obj) {
		super.visit(obj);
		limitClause.append(LIMIT).append(SPACE).append(obj.getRowLimit());
	}
	
	/*
	 * The SOQL SELECT command uses the following syntax: SELECT fieldList FROM
	 * objectType [WHERE The Condition Expression (WHERE Clause)] [ORDER BY]
	 * LIMIT ?
	 */

	public String getQuery() throws TranslatorException {
		if (!exceptions.isEmpty()) {
			throw exceptions.get(0);
		}
		StringBuilder result = new StringBuilder();
		result.append(SELECT).append(SPACE);
		addSelectSymbols(result);
		result.append(SPACE);
		result.append(FROM).append(SPACE);
		result.append(table.getNameInSource()).append(SPACE);
		addCriteriaString(result);
		appendGroupByHaving(result);
		//result.append(orderByClause).append(SPACE);
		result.append(limitClause);
		return result.toString();
	}

	protected void appendGroupByHaving(StringBuilder result) {
		result.append(this.groupByClause);
		result.append(this.havingClause);
	}

	private void addSelectSymbols(StringBuilder result) {
		for (int i = 0; i < selectSymbols.size(); i++) {
			DerivedColumn symbol = selectSymbols.get(i);
			if (i > 0) {
				result.append(", "); //$NON-NLS-1$
			}
			Expression expression = symbol.getExpression();
			if (expression instanceof ColumnReference) {
				appendColumnReference(result, (ColumnReference) expression);
			} else if (expression instanceof AggregateFunction) {
				AggregateFunction af = (AggregateFunction)expression;
				appendAggregateFunction(result, af);
			} else {
				throw new AssertionError("Unknown select symbol type" + symbol); //$NON-NLS-1$
			}
		}
	}

	public int getSelectSymbolCount() {
		return selectSymbolCount;
	}

	public Expression getSelectSymbolMetadata(int index) {
		return selectSymbolIndexToElement.get(index);
	}
	
	public Integer getSelectSymbolIndex(String name) {
		return selectSymbolNameToIndex.get(name);
	}
	
	/**
	 * Returns the index of the ID column.
	 * @return the index of the ID column, -1 if there is no ID column.
	 */
	public int getIdIndex() {
		return idIndex;
	}


	public Boolean getQueryAll() {
		return queryAll;
	}


	public String getRetrieveFieldList() {
		assertRetrieveValidated();
		StringBuilder result = new StringBuilder();
		addSelectSymbols(result);
		return result.toString();
	}


	public List<String> getIdInCriteria() {
		assertRetrieveValidated();
		List<Expression> expressions = this.idInCriteria.getRightExpressions();
		List<String> result = new ArrayList<String>(expressions.size());
		for(int i = 0; i < expressions.size(); i++) {
			result.add(getValue(expressions.get(i), true));
		}      
		return result;
	}

	private void assertRetrieveValidated() throws AssertionError {
		if(!hasOnlyIDCriteria()) {
			throw new AssertionError("Must call hasOnlyIdInCriteria() before this method"); //$NON-NLS-1$
		}
	}

	public boolean hasOnlyIdInCriteria() {
		return hasOnlyIDCriteria() && idInCriteria != null;
	}
	
	public boolean canRetrieve() {
		return objectSupportsRetrieve && hasOnlyIDCriteria() && this.limitClause.length() == 0 && groupByClause.length() == 0;
	}

}
