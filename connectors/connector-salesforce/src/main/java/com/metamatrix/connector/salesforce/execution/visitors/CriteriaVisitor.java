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
package com.metamatrix.connector.salesforce.execution.visitors;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.language.ICompareCriteria;
import com.metamatrix.connector.language.IElement;
import com.metamatrix.connector.language.IExpression;
import com.metamatrix.connector.language.IFunction;
import com.metamatrix.connector.language.IGroup;
import com.metamatrix.connector.language.IInCriteria;
import com.metamatrix.connector.language.ILikeCriteria;
import com.metamatrix.connector.language.ILiteral;
import com.metamatrix.connector.language.ICompareCriteria.Operator;
import com.metamatrix.connector.metadata.runtime.Element;
import com.metamatrix.connector.metadata.runtime.Group;
import com.metamatrix.connector.metadata.runtime.MetadataID;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;
import com.metamatrix.connector.salesforce.Messages;
import com.metamatrix.connector.salesforce.Util;
import com.metamatrix.connector.visitor.framework.HierarchyVisitor;

/**
 * 
 * Parses Criteria in support of all of the ExecutionImpl classes.
 *
 */
public abstract class CriteriaVisitor extends HierarchyVisitor implements ICriteriaVisitor {

	protected static final String SELECT = "SELECT";
	protected static final String FROM = "FROM";
	protected static final String WHERE = "WHERE";
	protected static final String ORDER_BY = "ORDER BY";
	protected static final String LIMIT = "LIMIT";
	protected static final String SPACE = " ";
	protected static final String EXCLUDES = "EXCLUDES";
	protected static final String INCLUDES = "includes";
	protected static final String COMMA = ",";
	protected static final String SEMI = ";";
	protected static final String APOS = "'";
	protected static final String OPEN = "(";
	protected static final String CLOSE = ")";
	
	protected RuntimeMetadata metadata;
	private HashMap<ICompareCriteria.Operator, String> comparisonOperators;
	protected List<String> criteriaList = new ArrayList<String>();
	protected boolean hasCriteria;
	protected Map<String, Element> columnElementsByName = new HashMap<String, Element>();
	protected List<ConnectorException> exceptions = new ArrayList<ConnectorException>();
	protected Group table;
	boolean onlyIDCriteria;

	public CriteriaVisitor(RuntimeMetadata metadata) {
		this.metadata = metadata;
		comparisonOperators = new HashMap<ICompareCriteria.Operator, String>();
		comparisonOperators.put(Operator.EQ, "=");
		comparisonOperators.put(Operator.GE, ">=");
		comparisonOperators.put(Operator.GT, ">");
		comparisonOperators.put(Operator.LE, "<=");
		comparisonOperators.put(Operator.LT, "<");
		comparisonOperators.put(Operator.NE, "!=");		
	}
	
	@Override
	public void visit(ICompareCriteria criteria) {
		super.visit(criteria);
		try {
			addCompareCriteria(criteriaList, criteria);
			boolean isAcceptableID = (Operator.EQ == criteria.getOperator() &&
					isIdColumn(criteria.getLeftExpression()));
			setHasCriteria(true, isAcceptableID);
		} catch (ConnectorException e) {
			exceptions.add(e);
		}
	}

	@Override
	public void visit(ILikeCriteria criteria) {
		super.visit(criteria);
		try {
			if(isIdColumn(criteria.getLeftExpression())) {
				ConnectorException e = new ConnectorException(Messages
						.getString("CriteriaVisitor.LIKE.not.supported.on.Id"));
				exceptions.add(e);
			}
			if(isMultiSelectColumn(criteria.getLeftExpression())) {
				ConnectorException e = new ConnectorException(Messages
						.getString("CriteriaVisitor.LIKE.not.supported.on.multiselect"));
				exceptions.add(e);
			}
		} catch (ConnectorException e) {
			exceptions.add(e);
		}
		criteriaList.add(criteria.toString());
		// don't check if it's ID, Id LIKE '123%' still requires a query
		setHasCriteria(true, false);
	}

	@Override
	public void visit(IInCriteria criteria) {
		super.visit(criteria);
		try {
			IExpression lExpr = criteria.getLeftExpression();
			String columnName = lExpr.toString();
			if(columnElementsByName.containsKey(columnName)) {
				Element column = (Element) columnElementsByName.get(columnName);
				if(null != column.getNativeType() && (column.getNativeType().equals("multipicklist") ||
						column.getNativeType().equals("restrictedmultiselectpicklist"))) {
					appendMultiselectIn(column, criteria);
				} else {
					appendCriteria(criteriaList, criteria);
				}
			}
			setHasCriteria(true, isIdColumn(criteria.getLeftExpression()));
		} catch (ConnectorException e) {
			exceptions.add(e);
		}
	}
	
	public void parseFunction(IFunction func) {
		super.visit(func);
		String functionName = func.getName();
		try {
			if (functionName.equalsIgnoreCase("includes")) {
				generateMultiSelect(func, INCLUDES);
			} else if (functionName.equalsIgnoreCase("excludes")) {
				generateMultiSelect(func, EXCLUDES);
			}
		} catch (ConnectorException e) {
			exceptions.add(e);
		}
	}

	private void generateMultiSelect(IFunction func, String funcName) throws ConnectorException {
		IExpression[] expressions = func.getParameters();
		validateFunction(expressions);
		IExpression columnExpression = expressions[0]; 
		Element column = (Element)metadata.getObject(((IElement)columnExpression).getMetadataID());
		StringBuffer criterion = new StringBuffer();
		criterion.append(column.getNameInSource()).append(SPACE).append(funcName);
		addFunctionParams((ILiteral)expressions[1], criterion);
		criteriaList.add(criterion.toString());
	}
	
	private void appendMultiselectIn(Element column, IInCriteria criteria) throws ConnectorException {
		StringBuffer result = new StringBuffer();
		IExpression left = criteria.getLeftExpression();
		result.append(column.getNameInSource()).append(SPACE);
		if(criteria.isNegated()) {
			result.append(EXCLUDES).append(SPACE);
		} else {
			result.append(INCLUDES).append(SPACE);
		}
		result.append('(');
		List<IExpression> rightExpressions = criteria.getRightExpressions();
		Iterator<IExpression> iter = rightExpressions.iterator();
		boolean first = true;
		while(iter.hasNext()) {
			IExpression rightExpression = iter.next();
			if(first) {
				result.append(rightExpression.toString());
				first = false;
			} else {
				result.append(COMMA).append(rightExpression.toString());
			}
			
		}
		result.append(')');
		criteriaList.add(result.toString());
	}

	private void validateFunction(IExpression[] expressions) throws ConnectorException {
		if(expressions.length != 2) {
			throw new ConnectorException(Messages.getString("CriteriaVisitor.invalid.arg.count"));
		}
		IExpression columnExpression = expressions[0];
		if(!(expressions[0] instanceof IElement)) {
			throw new ConnectorException(Messages.getString("CriteriaVisitor.function.not.column.arg"));
		}
		if(!(expressions[1] instanceof ILiteral)) {
			throw new ConnectorException(Messages.getString("CriteriaVisitor.function.not.literal.arg"));
		}
	}

	private void addFunctionParams(ILiteral param,
			StringBuffer criterion) {
		criterion.append(OPEN);
		boolean first = true;
		String fullParam = param.toString();
		String[] params = fullParam.split(",");
		for(int i = 0; i < params.length; i++) {
			String token = params[i];
			if(first) {
				criterion.append(SPACE).append(Util.addSingleQuotes(token));
				first = false;
			} else {
				criterion.append(COMMA).append(SPACE).append(Util.addSingleQuotes(token));
			}
		}
		criterion.append(CLOSE);
	}
	
	protected void addCompareCriteria(List criteriaList,
			ICompareCriteria compCriteria) throws ConnectorException {
		IExpression lExpr = compCriteria.getLeftExpression();
		if(lExpr instanceof IFunction) {
			parseFunction((IFunction)lExpr);
		} else {
		IElement left = (IElement) lExpr;
		Element column = (Element) metadata.getObject(left.getMetadataID());
		String columnName = column.getNameInSource();
		StringBuffer queryString = new StringBuffer();
		queryString.append(columnName).append(SPACE);
		queryString.append(comparisonOperators.get(compCriteria.getOperator()));
		queryString.append(' ');
		ILiteral literal = (ILiteral) compCriteria.getRightExpression();
		if (column.getJavaType().equals(Boolean.class)) {
			queryString.append(((Boolean) literal.getValue()).toString());
		} else if (column.getJavaType().equals(java.sql.Timestamp.class)) {
			Timestamp datetime = (java.sql.Timestamp)literal.getValue();
			String value = Util.getSalesforceDateTimeFormat().format(datetime);
			String zoneValue = Util.getTimeZoneOffsetFormat().format(datetime);
			queryString.append(value).append(zoneValue.subSequence(0, 3)).append(':').append(zoneValue.subSequence(3, 5));
		} else if (column.getJavaType().equals(java.sql.Time.class)) {
			String value = Util.getSalesforceDateTimeFormat().format((java.sql.Time)literal.getValue());
			queryString.append(value);
		} else if (column.getJavaType().equals(java.sql.Date.class)) {
			String value = Util.getSalesforceDateFormat().format((java.sql.Date)literal.getValue());
			queryString.append(value);
		} else {
			queryString.append(compCriteria.getRightExpression().toString());
		}
		
		criteriaList.add(queryString.toString());
		}
	}
	
	private void appendCriteria(List criteriaList, IInCriteria criteria) throws ConnectorException {
		StringBuffer queryString = new StringBuffer();
		queryString.append(' ');
		queryString.append(getValue(criteria.getLeftExpression()));
		queryString.append(' ');
		queryString.append("IN");
		queryString.append('(');
		Element column = (Element) metadata.getObject(
				((IElement)criteria.getLeftExpression()).getMetadataID());
		boolean timeColumn = isTimeColumn(column);
		boolean first = true;
		Iterator iter = criteria.getRightExpressions().iterator();
		while (iter.hasNext()) {
			if (!first)
				queryString.append(',');
			if (!timeColumn) queryString.append('\'');
			queryString.append(getValue((IExpression) iter.next()));
			if (!timeColumn) queryString.append('\'');
			first = false;
		}
		queryString.append(')');
		criteriaList.add(queryString.toString());
	}

	private boolean isTimeColumn(Element column) throws ConnectorException {
		boolean result = false;
		if (column.getJavaType().equals(java.sql.Timestamp.class)
				|| column.getJavaType().equals(java.sql.Time.class)
				|| column.getJavaType().equals(java.sql.Date.class)) {
			result = true;
		}
		return result;
	}

	private String getValue(IExpression expr) throws ConnectorException {
		String result;
		if (expr instanceof IElement) {
			IElement element = (IElement) expr;
			Element element2 = (Element) metadata.getObject(element
					.getMetadataID());
			result = element2.getNameInSource();
		} else if (expr instanceof ILiteral) {
			ILiteral literal = (ILiteral) expr;
			result = literal.getValue().toString();
		} else {
			throw new RuntimeException(
					"unknown type in SalesforceQueryExecution.getValue(): "
							+ expr.toString());
		}
		return result;
	}

	protected void loadColumnMetadata(IGroup group) throws ConnectorException {
		MetadataID id = group.getMetadataID();
		table = (Group) metadata.getObject(id);
		String supportsQuery = (String) table.getProperties().get(
				"Supports Query");
		if (!Util.convertStringToBoolean(supportsQuery)) {
			throw new ConnectorException(
					table.getNameInSource()
							+ " "
							+ Messages
									.getString("CriteriaVisitor.query.not.supported"));
		}
		List<MetadataID> columnIds = table.getMetadataID().getChildIDs();
		Iterator<MetadataID> iter = columnIds.iterator();
		while (iter.hasNext()) {
			MetadataID columnID = (MetadataID) iter.next();
			Element element = (Element) metadata.getObject(columnID);
			String name = columnID.getParentID().getName() + '.'
					+ columnID.getName();
			columnElementsByName.put(name, element);
		}
	}
	
	protected boolean isIdColumn(IExpression expression)
			throws ConnectorException {
		boolean result = false;
		if (expression instanceof IElement) {
			MetadataID elementID = (MetadataID) ((IElement) expression)
					.getMetadataID();
			Element element = (Element) metadata.getObject(elementID);
			String nameInSource = element.getNameInSource();
			if (nameInSource.equalsIgnoreCase("id")) {
				result = true;
			}
		}
		return result;
	}

	protected boolean isMultiSelectColumn(IExpression expression)
			throws ConnectorException {
		boolean result = false;
		if (expression instanceof IElement) {
			MetadataID elementID = (MetadataID) ((IElement) expression)
					.getMetadataID();
			Element element = (Element) metadata.getObject(elementID);
			String nativeType = element.getNativeType();
			if (nativeType.equalsIgnoreCase("multipicklist") || 
					nativeType.equalsIgnoreCase("restrictedmultiselectpicklist")) {
				result = true;
			}
		}
		return result;
	}
	
	public boolean hasCriteria() {
		return hasCriteria;
	}

	public void setHasCriteria(boolean hasCriteria, boolean isIdCriteria) {
		if(isIdCriteria) {
			if(hasCriteria()) {
				this.onlyIDCriteria = false;
			} else {
				this.onlyIDCriteria = true;
			}
		} else if (this.onlyIDCriteria) {
			this.onlyIDCriteria = false;
		}
		this.hasCriteria = hasCriteria;
	}

	public boolean hasOnlyIDCriteria() {
		return this.onlyIDCriteria;
	}

	public String getTableName() throws ConnectorException {
		return table.getNameInSource();
	}
}
