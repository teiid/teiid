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
package org.teiid.resource.adapter.salesforce.execution.visitors;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.teiid.connector.language.AndOr;
import org.teiid.connector.language.ColumnReference;
import org.teiid.connector.language.Comparison;
import org.teiid.connector.language.Expression;
import org.teiid.connector.language.Function;
import org.teiid.connector.language.In;
import org.teiid.connector.language.Like;
import org.teiid.connector.language.Literal;
import org.teiid.connector.language.NamedTable;
import org.teiid.connector.language.Not;
import org.teiid.connector.language.Comparison.Operator;
import org.teiid.connector.metadata.runtime.Column;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;
import org.teiid.connector.metadata.runtime.Table;
import org.teiid.connector.visitor.framework.HierarchyVisitor;
import org.teiid.resource.ConnectorException;
import org.teiid.resource.adapter.salesforce.Messages;
import org.teiid.resource.adapter.salesforce.Util;


/**
 * Parses Criteria in support of all of the ExecutionImpl classes.
 */
public abstract class CriteriaVisitor extends HierarchyVisitor implements ICriteriaVisitor {

    private static final String RESTRICTEDMULTISELECTPICKLIST = "restrictedmultiselectpicklist";
	private static final String MULTIPICKLIST = "multipicklist";
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
    private HashMap<Comparison.Operator, String> comparisonOperators;
    protected List<String> criteriaList = new ArrayList<String>();
    protected boolean hasCriteria;
    protected Map<String, Column> columnElementsByName = new HashMap<String, Column>();
    protected List<ConnectorException> exceptions = new ArrayList<ConnectorException>();
    protected Table table;
    boolean onlyIDCriteria;
    protected Boolean queryAll = Boolean.FALSE;
	
    // support for invoking a retrieve when possible.
    protected In idInCriteria = null;
	

    public CriteriaVisitor( RuntimeMetadata metadata ) {
        this.metadata = metadata;
        comparisonOperators = new HashMap<Comparison.Operator, String>();
        comparisonOperators.put(Operator.EQ, "=");
        comparisonOperators.put(Operator.GE, ">=");
        comparisonOperators.put(Operator.GT, ">");
        comparisonOperators.put(Operator.LE, "<=");
        comparisonOperators.put(Operator.LT, "<");
        comparisonOperators.put(Operator.NE, "!=");
    }

    @Override
    public void visit( Comparison criteria ) {
        super.visit(criteria);
        try {
            addCompareCriteria(criteriaList, criteria);
            boolean isAcceptableID = (Operator.EQ == criteria.getOperator() && isIdColumn(criteria.getLeftExpression()));
            setHasCriteria(true, isAcceptableID);
        } catch (ConnectorException e) {
            exceptions.add(e);
        }
    }

    @Override
    public void visit( Like criteria ) {
        try {
            if (isIdColumn(criteria.getLeftExpression())) {
                ConnectorException e = new ConnectorException(Messages.getString("CriteriaVisitor.LIKE.not.supported.on.Id"));
                exceptions.add(e);
            }
            if (isMultiSelectColumn(criteria.getLeftExpression())) {
                ConnectorException e = new ConnectorException(
                                                              Messages.getString("CriteriaVisitor.LIKE.not.supported.on.multiselect"));
                exceptions.add(e);
            }
        } catch (ConnectorException e) {
            exceptions.add(e);
        }
        boolean negated = criteria.isNegated();
        criteria.setNegated(false);
        criteriaList.add(criteria.toString());
        if (negated) {
        	addNot();
        	criteria.setNegated(true);
        }
        // don't check if it's ID, Id LIKE '123%' still requires a query
        setHasCriteria(true, false);
    }
    
    @Override
    public void visit(AndOr obj) {
    	List<String> savedCriteria = new LinkedList<String>();
    	savedCriteria.add(OPEN);
		super.visitNode(obj.getLeftCondition());
		savedCriteria.addAll(this.criteriaList);
		this.criteriaList.clear();
		savedCriteria.add(CLOSE);
		savedCriteria.add(SPACE);
		savedCriteria.add(obj.getOperator().toString());
		savedCriteria.add(SPACE);
		savedCriteria.add(OPEN);
		super.visitNode(obj.getRightCondition());
		savedCriteria.addAll(this.criteriaList);
		this.criteriaList.clear();
		this.criteriaList = savedCriteria;
		this.criteriaList.add(CLOSE);
    }
    
    @Override
    public void visit(Not obj) {
    	super.visit(obj);
    	addNot();
    }

	private void addNot() {
		if (!criteriaList.isEmpty()) {
    		criteriaList.add(0, "NOT ("); //$NON-NLS-1$
    		criteriaList.add(CLOSE);
    	}
	}

    @Override
    public void visit( In criteria ) {
        try {
            Expression lExpr = criteria.getLeftExpression();
            String columnName = lExpr.toString();
            if (columnElementsByName.containsKey(columnName)) {
                Column column = columnElementsByName.get(columnName);
                if (MULTIPICKLIST.equalsIgnoreCase(column.getNativeType()) || RESTRICTEDMULTISELECTPICKLIST.equalsIgnoreCase(column.getNativeType())) {
                    appendMultiselectIn(column, criteria);
                } else {
                    appendCriteria(criteria);
                }
            }
            setHasCriteria(true, isIdColumn(criteria.getLeftExpression()));
        } catch (ConnectorException e) {
            exceptions.add(e);
        }
    }

    public void parseFunction( Function func ) {
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

    private void generateMultiSelect( Function func,
                                      String funcName ) throws ConnectorException {
        List<Expression> expressions = func.getParameters();
        validateFunction(expressions);
        Expression columnExpression = expressions.get(0);
        Column column = ((ColumnReference)columnExpression).getMetadataObject();
        StringBuffer criterion = new StringBuffer();
        criterion.append(column.getNameInSource()).append(SPACE).append(funcName);
        addFunctionParams((Literal)expressions.get(1), criterion);
        criteriaList.add(criterion.toString());
    }

    private void appendMultiselectIn( Column column,
                                      In criteria ) throws ConnectorException {
        StringBuffer result = new StringBuffer();
        result.append(column.getNameInSource()).append(SPACE);
        if (criteria.isNegated()) {
            result.append(EXCLUDES).append(SPACE);
        } else {
            result.append(INCLUDES).append(SPACE);
        }
        result.append('(');
        List<Expression> rightExpressions = criteria.getRightExpressions();
        Iterator<Expression> iter = rightExpressions.iterator();
        boolean first = true;
        while (iter.hasNext()) {
            Expression rightExpression = iter.next();
            if (first) {
                result.append(rightExpression.toString());
                first = false;
            } else {
                result.append(COMMA).append(rightExpression.toString());
            }

        }
        result.append(')');
        criteriaList.add(result.toString());
    }

    private void validateFunction( List<Expression> expressions ) throws ConnectorException {
        if (expressions.size() != 2) {
            throw new ConnectorException(Messages.getString("CriteriaVisitor.invalid.arg.count"));
        }
        if (!(expressions.get(0) instanceof ColumnReference)) {
            throw new ConnectorException(Messages.getString("CriteriaVisitor.function.not.column.arg"));
        }
        if (!(expressions.get(1) instanceof Literal)) {
            throw new ConnectorException(Messages.getString("CriteriaVisitor.function.not.literal.arg"));
        }
    }

    private void addFunctionParams( Literal param,
                                    StringBuffer criterion ) {
        criterion.append(OPEN);
        boolean first = true;
        String fullParam = param.toString();
        String[] params = fullParam.split(",");
        for (int i = 0; i < params.length; i++) {
            String token = params[i];
            if (first) {
                criterion.append(SPACE).append(Util.addSingleQuotes(token));
                first = false;
            } else {
                criterion.append(COMMA).append(SPACE).append(Util.addSingleQuotes(token));
            }
        }
        criterion.append(CLOSE);
    }

    protected void addCompareCriteria( List criteriaList,
                                       Comparison compCriteria ) throws ConnectorException {
        Expression lExpr = compCriteria.getLeftExpression();
        if (lExpr instanceof Function) {
            parseFunction((Function)lExpr);
        } else {
            ColumnReference left = (ColumnReference)lExpr;
            Column column = left.getMetadataObject();
            String columnName = column.getNameInSource();
            StringBuffer queryString = new StringBuffer();
            queryString.append(column.getParent().getNameInSource());
            queryString.append('.');
            queryString.append(columnName).append(SPACE);
            queryString.append(comparisonOperators.get(compCriteria.getOperator()));
            queryString.append(' ');
            Expression rExp = compCriteria.getRightExpression();
            if(rExp instanceof Literal) {
            	Literal literal = (Literal)rExp;
            	if (column.getJavaType().equals(Boolean.class)) {
            		queryString.append(((Boolean)literal.getValue()).toString());
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
            } else if(rExp instanceof ColumnReference) {
            	ColumnReference right = (ColumnReference)lExpr;
                column = left.getMetadataObject();
                columnName = column.getNameInSource();
                queryString.append(columnName);
            }

            criteriaList.add(queryString.toString());

            if (columnName.equals("IsDeleted")) {
                Literal isDeletedLiteral = (Literal)compCriteria.getRightExpression();
                Boolean isDeleted = (Boolean)isDeletedLiteral.getValue();
                if (isDeleted) {
                    this.queryAll = isDeleted;
                }
            }
        }
    }

    private void appendCriteria( In criteria ) throws ConnectorException {
        StringBuffer queryString = new StringBuffer();
        Expression leftExp = criteria.getLeftExpression();
        if(isIdColumn(leftExp)) {
        	idInCriteria  = criteria;
        }
        queryString.append(getValue(leftExp));
        queryString.append(' ');
        if (criteria.isNegated()) {
            queryString.append("NOT ");
        }
        queryString.append("IN");
        queryString.append('(');
        Column column = ((ColumnReference)criteria.getLeftExpression()).getMetadataObject();
        boolean timeColumn = isTimeColumn(column);
        boolean first = true;
        Iterator iter = criteria.getRightExpressions().iterator();
        while (iter.hasNext()) {
            if (!first) queryString.append(',');
            if (!timeColumn) queryString.append('\'');
            queryString.append(getValue((Expression)iter.next()));
            if (!timeColumn) queryString.append('\'');
            first = false;
        }
        queryString.append(')');
        criteriaList.add(queryString.toString());
    }

    private boolean isTimeColumn( Column column ) throws ConnectorException {
        boolean result = false;
        if (column.getJavaType().equals(java.sql.Timestamp.class) || column.getJavaType().equals(java.sql.Time.class)
            || column.getJavaType().equals(java.sql.Date.class)) {
            result = true;
        }
        return result;
    }

    protected String getValue( Expression expr ) throws ConnectorException {
        String result;
        if (expr instanceof ColumnReference) {
            ColumnReference element = (ColumnReference)expr;
            Column element2 = element.getMetadataObject();
            result = element2.getNameInSource();
        } else if (expr instanceof Literal) {
            Literal literal = (Literal)expr;
            result = literal.getValue().toString();
        } else {
            throw new RuntimeException("unknown type in SalesforceQueryExecution.getValue(): " + expr.toString());
        }
        return result;
    }

    protected void loadColumnMetadata( NamedTable group ) throws ConnectorException {
        table = group.getMetadataObject();
        String supportsQuery = table.getProperties().get("Supports Query");
        if (!Boolean.valueOf(supportsQuery)) {
            throw new ConnectorException(table.getNameInSource() + " "
                                         + Messages.getString("CriteriaVisitor.query.not.supported"));
        }
        List<Column> columnIds = table.getColumns();
        for (Column element : columnIds) {
            String name = table.getName() + '.' + element.getNameInSource();
            columnElementsByName.put(name, element);

            // influences queryAll behavior
            if (element.getNameInSource().equals("IsDeleted")) {
                String isDeleted = element.getDefaultValue();
                if (Boolean.parseBoolean(isDeleted)) {
                    this.queryAll = true;
                }
            }
        }
    }

    protected boolean isIdColumn( Expression expression ) throws ConnectorException {
        boolean result = false;
        if (expression instanceof ColumnReference) {
            Column element = ((ColumnReference)expression).getMetadataObject();
            String nameInSource = element.getNameInSource();
            if (nameInSource.equalsIgnoreCase("id")) {
                result = true;
            }
        }
        return result;
    }

    protected boolean isMultiSelectColumn( Expression expression ) throws ConnectorException {
        boolean result = false;
        if (expression instanceof ColumnReference) {
            Column element = ((ColumnReference)expression).getMetadataObject();
            String nativeType = element.getNativeType();
            if (MULTIPICKLIST.equalsIgnoreCase(nativeType) || RESTRICTEDMULTISELECTPICKLIST.equalsIgnoreCase(nativeType)) {
                result = true;
            }
        }
        return result;
    }

    public boolean hasCriteria() {
        return hasCriteria;
    }

    public void setHasCriteria( boolean hasCriteria,
                                boolean isIdCriteria ) {
        if (isIdCriteria) {
            if (hasCriteria()) {
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
    
    protected void addCriteriaString(StringBuffer result) {
    	if(hasCriteria()) {
			result.append(WHERE).append(SPACE);
			for (String string : criteriaList) {
				result.append(string);
			}
			result.append(SPACE);
		}
	}
}
