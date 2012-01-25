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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.language.*;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.salesforce.SalesForcePlugin;
import org.teiid.translator.salesforce.Util;


/**
 * Parses Criteria in support of all of the ExecutionImpl classes.
 */
public class CriteriaVisitor extends HierarchyVisitor implements ICriteriaVisitor {

    private static final String RESTRICTEDMULTISELECTPICKLIST = "restrictedmultiselectpicklist"; //$NON-NLS-1$
	private static final String MULTIPICKLIST = "multipicklist"; //$NON-NLS-1$
	protected static final String SELECT = "SELECT"; //$NON-NLS-1$
    protected static final String FROM = "FROM"; //$NON-NLS-1$
    protected static final String WHERE = "WHERE"; //$NON-NLS-1$
    protected static final String ORDER_BY = "ORDER BY"; //$NON-NLS-1$
    protected static final String LIMIT = "LIMIT"; //$NON-NLS-1$
    protected static final String SPACE = " "; //$NON-NLS-1$
    protected static final String EXCLUDES = "EXCLUDES"; //$NON-NLS-1$
    protected static final String INCLUDES = "includes"; //$NON-NLS-1$
    protected static final String COMMA = ","; //$NON-NLS-1$
    protected static final String SEMI = ";"; //$NON-NLS-1$
    protected static final String APOS = "'"; //$NON-NLS-1$
    protected static final String OPEN = "("; //$NON-NLS-1$
    protected static final String CLOSE = ")"; //$NON-NLS-1$

    protected RuntimeMetadata metadata;
    
    protected List<String> criteriaList = new ArrayList<String>();
    protected boolean hasCriteria;
    protected List<TranslatorException> exceptions = new ArrayList<TranslatorException>();
    protected Table table;
    boolean onlyIDCriteria;
    protected Boolean queryAll = Boolean.FALSE;
    
    // support for invoking a retrieve when possible.
    protected In idInCriteria = null;
	

    public CriteriaVisitor( RuntimeMetadata metadata ) {
        this.metadata = metadata;
    }

    @Override
    public void visit( Comparison criteria ) {
        addCompareCriteria(criteria);
        boolean isAcceptableID = (Operator.EQ == criteria.getOperator() && isIdColumn(criteria.getLeftExpression()));
        setHasCriteria(true, isAcceptableID);
        if (isAcceptableID) {
        	this.idInCriteria = new In(criteria.getLeftExpression(), Arrays.asList(criteria.getRightExpression()), false);
        }
    }
    
    public void visit(IsNull obj) {
    	visit(new Comparison(obj.getExpression(), new Literal(null, obj.getExpression().getType()), obj.isNegated()?Comparison.Operator.NE:Comparison.Operator.EQ));
    }
    
    @Override
    public void visit( Like criteria ) {
        if (isIdColumn(criteria.getLeftExpression())) {
            TranslatorException e = new TranslatorException(SalesForcePlugin.Util.getString("CriteriaVisitor.LIKE.not.supported.on.Id")); //$NON-NLS-1$
            exceptions.add(e);
        }
        if (isMultiSelectColumn(criteria.getLeftExpression())) {
            TranslatorException e = new TranslatorException(SalesForcePlugin.Util.getString("CriteriaVisitor.LIKE.not.supported.on.multiselect")); //$NON-NLS-1$
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
        Expression lExpr = criteria.getLeftExpression();
        if (lExpr instanceof ColumnReference) {
        	ColumnReference cr = (ColumnReference)lExpr;
            Column column = cr.getMetadataObject();
            if (column != null && (MULTIPICKLIST.equalsIgnoreCase(column.getNativeType()) || RESTRICTEDMULTISELECTPICKLIST.equalsIgnoreCase(column.getNativeType()))) {
                appendMultiselectIn(column, criteria);
            } else {
                appendCriteria(criteria);
            }
        } else {
        	appendCriteria(criteria);
        }
        setHasCriteria(true, isIdColumn(criteria.getLeftExpression()));
    }

    public void parseFunction( Function func ) {
        String functionName = func.getName();
        try {
            if (functionName.equalsIgnoreCase("includes")) { //$NON-NLS-1$
                generateMultiSelect(func, INCLUDES);
            } else if (functionName.equalsIgnoreCase("excludes")) { //$NON-NLS-1$
                generateMultiSelect(func, EXCLUDES);
            }
        } catch (TranslatorException e) {
            exceptions.add(e);
        }
    }

    private void generateMultiSelect( Function func,
                                      String funcName ) throws TranslatorException {
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
                                      In criteria ) {
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

    private void validateFunction( List<Expression> expressions ) throws TranslatorException {
        if (expressions.size() != 2) {
            throw new TranslatorException(SalesForcePlugin.Util.getString("CriteriaVisitor.invalid.arg.count")); //$NON-NLS-1$
        }
        if (!(expressions.get(0) instanceof ColumnReference)) {
            throw new TranslatorException(SalesForcePlugin.Util.getString("CriteriaVisitor.function.not.column.arg")); //$NON-NLS-1$
        }
        if (!(expressions.get(1) instanceof Literal)) {
            throw new TranslatorException(SalesForcePlugin.Util.getString("CriteriaVisitor.function.not.literal.arg")); //$NON-NLS-1$
        }
    }

    private void addFunctionParams( Literal param,
                                    StringBuffer criterion ) {
        criterion.append(OPEN);
        boolean first = true;
        String fullParam = param.toString();
        String[] params = fullParam.split(","); //$NON-NLS-1$
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
    
    protected void addCompareCriteria(Comparison compCriteria ) {
        Expression lExpr = compCriteria.getLeftExpression();
        if (lExpr instanceof Function) {
            parseFunction((Function)lExpr);
        } else {
            StringBuilder queryString = new StringBuilder();
            queryString.append(getValue(lExpr, false));
            queryString.append(SPACE);
            queryString.append(compCriteria.getOperator()==Operator.NE?"!=":compCriteria.getOperator()); //$NON-NLS-1$
            queryString.append(' ');
            Expression rExp = compCriteria.getRightExpression();
            queryString.append(getValue(rExp, false));
            criteriaList.add(queryString.toString());

            if (lExpr instanceof ColumnReference && "IsDeleted".equalsIgnoreCase(((ColumnReference)lExpr).getMetadataObject().getNameInSource())) { //$NON-NLS-1$
                Literal isDeletedLiteral = (Literal)compCriteria.getRightExpression();
                Boolean isDeleted = (Boolean)isDeletedLiteral.getValue();
                if (isDeleted) {
                    this.queryAll = isDeleted;
                }
            }
        }
    }

	void appendColumnReference(StringBuilder queryString,
			ColumnReference ref) {
		queryString.append(ref.getMetadataObject().getParent().getNameInSource());
		queryString.append('.');
		queryString.append(ref.getMetadataObject().getNameInSource());
	}

    private void appendCriteria( In criteria ) {
        StringBuffer queryString = new StringBuffer();
        Expression leftExp = criteria.getLeftExpression();
        if(isIdColumn(leftExp)) {
        	idInCriteria  = criteria;
        }
        queryString.append(getValue(leftExp, false));
        queryString.append(' ');
        if (criteria.isNegated()) {
            queryString.append("NOT "); //$NON-NLS-1$
        }
        queryString.append("IN"); //$NON-NLS-1$
        queryString.append('(');
        Iterator<Expression> iter = criteria.getRightExpressions().iterator();
        while (iter.hasNext()) {
            queryString.append(getValue(iter.next(), false));
            if (iter.hasNext()) {
            	queryString.append(',');
            }
        }
        queryString.append(')');
        criteriaList.add(queryString.toString());
    }
    
    protected String getValue( Expression expr, boolean raw) {
        StringBuilder result = new StringBuilder();
        if (expr instanceof ColumnReference) {
        	appendColumnReference(result, (ColumnReference)expr);
        } else if (expr instanceof Literal) {
        	Literal literal = (Literal)expr;
        	if (literal.getValue() == null) {
        		if (raw) {
        			return null;
        		}
        		return "NULL"; //$NON-NLS-1$
    		}
        	if (raw) {
        		return literal.getValue().toString();
        	}
        	if (literal.getValue().getClass().equals(Boolean.class)) {
        		result.append(((Boolean)literal.getValue()).toString());
        	} else if (literal.getValue().getClass().equals(java.sql.Timestamp.class)) {
        		Timestamp datetime = (java.sql.Timestamp)literal.getValue();
        		String value = datetime.toString();
        		int fractionalPlace = value.lastIndexOf('.');
        		int fractionalLength = value.length() - fractionalPlace - 1;
				if (fractionalLength > 3) {
        			value = value.substring(0, fractionalPlace + 3);
        		} else if (fractionalLength < 3) {
        			value += "00".substring(fractionalLength - 1); //$NON-NLS-1$
        		}
        		result.append(value).setCharAt(result.length()-value.length()+10, 'T');
        		Calendar c = TimestampWithTimezone.getCalendar();
        		c.setTime(datetime);
        		int minutes = (c.get(Calendar.ZONE_OFFSET) +
        			     c.get(Calendar.DST_OFFSET)) / 60000;
        		int val = minutes/60;
        		result.append(String.format("%1$+03d", val)); //$NON-NLS-1$
        		result.append(':');
        		val = minutes%60;
    			result.append(val/10);
    			result.append(val%10);
        	} else if (literal.getValue().getClass().equals(java.sql.Time.class)) {
        		result.append(literal.getValue()).append(".000").append(Util.getDefaultTimeZoneString()); //$NON-NLS-1$
        	} else if (literal.getValue().getClass().equals(java.sql.Date.class)) {
        		result.append(literal.getValue());
        	} else {
        		result.append(expr.toString());
        	}
        } else if (expr instanceof AggregateFunction) {
        	appendAggregateFunction(result, (AggregateFunction)expr);
        } else {
            throw new RuntimeException("unknown type in SalesforceQueryExecution.getValue(): " + expr.toString()); //$NON-NLS-1$
        }
        return result.toString();
    }
    
	protected void appendAggregateFunction(StringBuilder result,
			AggregateFunction af) {
		if (af.getName().equalsIgnoreCase(SQLConstants.NonReserved.COUNT) 
				&& (af.getExpression() == null || af.getExpression() instanceof Literal)) {
			result.append("COUNT(Id)"); //$NON-NLS-1$
		} else {
			result.append(af.getName() + "(" + getValue(af.getExpression(), false) + ")"); //$NON-NLS-1$ //$NON-NLS-2$
		}
	}

    protected void loadColumnMetadata( NamedTable group ) throws TranslatorException {
        table = group.getMetadataObject();
        String supportsQuery = table.getProperties().get("Supports Query"); //$NON-NLS-1$
        if (!Boolean.valueOf(supportsQuery)) {
            throw new TranslatorException(table.getNameInSource() + " " + SalesForcePlugin.Util.getString("CriteriaVisitor.query.not.supported")); //$NON-NLS-1$ //$NON-NLS-2$
        }
        List<Column> columnIds = table.getColumns();
        for (Column element : columnIds) {
            // influences queryAll behavior
            if (element.getNameInSource().equals("IsDeleted")) { //$NON-NLS-1$
                String isDeleted = element.getDefaultValue();
                if (Boolean.parseBoolean(isDeleted)) {
                    this.queryAll = true;
                }
            }
        }
    }

    protected boolean isIdColumn( Expression expression ) {
        boolean result = false;
        if (expression instanceof ColumnReference) {
            Column element = ((ColumnReference)expression).getMetadataObject();
            String nameInSource = element.getNameInSource();
            if (nameInSource.equalsIgnoreCase("id")) { //$NON-NLS-1$
                result = true;
            }
        }
        return result;
    }

    protected boolean isMultiSelectColumn( Expression expression ) {
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

    public String getTableName() throws TranslatorException {
        return table.getNameInSource();
    }
    
    protected void addCriteriaString(StringBuilder result) {
    	addCriteriaString(WHERE, result);
	}
    
    protected void addCriteriaString(String clause, StringBuilder result) {
    	if(hasCriteria()) {
			result.append(clause).append(SPACE);
			for (String string : criteriaList) {
				result.append(string);
			}
			result.append(SPACE);
		}
	}
}
