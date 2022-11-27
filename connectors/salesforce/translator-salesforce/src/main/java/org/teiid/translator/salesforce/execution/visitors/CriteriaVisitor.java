/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.translator.salesforce.execution.visitors;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.language.AggregateFunction;
import org.teiid.language.AndOr;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.In;
import org.teiid.language.IsNull;
import org.teiid.language.Like;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.Not;
import org.teiid.language.SQLConstants;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.salesforce.SalesForceExecutionFactory;
import org.teiid.translator.salesforce.SalesForceMetadataProcessor;
import org.teiid.translator.salesforce.SalesForcePlugin;
import org.teiid.translator.salesforce.Util;


/**
 * Parses Criteria in support of all of the ExecutionImpl classes.
 */
public class CriteriaVisitor extends HierarchyVisitor implements ICriteriaVisitor {

    private static final double SCIENTIFIC_LOW = Math.pow(10, -3);
    private static final double SCIENTIFIC_HIGH = Math.pow(10, 7);

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

    //buffer of criteria parts
    protected List<String> criteriaBuffer = new ArrayList<String>();
    protected List<TranslatorException> exceptions = new ArrayList<TranslatorException>();
    protected NamedTable table;
    Boolean onlyIDCriteria = null;
    protected boolean queryAll = Boolean.FALSE;

    // support for invoking a retrieve when possible.
    protected In idInCriteria = null;


    public CriteriaVisitor( RuntimeMetadata metadata ) {
        this.metadata = metadata;
    }

    @Override
    public void visit( Comparison criteria ) {
        addCompareCriteria(criteria);
        boolean isAcceptableID = (Operator.EQ == criteria.getOperator() && isIdColumn(criteria.getLeftExpression()));
        setHasCriteria(isAcceptableID);
        if (isAcceptableID) {
            this.idInCriteria = new In(criteria.getLeftExpression(), Arrays.asList(criteria.getRightExpression()), false);
        }
    }

    @Override
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
        if (negated) {
            criteriaBuffer.add("NOT ("); //$NON-NLS-1$
        }
        criteriaBuffer.add(getValue(criteria.getLeftExpression(), false));
        criteriaBuffer.add(SPACE + SQLConstants.Reserved.LIKE + SPACE);
        criteriaBuffer.add(getValue(criteria.getRightExpression(), false));
        if (negated) {
            criteriaBuffer.add(CLOSE);
            criteria.setNegated(true);
        }
        // don't check if it's ID, Id LIKE '123%' still requires a query
        setHasCriteria(false);
    }

    @Override
    public void visit(AndOr obj) {
        this.criteriaBuffer.add(OPEN);
        super.visitNode(obj.getLeftCondition());
        this.criteriaBuffer.add(CLOSE);
        this.criteriaBuffer.add(SPACE);
        this.criteriaBuffer.add(obj.getOperator().toString());
        this.criteriaBuffer.add(SPACE);
        this.criteriaBuffer.add(OPEN);
        super.visitNode(obj.getRightCondition());
        this.criteriaBuffer.add(CLOSE);
    }

    @Override
    public void visit(Not obj) {
        criteriaBuffer.add("NOT ("); //$NON-NLS-1$
        super.visit(obj);
        criteriaBuffer.add(CLOSE);
    }

    @Override
    public void visit( In criteria ) {
        Expression lExpr = criteria.getLeftExpression();
        if (lExpr instanceof ColumnReference) {
            ColumnReference cr = (ColumnReference)lExpr;
            Column column = cr.getMetadataObject();
            if (column != null && (MULTIPICKLIST.equalsIgnoreCase(column.getNativeType()) || RESTRICTEDMULTISELECTPICKLIST.equalsIgnoreCase(column.getNativeType()))) {
                appendMultiselectIn(cr, criteria);
            } else {
                appendCriteria(criteria);
            }
        } else {
            appendCriteria(criteria);
        }
        criteriaBuffer.add(OPEN);
        Iterator<Expression> iter = criteria.getRightExpressions().iterator();
        while (iter.hasNext()) {
            criteriaBuffer.add(getValue(iter.next(), false));
            if (iter.hasNext()) {
                criteriaBuffer.add(COMMA);
            }
        }
        criteriaBuffer.add(CLOSE);
        setHasCriteria(!criteria.isNegated()
                && isIdColumn(criteria.getLeftExpression())
                && criteria.getRightExpressions()
                        .size() <= SalesForceExecutionFactory.MAX_RETRIEVE);
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
        criteriaBuffer.add(column.getSourceName());
        criteriaBuffer.add(SPACE);
        criteriaBuffer.add(funcName);
        criteriaBuffer.add(OPEN);
        //TODO: this should be a vararg array, rather than a single value
        String fullParam = (String)((Literal)expressions.get(1)).getValue();
        String[] params = fullParam.split(","); //$NON-NLS-1$
        for (int i = 0; i < params.length; i++) {
            String token = params[i];
            if (i != 0) {
                criteriaBuffer.add(COMMA);
            }
            criteriaBuffer.add("'" + token + "'"); //$NON-NLS-1$ //$NON-NLS-2$
        }
        criteriaBuffer.add(CLOSE);
    }

    private void appendCriteria( In criteria ) {
        Expression leftExp = criteria.getLeftExpression();
        if(isIdColumn(leftExp)) {
            idInCriteria  = criteria;
        }
        criteriaBuffer.add(getValue(leftExp, false));
        criteriaBuffer.add(SPACE);
        if (criteria.isNegated()) {
            criteriaBuffer.add("NOT "); //$NON-NLS-1$
        }
        criteriaBuffer.add("IN"); //$NON-NLS-1$
    }

    private void appendMultiselectIn( ColumnReference column,
                                      In criteria ) {
        StringBuilder sb = new StringBuilder();
        appendColumnReference(sb, column);
        criteriaBuffer.add(sb.toString());
        criteriaBuffer.add(SPACE);
        if (criteria.isNegated()) {
            criteriaBuffer.add(EXCLUDES);
        } else {
            criteriaBuffer.add(INCLUDES);
        }
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

    protected void addCompareCriteria(Comparison compCriteria ) {
        Expression lExpr = compCriteria.getLeftExpression();
        if (lExpr instanceof Function) {
            parseFunction((Function)lExpr);
        } else {
            criteriaBuffer.add(getValue(lExpr, false));
            criteriaBuffer.add(SPACE);
            criteriaBuffer.add(compCriteria.getOperator()==Operator.NE?"!=":compCriteria.getOperator().toString()); //$NON-NLS-1$
            criteriaBuffer.add(SPACE);
            Expression rExp = compCriteria.getRightExpression();
            criteriaBuffer.add(getValue(rExp, false));

            if (lExpr instanceof ColumnReference && "IsDeleted".equalsIgnoreCase(((ColumnReference)lExpr).getMetadataObject().getSourceName())) { //$NON-NLS-1$
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
        queryString.append(ref.getMetadataObject().getSourceName());
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
            appendLiteralValue(result, literal);
        } else if (expr instanceof AggregateFunction) {
            appendAggregateFunction(result, (AggregateFunction)expr);
        } else {
            throw new RuntimeException("unknown type in SalesforceQueryExecution.getValue(): " + expr.toString()); //$NON-NLS-1$
        }
        return result.toString();
    }

    public static void appendLiteralValue(StringBuilder result, Literal literal) {
        Object v = literal.getValue();
        if (v.getClass().equals(Boolean.class)) {
            result.append(((Boolean)v).toString());
        } else if (v.getClass().equals(java.sql.Timestamp.class)) {
            Timestamp datetime = (java.sql.Timestamp)v;
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
        } else if (v.getClass().equals(java.sql.Time.class)) {
            result.append(v).append(".000").append(Util.getDefaultTimeZoneString()); //$NON-NLS-1$
        } else if (v.getClass().equals(java.sql.Date.class)) {
            result.append(v);
        } else if (v instanceof Double) {
            Double doubleVal = (Double)v;
            double value = Math.abs(doubleVal.doubleValue());
            if (value <= SCIENTIFIC_LOW || value >= SCIENTIFIC_HIGH) {
                result.append(BigDecimal.valueOf(doubleVal).toPlainString());
            } else {
                result.append(literal.toString());
            }
        } else if (v instanceof Float) {
            Float floatVal = (Float)v;
            float value = Math.abs(floatVal);
            if (value <= SCIENTIFIC_LOW || value >= SCIENTIFIC_HIGH) {
                result.append(BigDecimal.valueOf(floatVal).toPlainString());
            } else {
                result.append(literal.toString());
            }
        } else if (v instanceof BigDecimal) {
            result.append(((BigDecimal)v).toPlainString());
        } else if (v instanceof String) {
            String stringVal = (String)v;
            stringVal = stringVal.replaceAll("['\\\\]", "\\\\$0");
            result.append("'").append(stringVal).append("'");
        } else {
            result.append(literal.toString());
        }
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
        table = group;
        String supportsQuery = table.getMetadataObject().getProperty(SalesForceMetadataProcessor.TABLE_SUPPORTS_QUERY, true);
        if (supportsQuery != null && !Boolean.valueOf(supportsQuery)) {
            throw new TranslatorException(table.getMetadataObject().getSourceName() + " " + SalesForcePlugin.Util.getString("CriteriaVisitor.query.not.supported")); //$NON-NLS-1$ //$NON-NLS-2$
        }
        List<Column> columnIds = table.getMetadataObject().getColumns();
        for (Column element : columnIds) {
            // influences queryAll behavior
            if (element.getSourceName().equals("IsDeleted")) { //$NON-NLS-1$
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
            String nameInSource = element.getSourceName();
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

    public void setHasCriteria( boolean isIdCriteria ) {
        if (isIdCriteria && onlyIDCriteria == null) {
            onlyIDCriteria = Boolean.TRUE;
        } else {
            onlyIDCriteria = Boolean.FALSE;
        }
    }

    @Override
    public boolean hasOnlyIDCriteria() {
        return Boolean.TRUE.equals(this.onlyIDCriteria);
    }

    @Override
    public String getTableName() {
        return table.getMetadataObject().getSourceName();
    }

    protected void addCriteriaString(StringBuilder result) {
        addCriteriaString(WHERE, result);
    }

    protected void addCriteriaString(String clause, StringBuilder result) {
        if(!criteriaBuffer.isEmpty()) {
            result.append(clause).append(SPACE);
            for (String string : criteriaBuffer) {
                result.append(string);
            }
            result.append(SPACE);
        }
    }
}
