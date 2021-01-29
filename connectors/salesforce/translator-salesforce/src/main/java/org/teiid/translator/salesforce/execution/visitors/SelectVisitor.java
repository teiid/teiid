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


import static org.teiid.language.SQLConstants.Reserved.DESC;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.language.AggregateFunction;
import org.teiid.language.AndOr.Operator;
import org.teiid.language.ColumnReference;
import org.teiid.language.Condition;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.GroupBy;
import org.teiid.language.LanguageFactory;
import org.teiid.language.Limit;
import org.teiid.language.NamedTable;
import org.teiid.language.OrderBy;
import org.teiid.language.SQLConstants;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.Select;
import org.teiid.language.SortSpecification;
import org.teiid.language.SortSpecification.Ordering;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.salesforce.SalesForceMetadataProcessor;
import org.teiid.translator.salesforce.SalesForcePlugin;


public class SelectVisitor extends CriteriaVisitor implements IQueryProvidingVisitor {
    public static final String AGG_PREFIX = "expr"; //$NON-NLS-1$
    private Map<Integer, Expression> selectSymbolIndexToElement = new HashMap<Integer, Expression>();
    private int selectSymbolCount;
    protected List<DerivedColumn> selectSymbols;
    protected StringBuilder limitClause = new StringBuilder();
    protected StringBuilder groupByClause = new StringBuilder();
    protected StringBuilder havingClause = new StringBuilder();
    protected StringBuilder orderByClause = new StringBuilder();
    private Boolean objectSupportsRetrieve;
    private Condition implicitCondition;
    private boolean selectAggregate;

    public SelectVisitor(RuntimeMetadata metadata) {
        super(metadata);
    }

    @Override
    public void visit(Select query) {
        super.visitNodes(query.getFrom());

        Condition condition = query.getWhere();
        if (this.implicitCondition != null) {
            if (condition != null) {
                condition = LanguageFactory.INSTANCE.createAndOr(Operator.AND, condition, this.implicitCondition);
            }
            else {
                condition = implicitCondition;
            }
        }

        super.visitNode(condition);
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
        super.visitNode(query.getOrderBy());
        super.visitNode(query.getLimit());
        if (query.isDistinct()) {
            exceptions.add(new TranslatorException(SalesForcePlugin.Util.getString("SelectVisitor.distinct.not.supported"))); //$NON-NLS-1$
        }
        selectSymbols = query.getDerivedColumns();
        selectSymbolCount = selectSymbols.size();
        for (int index = 0; index < selectSymbols.size(); index++) {
            DerivedColumn symbol = selectSymbols.get(index);
            // get the name in source
            Expression expression = symbol.getExpression();
            selectSymbolIndexToElement.put(index, expression);
        }
    }

    protected void addCriteria(Condition condition) {
        this.implicitCondition  = condition;
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
            table = obj;
            String supportsQuery = table.getMetadataObject().getProperty(SalesForceMetadataProcessor.TABLE_SUPPORTS_QUERY, true);
            objectSupportsRetrieve = Boolean.valueOf(table.getMetadataObject().getProperty(SalesForceMetadataProcessor.TABLE_SUPPORTS_RETRIEVE, true));
            if (supportsQuery != null && !Boolean.valueOf(supportsQuery)) {
                throw new TranslatorException(table.getMetadataObject().getSourceName() + " " + SalesForcePlugin.Util.getString("CriteriaVisitor.query.not.supported")); //$NON-NLS-1$ //$NON-NLS-2$
            }
            loadColumnMetadata(obj);
        } catch (TranslatorException ce) {
            exceptions.add(ce);
        }
    }

    @Override
    public void visit(OrderBy orderBy) {
        List<SortSpecification> items = orderBy.getSortSpecifications();
        orderByClause.append(ORDER_BY).append(SPACE);
        //only 1 item is supported, but we'll let that manifest as a backend
        //error
        for (Iterator<SortSpecification> iter = items.iterator(); iter.hasNext();) {
            SortSpecification spec = iter.next();
            orderByClause.append(getValue(spec.getExpression(), false));
            if (spec.getOrdering() == Ordering.DESC) {
                orderByClause.append(Tokens.SPACE)
                      .append(DESC);
            } // Don't print default "ASC"
            if (spec.getNullOrdering() != null) {
                orderByClause.append(Tokens.SPACE)
                    .append(NonReserved.NULLS)
                    .append(Tokens.SPACE)
                    .append(spec.getNullOrdering().name());
            }
            if (iter.hasNext()) {
                orderByClause.append(COMMA).append(SPACE);
            }
        }
        this.orderByClause.append(SPACE);
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

    @Override
    public String getQuery() throws TranslatorException {
        if (!exceptions.isEmpty()) {
            throw exceptions.get(0);
        }

        StringBuilder result = new StringBuilder();
        result.append(SELECT).append(SPACE);
        addSelectSymbols(result);
        result.append(SPACE);

        result.append(FROM).append(SPACE);
        result.append(table.getMetadataObject().getSourceName()).append(SPACE);
        addCriteriaString(result);
        appendGroupByHaving(result);
        result.append(orderByClause);
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
                this.selectAggregate = true;
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

    public boolean getQueryAll() {
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
            throw new AssertionError("hasOnlyIdInCriteria is false"); //$NON-NLS-1$
        }
    }

    public boolean hasOnlyIdInCriteria() {
        return hasOnlyIDCriteria() && idInCriteria != null;
    }

    public boolean isSelectAggregate() {
        return selectAggregate;
    }

    public boolean canRetrieve() {
        return objectSupportsRetrieve && hasOnlyIDCriteria() && this.limitClause.length() == 0 && groupByClause.length() == 0 && orderByClause.length() == 0;
    }

}
