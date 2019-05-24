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

package org.teiid.language;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.SortSpecification.Ordering;
import org.teiid.metadata.Column;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Table;

/**
 * Factory for the construction of language objects that implement the language interfaces.
 * This factory is provided by the connector environment and can be used in modifying the language
 * interfaces if needed.
 */
public class LanguageFactory {

    /**
     * Public instance, holds no state so can be shared by everyone.
     */
    public static final LanguageFactory INSTANCE = new LanguageFactory();

    public AggregateFunction createAggregate(String name, boolean isDistinct, Expression expression, Class<?> type) {
        return new AggregateFunction(name, isDistinct, new ArrayList<Expression>(Arrays.asList(expression)), type);
    }

    public Comparison createCompareCriteria(
        Comparison.Operator operator,
        Expression leftExpression,
        Expression rightExpression) {
        return new Comparison(leftExpression, rightExpression, operator);
    }

    public AndOr createAndOr(AndOr.Operator operator, Condition left, Condition right) {
        return new AndOr(left, right, operator);
    }

    public Delete createDelete(NamedTable group, Condition where) {
        return new Delete(group, where);
    }

    public ColumnReference createColumnReference(String name, NamedTable group, Column metadataReference, Class<?> type) {
        return new ColumnReference(group, name, metadataReference, type);
    }

    public Exists createExists(Select query) {
        return new Exists(query);
    }

    public Function createFunction(String functionName, Expression[] args, Class<?> type) {
        return new Function(functionName, Arrays.asList(args), type);
    }

    public Function createFunction(String functionName, List<? extends Expression> args, Class<?> type) {
        return new Function(functionName, args, type);
    }

    public NamedTable createNamedTable(String name, String correlationName, Table metadataReference) {
        return new NamedTable(name, correlationName, metadataReference);
    }

    public GroupBy createGroupBy(List<Expression> items) {
        return new GroupBy(items);
    }

    public In createIn(Expression leftExpression, List<Expression> rightExpressions, boolean isNegated) {
        return new In(leftExpression, rightExpressions, isNegated);
    }

    public Insert createInsert(NamedTable group, List<ColumnReference> columns, InsertValueSource valueSource) {
        return new Insert(group, columns, valueSource);
    }

    public ExpressionValueSource createInsertExpressionValueSource(List<Expression> values) {
        return new ExpressionValueSource(values);
    }

    public IsNull createIsNullCriteria(Expression expression, boolean isNegated) {
        return new IsNull(expression, isNegated);
    }

    public Join createJoin(Join.JoinType joinType, TableReference leftItem, TableReference rightItem, Condition condition) {
        return new Join(leftItem, rightItem, joinType, condition);
    }

    public Like createLikeCriteria(
        Expression leftExpression,
        Expression rightExpression,
        Character escapeCharacter,
        boolean isNegated) {
        return new Like(leftExpression, rightExpression, escapeCharacter, isNegated);
    }

    public Literal createLiteral(Object value, Class<?> type) {
        return new Literal(value, type);
    }

    public Not createNot(Condition criteria) {
        return new Not(criteria);
    }

    public OrderBy createOrderBy(List<SortSpecification> items) {
        return new OrderBy(items);
    }

    public SortSpecification createOrderByItem(ColumnReference element, Ordering direction) {
        return new SortSpecification(direction, element);
    }

    public Argument createArgument(Argument.Direction direction, Expression value, Class<?> type, ProcedureParameter metadataReference) {
        return new Argument(direction, value, type, metadataReference);
    }

    public Call createCall(String name, List<Argument> parameters, Procedure metadataReference) {
        return new Call(name, parameters, metadataReference);
    }

    public Select createQuery(
        List<DerivedColumn> select,
        boolean isDistinct,
        List<TableReference> from,
        Condition where,
        GroupBy groupBy,
        Condition having,
        OrderBy orderBy) {
        return new Select(select, isDistinct, from, where, groupBy, having, orderBy);
    }

    public ScalarSubquery createScalarSubquery(Select query) {
        return new ScalarSubquery(query);
    }

    public SearchedCase createSearchedCaseExpression(
        List<SearchedWhenClause> cases,
        Expression elseExpression,
        Class<?> type) {
        return new SearchedCase(cases, elseExpression, type);
    }

    public SearchedWhenClause createSearchedWhenCondition(Condition condition, Expression result) {
        return new SearchedWhenClause(condition, result);
    }

    public DerivedColumn createSelectSymbol(String name, Expression expression) {
        return new DerivedColumn(name, expression);
    }

    public SubqueryComparison createSubqueryCompareCriteria(
        Expression leftExpression,
        Comparison.Operator operator,
        SubqueryComparison.Quantifier quantifier,
        Select subquery) {
        return new SubqueryComparison(leftExpression, operator, quantifier, subquery);
    }

    public SubqueryIn createSubqueryInCriteria(Expression expression, Select subquery, boolean isNegated) {
        return new SubqueryIn(expression, isNegated, subquery);
    }

    public Update createUpdate(NamedTable group, List<SetClause> updates, Condition criteria) {
        return new Update(group, updates, criteria);
    }

    public DerivedTable createInlineView(QueryExpression query, String name) {
        return new DerivedTable(query, name);
    }

    public SetQuery createSetOp(SetQuery.Operation operation, boolean all, QueryExpression leftQuery, QueryExpression rightQuery, OrderBy orderBy, Limit limit) {
        SetQuery queryImpl = new SetQuery();
        queryImpl.setOperation(operation);
        queryImpl.setAll(all);
        queryImpl.setLeftQuery(leftQuery);
        queryImpl.setRightQuery(rightQuery);
        queryImpl.setOrderBy(orderBy);
        queryImpl.setLimit(limit);
        return queryImpl;
    }

    public SetClause createSetClause(ColumnReference symbol, Expression value) {
        return new SetClause(symbol, value);
    }

}
