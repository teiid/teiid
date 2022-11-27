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

package org.teiid.language.visitor;

import org.teiid.language.*;

/**
 * Visits each node in  a hierarchy of LanguageObjects. The default
 * implementation of each visit() method is simply to visit the children of a
 * given LanguageObject, if any exist, with this HierarchyVisitor (without
 * performing any actions on the node). A subclass can selectively override
 * visit() methods to delegate the actions performed on a node to another
 * visitor by calling that Visitor's visit() method. This implementation makes
 * no guarantees about the order in which the children of an LanguageObject are
 * visited.
 * @see DelegatingHierarchyVisitor
 */
public abstract class HierarchyVisitor extends AbstractLanguageVisitor {

    private boolean visitSubcommands;

    public HierarchyVisitor() {
        this(true);
    }

    public HierarchyVisitor(boolean visitSubcommands) {
        this.visitSubcommands = visitSubcommands;
    }

    public void visit(AggregateFunction obj) {
        visitNodes(obj.getParameters());
        visitNode(obj.getCondition());
        visitNode(obj.getOrderBy());
    }

    public void visit(BatchedUpdates obj) {
        visitNodes(obj.getUpdateCommands());
    }

    public void visit(Comparison obj) {
        visitNode(obj.getLeftExpression());
        visitNode(obj.getRightExpression());
    }

    public void visit(AndOr obj) {
        visitNode(obj.getLeftCondition());
        visitNode(obj.getRightCondition());
    }

    public void visit(Delete obj) {
        visitNode(obj.getTable());
        visitNode(obj.getWhere());
    }

    public void visit(Call obj) {
        visitNodes(obj.getArguments());
    }

    public void visit(Exists obj) {
        if (visitSubcommands) {
            visitNode(obj.getSubquery());
        }
    }

    public void visit(Function obj) {
        visitNodes(obj.getParameters());
    }

    public void visit(GroupBy obj) {
        visitNodes(obj.getElements());
    }

    public void visit(In obj) {
        visitNode(obj.getLeftExpression());
        visitNodes(obj.getRightExpressions());
    }

    public void visit(Insert obj) {
        visitNode(obj.getTable());
        visitNodes(obj.getColumns());
        if (!(obj.getValueSource() instanceof QueryExpression) || visitSubcommands) {
            visitNode(obj.getValueSource());
        }
    }

    @Override
    public void visit(ExpressionValueSource obj) {
        visitNodes(obj.getValues());
    }

    public void visit(IsNull obj) {
        visitNode(obj.getExpression());
    }

    public void visit(Join obj) {
        visitNode(obj.getLeftItem());
        visitNode(obj.getRightItem());
        visitNode(obj.getCondition());
    }

    public void visit(Like obj) {
        visitNode(obj.getLeftExpression());
        visitNode(obj.getRightExpression());
    }

    public void visit(Not obj) {
        visitNode(obj.getCriteria());
    }

    public void visit(OrderBy obj) {
        visitNodes(obj.getSortSpecifications());
    }

    @Override
    public void visit(SortSpecification obj) {
        visitNode(obj.getExpression());
    }

    public void visit(Select obj) {
        visitNode(obj.getWith());
        visitNodes(obj.getDerivedColumns());
        visitNodes(obj.getFrom());
        visitNode(obj.getWhere());
        visitNode(obj.getGroupBy());
        visitNode(obj.getHaving());
        visitNode(obj.getOrderBy());
        visitNode(obj.getLimit());
    }

    public void visit(ScalarSubquery obj) {
        if (visitSubcommands) {
            visitNode(obj.getSubquery());
        }
    }

    public void visit(SearchedCase obj) {
        visitNodes(obj.getCases());
        visitNode(obj.getElseExpression());
    }

    @Override
    public void visit(SearchedWhenClause obj) {
        visitNode(obj.getCondition());
        visitNode(obj.getResult());
    }

    public void visit(DerivedColumn obj) {
        visitNode(obj.getExpression());
    }

    public void visit(SubqueryComparison obj) {
        visitNode(obj.getLeftExpression());
        if (visitSubcommands) {
            visitNode(obj.getSubquery());
        }
    }

    public void visit(SubqueryIn obj) {
        visitNode(obj.getLeftExpression());
        if (visitSubcommands) {
            visitNode(obj.getSubquery());
        }
    }

    public void visit(SetQuery obj) {
        visitNode(obj.getWith());
        if (visitSubcommands) {
            visitNode(obj.getLeftQuery());
            visitNode(obj.getRightQuery());
        }
        visitNode(obj.getOrderBy());
        visitNode(obj.getLimit());
    }

    public void visit(Update obj) {
        visitNode(obj.getTable());
        visitNodes(obj.getChanges());
        visitNode(obj.getWhere());
    }

    @Override
    public void visit(DerivedTable obj) {
        if (visitSubcommands) {
            visitNode(obj.getQuery());
        }
    }

    @Override
    public void visit(NamedProcedureCall namedProcedureCall) {
        if (visitSubcommands) {
            visitNode(namedProcedureCall.getCall());
        }
    }

    @Override
    public void visit(SetClause obj) {
        visitNode(obj.getSymbol());
        visitNode(obj.getValue());
    }

    @Override
    public void visit(With obj) {
        visitNodes(obj.getItems());
    }

    @Override
    public void visit(WithItem obj) {
        visitNode(obj.getTable());
        visitNodes(obj.getColumns());
        if (visitSubcommands) {
            visitNode(obj.getSubquery());
        }
    }

    @Override
    public void visit(WindowFunction windowFunction) {
        visitNode(windowFunction.getFunction());
        visitNode(windowFunction.getWindowSpecification());
    }

    @Override
    public void visit(WindowSpecification windowSpecification) {
        visitNodes(windowSpecification.getPartition());
        visitNode(windowSpecification.getOrderBy());
        visitNode(windowSpecification.getWindowFrame());
    }

    @Override
    public void visit(IsDistinct obj) {
        visitNode(obj.getLeftExpression());
        visitNode(obj.getRightExpression());
    }

    @Override
    public void visit(Array array) {
        visitNodes(array.getExpressions());
    }

}
