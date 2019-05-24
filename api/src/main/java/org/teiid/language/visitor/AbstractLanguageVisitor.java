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

import java.util.Collection;

import org.teiid.language.*;


/**
 * Visitor that visits an instance of LanguageObject and performs an operation
 * on that instance. The visit() methods of this Visitor can be selectively
 * overridden to perform operations on each type of LanguageObject. The public
 * visit() methods should not be called directly, as they are only used by this
 * visitor framework to let the instance invoke the type-specific visit() method
 */
public abstract class AbstractLanguageVisitor implements LanguageObjectVisitor {

    protected AbstractLanguageVisitor() {
    }

    /**
     * Visit the LanguageObject instance to perform the Visitor's operation on
     * that instance. This method can also be used by the subclass to visit any
     * LanguageObject instances that the given instance may contain.
     * @see HierarchyVisitor
     * @param obj an LanguageObject instance
     */
    public void visitNode(LanguageObject obj) {
        if (obj != null) {
            obj.acceptVisitor(this);
        }
    }

    /**
     * Visits a Collection of LanguageObjects in iteration order. This method
     * can be used by subclasses to visit each LanguageObject in the Collection
     * @param nodes a Collection of LanguageObjects
     */
    public void visitNodes(Collection<? extends LanguageObject> nodes) {
        if (nodes != null && nodes.size() > 0) {
            for (LanguageObject node : nodes) {
                visitNode(node);
            }
        }
    }

    /**
     * Visits an array of LanguageObjects in order. This method can be used by
     * subclasses to visit each LanguageObject in the array.
     * @param nodes an LanguageObject[]
     */
    public void visitNodes(LanguageObject[] nodes) {
        if (nodes != null && nodes.length > 0) {
            for (int i = 0; i < nodes.length; i++) {
                visitNode(nodes[i]);
            }
        }
    }

    public void visit(AggregateFunction obj) {}
    public void visit(BatchedUpdates obj) {}
    public void visit(Comparison obj) {}
    public void visit(AndOr obj) {}
    public void visit(Delete obj) {}
    public void visit(ColumnReference obj) {}
    public void visit(Call obj) {}
    public void visit(Exists obj) {}
    public void visit(Function obj) {}
    public void visit(NamedTable obj) {}
    public void visit(GroupBy obj) {}
    public void visit(In obj) {}
    public void visit(DerivedTable obj) {}
    public void visit(Insert obj) {}
    public void visit(ExpressionValueSource obj) {}
    public void visit(IsNull obj) {}
    public void visit(Join obj) {}
    public void visit(Like obj) {}
    public void visit(Limit obj) {}
    public void visit(Literal obj) {}
    public void visit(Not obj) {}
    public void visit(OrderBy obj) {}
    public void visit(SortSpecification obj) {}
    public void visit(Argument obj) {}
    public void visit(Select obj) {}
    public void visit(ScalarSubquery obj) {}
    public void visit(SearchedCase obj) {}
    public void visit(DerivedColumn obj) {}
    public void visit(SubqueryComparison obj) {}
    public void visit(SubqueryIn obj) {}
    public void visit(Update obj) {}
    public void visit(SetQuery obj) {}
    public void visit(SetClause obj) {}
    public void visit(SearchedWhenClause obj) {}
    public void visit(Parameter obj) {}
    @Override
    public void visit(WindowFunction windowFunction) {}
    @Override
    public void visit(WindowSpecification windowSpecification) {}
    @Override
    public void visit(With obj) {}
    @Override
    public void visit(WithItem obj) {}
    @Override
    public void visit(Array array) {}
    @Override
    public void visit(NamedProcedureCall namedProcedureCall) {}
    @Override
    public void visit(IsDistinct isDistinct) {}
    @Override
    public void visit(WindowFrame windowFrame) {}

}
