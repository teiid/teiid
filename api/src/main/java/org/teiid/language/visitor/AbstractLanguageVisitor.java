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

package org.teiid.language.visitor;

import java.util.Collection;

import org.teiid.language.*;


/**
 * Visitor that visits an instance of ILanguageObject and performs an operation
 * on that instance. The visit() methods of this Visitor can be selectively
 * overridden to perform operations on each type of ILanguageObject. The public
 * visit() methods should not be called directly, as they are only used by this
 * visitor framework to let the instance invoke the type-specific visit() method
 */
public abstract class AbstractLanguageVisitor implements LanguageObjectVisitor {
    
    protected AbstractLanguageVisitor() {
    }
    
    /**
     * Visit the ILanguageObject instance to perform the Visitor's operation on
     * that instance. This method can also be used by the subclass to visit any
     * ILanguageObject instances that the given instance may contain.
     * @see HierarchyVisitor
     * @param obj an ILanguageObject instance
     */
    public void visitNode(LanguageObject obj) {
        if (obj != null) {
            obj.acceptVisitor(this);
        }
    }
    
    /**
     * Visits a Collection of ILanguageObjects in iteration order. This method
     * can be used by subclasses to visit each ILanguageObject in the Collection
     * @param nodes a Collection of ILanguageObjects
     */
    public void visitNodes(Collection<? extends LanguageObject> nodes) {
        if (nodes != null && nodes.size() > 0) {
            for (LanguageObject node : nodes) {
                visitNode(node);
            }
        }
    }
    
    /**
     * Visits an array of ILanguageObjects in order. This method can be used by
     * subclasses to visit each ILanguageObject in the array.
     * @param nodes an ILanguageObject[]
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
    public void visit(IteratorValueSource obj) {}
    @Override
    public void visit(WindowFunction windowFunction) {}
    @Override
    public void visit(WindowSpecification windowSpecification) {}
    @Override
    public void visit(With obj) {}
    @Override
    public void visit(WithItem obj) {}
}
