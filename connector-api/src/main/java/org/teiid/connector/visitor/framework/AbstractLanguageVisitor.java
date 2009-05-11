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

package org.teiid.connector.visitor.framework;

import java.util.Collection;

import org.teiid.connector.language.IAggregate;
import org.teiid.connector.language.IBatchedUpdates;
import org.teiid.connector.language.ICompareCriteria;
import org.teiid.connector.language.ICompoundCriteria;
import org.teiid.connector.language.IDelete;
import org.teiid.connector.language.IElement;
import org.teiid.connector.language.IExistsCriteria;
import org.teiid.connector.language.IFrom;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.IGroup;
import org.teiid.connector.language.IGroupBy;
import org.teiid.connector.language.IInCriteria;
import org.teiid.connector.language.IInlineView;
import org.teiid.connector.language.IInsert;
import org.teiid.connector.language.IInsertExpressionValueSource;
import org.teiid.connector.language.IIsNullCriteria;
import org.teiid.connector.language.IJoin;
import org.teiid.connector.language.ILanguageObject;
import org.teiid.connector.language.ILikeCriteria;
import org.teiid.connector.language.ILimit;
import org.teiid.connector.language.ILiteral;
import org.teiid.connector.language.INotCriteria;
import org.teiid.connector.language.IOrderBy;
import org.teiid.connector.language.IOrderByItem;
import org.teiid.connector.language.IParameter;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.language.IScalarSubquery;
import org.teiid.connector.language.ISearchedCaseExpression;
import org.teiid.connector.language.ISelect;
import org.teiid.connector.language.ISelectSymbol;
import org.teiid.connector.language.ISetClause;
import org.teiid.connector.language.ISetClauseList;
import org.teiid.connector.language.ISetQuery;
import org.teiid.connector.language.ISubqueryCompareCriteria;
import org.teiid.connector.language.ISubqueryInCriteria;
import org.teiid.connector.language.IUpdate;


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
    public void visitNode(ILanguageObject obj) {
        if (obj != null) {
            obj.acceptVisitor(this);
        }
    }
    
    /**
     * Visits a Collection of ILanguageObjects in iteration order. This method
     * can be used by subclasses to visit each ILanguageObject in the Collection
     * @param nodes a Collection of ILanguageObjects
     */
    public void visitNodes(Collection<? extends ILanguageObject> nodes) {
        if (nodes != null && nodes.size() > 0) {
            for (ILanguageObject node : nodes) {
                visitNode(node);
            }
        }
    }
    
    /**
     * Visits an array of ILanguageObjects in order. This method can be used by
     * subclasses to visit each ILanguageObject in the array.
     * @param nodes an ILanguageObject[]
     */
    public void visitNodes(ILanguageObject[] nodes) {
        if (nodes != null && nodes.length > 0) {
            for (int i = 0; i < nodes.length; i++) {
                visitNode(nodes[i]);
            }
        }
    }
    
    public void visit(IAggregate obj) {}
    public void visit(IBatchedUpdates obj) {}
    public void visit(ICompareCriteria obj) {}
    public void visit(ICompoundCriteria obj) {}
    public void visit(IDelete obj) {}
    public void visit(IElement obj) {}
    public void visit(IProcedure obj) {}
    public void visit(IExistsCriteria obj) {}
    public void visit(IFrom obj) {}
    public void visit(IFunction obj) {}
    public void visit(IGroup obj) {}
    public void visit(IGroupBy obj) {}
    public void visit(IInCriteria obj) {}
    public void visit(IInlineView obj) {}
    public void visit(IInsert obj) {}
    public void visit(IInsertExpressionValueSource obj) {}
    public void visit(IIsNullCriteria obj) {}
    public void visit(IJoin obj) {}
    public void visit(ILikeCriteria obj) {}
    public void visit(ILimit obj) {}
    public void visit(ILiteral obj) {}
    public void visit(INotCriteria obj) {}
    public void visit(IOrderBy obj) {}
    public void visit(IOrderByItem obj) {}
    public void visit(IParameter obj) {}
    public void visit(IQuery obj) {}
    public void visit(IScalarSubquery obj) {}
    public void visit(ISearchedCaseExpression obj) {}
    public void visit(ISelect obj) {}
    public void visit(ISelectSymbol obj) {}
    public void visit(ISubqueryCompareCriteria obj) {}
    public void visit(ISubqueryInCriteria obj) {}
    public void visit(IUpdate obj) {}
    public void visit(ISetQuery obj) {}
    public void visit(ISetClauseList obj) {}
    public void visit(ISetClause obj) {}
}
