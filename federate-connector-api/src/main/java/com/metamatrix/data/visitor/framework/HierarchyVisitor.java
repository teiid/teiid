/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.data.visitor.framework;

import com.metamatrix.data.language.IAggregate;
import com.metamatrix.data.language.IBatchedUpdates;
import com.metamatrix.data.language.ICaseExpression;
import com.metamatrix.data.language.ICompareCriteria;
import com.metamatrix.data.language.ICompoundCriteria;
import com.metamatrix.data.language.IDelete;
import com.metamatrix.data.language.IElement;
import com.metamatrix.data.language.IExistsCriteria;
import com.metamatrix.data.language.IFrom;
import com.metamatrix.data.language.IFunction;
import com.metamatrix.data.language.IGroup;
import com.metamatrix.data.language.IGroupBy;
import com.metamatrix.data.language.IInCriteria;
import com.metamatrix.data.language.IInlineView;
import com.metamatrix.data.language.IInsert;
import com.metamatrix.data.language.IIsNullCriteria;
import com.metamatrix.data.language.IJoin;
import com.metamatrix.data.language.ILikeCriteria;
import com.metamatrix.data.language.ILiteral;
import com.metamatrix.data.language.INotCriteria;
import com.metamatrix.data.language.IOrderBy;
import com.metamatrix.data.language.IOrderByItem;
import com.metamatrix.data.language.IParameter;
import com.metamatrix.data.language.IProcedure;
import com.metamatrix.data.language.IQuery;
import com.metamatrix.data.language.IScalarSubquery;
import com.metamatrix.data.language.ISearchedCaseExpression;
import com.metamatrix.data.language.ISelect;
import com.metamatrix.data.language.ISelectSymbol;
import com.metamatrix.data.language.ISetClause;
import com.metamatrix.data.language.ISetClauseList;
import com.metamatrix.data.language.ISetQuery;
import com.metamatrix.data.language.ISubqueryCompareCriteria;
import com.metamatrix.data.language.ISubqueryInCriteria;
import com.metamatrix.data.language.IUpdate;

/**
 * Visits each node in  a hierarchy of ILanguageObjects. The default
 * implementation of each visit() method is simply to visit the children of a
 * given ILanguageObject, if any exist, with this HierarchyVisitor (without
 * performing any actions on the node). A subclass can selectively override
 * visit() methods to delegate the actions performed on a node to another
 * visitor by calling that Visitor's visit() method. This implementation makes
 * no guarantees about the order in which the children of an ILanguageObject are
 * visited.
 * @see DelegatingHierarchyVisitor
 */
public abstract class HierarchyVisitor extends AbstractLanguageVisitor {

    public HierarchyVisitor() {
    }
    
    public void visit(IAggregate obj) {
        visitNode(obj.getExpression());
    }
    
    public void visit(IBatchedUpdates obj) {
        visitNodes(obj.getUpdateCommands());
    }
    
    public void visit(ICaseExpression obj) {
        visitNode(obj.getExpression());
        int whenCount = obj.getWhenCount();
        for (int i = 0; i < whenCount; i++) {
            visitNode(obj.getWhenExpression(i));
            visitNode(obj.getThenExpression(i));
        }
        visitNode(obj.getElseExpression());
    }
    
    public void visit(ICompareCriteria obj) {
        visitNode(obj.getLeftExpression());
        visitNode(obj.getRightExpression());
    }
    
    public void visit(ICompoundCriteria obj) {
        visitNodes(obj.getCriteria());
    }
    
    public void visit(IDelete obj) {
        visitNode(obj.getGroup());
        visitNode(obj.getCriteria());
    }
    
    public void visit(IElement obj) {
    }
    
    public void visit(IProcedure obj) {
        visitNodes(obj.getParameters());
    }
    
    public void visit(IExistsCriteria obj) {
        visitNode(obj.getQuery());
    }
    
    public void visit(IFrom obj) {
        visitNodes(obj.getItems());
    }
    
    public void visit(IFunction obj) {
        visitNodes(obj.getParameters());
    }

    public void visit(IGroup obj) {
    }
    
//    public void visit(IGroup obj) {
//    }
    
    public void visit(IGroupBy obj) {
        visitNodes(obj.getElements());
    }
    
    public void visit(IInCriteria obj) {
        visitNode(obj.getLeftExpression());
        visitNodes(obj.getRightExpressions());
    }
    
    public void visit(IInsert obj) {
        visitNode(obj.getGroup());
        visitNodes(obj.getElements());
        visitNodes(obj.getValues());
    }
    
    public void visit(IIsNullCriteria obj) {
        visitNode(obj.getExpression());
    }
    
    public void visit(IJoin obj) {
        visitNode(obj.getLeftItem());
        visitNode(obj.getRightItem());
        if(obj.getCriteria() != null) {
            visitNodes(obj.getCriteria());
        }
    }
    
    public void visit(ILikeCriteria obj) {
        visitNode(obj.getLeftExpression());
        visitNode(obj.getRightExpression());
    }

    public void visit(ILiteral obj) {
    }
        
    public void visit(INotCriteria obj) {
        visitNode(obj.getCriteria());
    }
    
    public void visit(IOrderBy obj) {
        visitNodes(obj.getItems());
    }

    public void visit(IOrderByItem obj) {
    }

    public void visit(IParameter obj) {
    }
        
    public void visit(IQuery obj) {
        visitNode(obj.getSelect());
        visitNode(obj.getFrom());
        visitNode(obj.getWhere());
        visitNode(obj.getGroupBy());
        visitNode(obj.getHaving());
        visitNode(obj.getOrderBy());
        visitNode(obj.getLimit());
    }

    public void visit(IScalarSubquery obj) {
        visitNode(obj.getQuery());
    }
    
    public void visit(ISearchedCaseExpression obj) {
        int whenCount = obj.getWhenCount();
        for (int i = 0; i < whenCount; i++) {
            visitNode(obj.getWhenCriteria(i));
            visitNode(obj.getThenExpression(i));
        }
        visitNode(obj.getElseExpression());
    }
    
    public void visit(ISelect obj) {
        visitNodes(obj.getSelectSymbols());
    }
    
    public void visit(ISelectSymbol obj) {
        visitNode(obj.getExpression());
    }

    public void visit(ISubqueryCompareCriteria obj) {
        visitNode(obj.getLeftExpression());
        visitNode(obj.getQuery());
    }

    public void visit(ISubqueryInCriteria obj) {
        visitNode(obj.getLeftExpression());        
        visitNode(obj.getQuery());
    }
    
    public void visit(ISetQuery obj) {
        visitNode(obj.getLeftQuery());
        visitNode(obj.getRightQuery());        
        visitNode(obj.getOrderBy());
        visitNode(obj.getLimit());
    }
    
    public void visit(IUpdate obj) {
        visitNode(obj.getGroup());
        visitNode(obj.getChanges());
        visitNode(obj.getCriteria());
    }
    
    @Override
    public void visit(IInlineView obj) {
    	visitNode(obj.getQuery());
    }
    
    @Override
    public void visit(ISetClauseList obj) {
    	visitNodes(obj.getClauses());
    }
    
    @Override
    public void visit(ISetClause obj) {
    	visitNode(obj.getSymbol());
    	visitNode(obj.getValue());
    }

    public void reset() {

    }

}
