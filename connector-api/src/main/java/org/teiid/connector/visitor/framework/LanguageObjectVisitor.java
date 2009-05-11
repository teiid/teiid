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

import org.teiid.connector.language.*;

/**
 */
public interface LanguageObjectVisitor {
    public void visit(IAggregate obj);
    public void visit(IBatchedUpdates obj);
    public void visit(IInsertExpressionValueSource obj);
    public void visit(ICompareCriteria obj);
    public void visit(ICompoundCriteria obj);
    public void visit(IDelete obj);
    public void visit(IElement obj);
    public void visit(IProcedure obj);
    public void visit(IExistsCriteria obj);
    public void visit(IFrom obj);
    public void visit(IFunction obj);
    public void visit(IGroup obj);
    public void visit(IGroupBy obj);
    public void visit(IInCriteria obj);
    public void visit(IInlineView obj);
    public void visit(IInsert obj);    
    public void visit(IIsNullCriteria obj);
    public void visit(IJoin obj);
    public void visit(ILikeCriteria obj);
    public void visit(ILimit obj);
    public void visit(ILiteral obj);
    public void visit(INotCriteria obj);
    public void visit(IOrderBy obj);
    public void visit(IOrderByItem obj);
    public void visit(IParameter obj);
    public void visit(IQuery obj);
    public void visit(IScalarSubquery obj);
    public void visit(ISearchedCaseExpression obj);
    public void visit(ISelect obj);
    public void visit(ISelectSymbol obj);
    public void visit(ISubqueryCompareCriteria obj);
    public void visit(ISubqueryInCriteria obj);
    public void visit(IUpdate obj);
    public void visit(ISetQuery obj);
    public void visit(ISetClauseList obj);
    public void visit(ISetClause obj);
}
