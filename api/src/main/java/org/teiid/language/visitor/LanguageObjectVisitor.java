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

import org.teiid.language.*;

/**
 */
public interface LanguageObjectVisitor {
    public void visit(AggregateFunction obj);
    public void visit(BatchedUpdates obj);
    public void visit(ExpressionValueSource obj);
    public void visit(Comparison obj);
    public void visit(AndOr obj);
    public void visit(Delete obj);
    public void visit(ColumnReference obj);
    public void visit(Call obj);
    public void visit(Exists obj);
    public void visit(Function obj);
    public void visit(NamedTable obj);
    public void visit(GroupBy obj);
    public void visit(In obj);
    public void visit(DerivedTable obj);
    public void visit(Insert obj);    
    public void visit(IsNull obj);
    public void visit(Join obj);
    public void visit(Like obj);
    public void visit(Limit obj);
    public void visit(Literal obj);
    public void visit(Not obj);
    public void visit(OrderBy obj);
    public void visit(SortSpecification obj);
    public void visit(Argument obj);
    public void visit(Select obj);
    public void visit(ScalarSubquery obj);
    public void visit(SearchedCase obj);
    public void visit(DerivedColumn obj);
    public void visit(SubqueryComparison obj);
    public void visit(SubqueryIn obj);
    public void visit(Update obj);
    public void visit(SetQuery obj);
    public void visit(SetClause obj);
    public void visit(SearchedWhenClause obj);
	public void visit(With obj);
	public void visit(WithItem obj);
	public void visit(WindowFunction windowFunction);
	public void visit(WindowSpecification windowSpecification);
	public void visit(Parameter obj);
	public void visit(Array array);
}
