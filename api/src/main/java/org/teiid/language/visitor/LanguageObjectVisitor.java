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

import org.teiid.language.AggregateFunction;
import org.teiid.language.AndOr;
import org.teiid.language.Argument;
import org.teiid.language.BatchedUpdates;
import org.teiid.language.Call;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Delete;
import org.teiid.language.DerivedColumn;
import org.teiid.language.DerivedTable;
import org.teiid.language.Exists;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Function;
import org.teiid.language.GroupBy;
import org.teiid.language.In;
import org.teiid.language.Insert;
import org.teiid.language.IsNull;
import org.teiid.language.IteratorValueSource;
import org.teiid.language.Join;
import org.teiid.language.Like;
import org.teiid.language.Limit;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.Not;
import org.teiid.language.OrderBy;
import org.teiid.language.ScalarSubquery;
import org.teiid.language.SearchedCase;
import org.teiid.language.SearchedWhenClause;
import org.teiid.language.Select;
import org.teiid.language.SetClause;
import org.teiid.language.SetQuery;
import org.teiid.language.SortSpecification;
import org.teiid.language.SubqueryComparison;
import org.teiid.language.SubqueryIn;
import org.teiid.language.Update;
import org.teiid.language.WindowFunction;
import org.teiid.language.With;
import org.teiid.language.WithItem;

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
	public void visit(IteratorValueSource obj);
	public void visit(With obj);
	public void visit(WithItem obj);
	public void visit(WindowFunction windowFunction);
}
