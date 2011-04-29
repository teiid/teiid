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

package org.teiid.query.sql;

import org.teiid.query.sql.lang.AlterProcedure;
import org.teiid.query.sql.lang.AlterTrigger;
import org.teiid.query.sql.lang.AlterView;
import org.teiid.query.sql.lang.ArrayTable;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.BetweenCriteria;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Create;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.DependentSetCriteria;
import org.teiid.query.sql.lang.Drop;
import org.teiid.query.sql.lang.DynamicCommand;
import org.teiid.query.sql.lang.ExistsCriteria;
import org.teiid.query.sql.lang.ExpressionCriteria;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.GroupBy;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.Into;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.JoinPredicate;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.Limit;
import org.teiid.query.sql.lang.MatchCriteria;
import org.teiid.query.sql.lang.NotCriteria;
import org.teiid.query.sql.lang.Option;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.lang.ProcedureContainer;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SetClause;
import org.teiid.query.sql.lang.SetClauseList;
import org.teiid.query.sql.lang.SetCriteria;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.SubqueryCompareCriteria;
import org.teiid.query.sql.lang.SubqueryFromClause;
import org.teiid.query.sql.lang.SubquerySetCriteria;
import org.teiid.query.sql.lang.TextTable;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.lang.WithQueryCommand;
import org.teiid.query.sql.lang.XMLTable;
import org.teiid.query.sql.proc.AssignmentStatement;
import org.teiid.query.sql.proc.Block;
import org.teiid.query.sql.proc.BreakStatement;
import org.teiid.query.sql.proc.CommandStatement;
import org.teiid.query.sql.proc.ContinueStatement;
import org.teiid.query.sql.proc.CreateUpdateProcedureCommand;
import org.teiid.query.sql.proc.CriteriaSelector;
import org.teiid.query.sql.proc.DeclareStatement;
import org.teiid.query.sql.proc.HasCriteria;
import org.teiid.query.sql.proc.IfStatement;
import org.teiid.query.sql.proc.LoopStatement;
import org.teiid.query.sql.proc.RaiseErrorStatement;
import org.teiid.query.sql.proc.TranslateCriteria;
import org.teiid.query.sql.proc.TriggerAction;
import org.teiid.query.sql.proc.WhileStatement;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.AllInGroupSymbol;
import org.teiid.query.sql.symbol.AllSymbol;
import org.teiid.query.sql.symbol.CaseExpression;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.DerivedColumn;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.QueryString;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.symbol.SearchedCaseExpression;
import org.teiid.query.sql.symbol.TextLine;
import org.teiid.query.sql.symbol.XMLAttributes;
import org.teiid.query.sql.symbol.XMLElement;
import org.teiid.query.sql.symbol.XMLForest;
import org.teiid.query.sql.symbol.XMLNamespaces;
import org.teiid.query.sql.symbol.XMLParse;
import org.teiid.query.sql.symbol.XMLQuery;
import org.teiid.query.sql.symbol.XMLSerialize;

/**
 * <p>The LanguageVisitor can be used to visit a LanguageObject as if it were a tree
 * and perform some action on some or all of the language objects that are visited.
 * The LanguageVisitor is extended to create a concrete visitor and some or all of 
 * the public visit methods should be overridden to provide the visitor functionality. 
 * These public visit methods SHOULD NOT be called directly.</p>
 */
@SuppressWarnings("unused")
public abstract class LanguageVisitor {
    
    private boolean abort = false;

    public void setAbort(boolean abort) {
        this.abort = abort;
    }
    
    public final boolean shouldAbort() {
        return abort;
    }

    // Visitor methods for language objects
    public void visit(BatchedUpdateCommand obj) {}
    public void visit(BetweenCriteria obj) {}
    public void visit(CaseExpression obj) {}
    public void visit(CompareCriteria obj) {}
    public void visit(CompoundCriteria obj) {}
    public void visit(Delete obj) {
        visit((ProcedureContainer)obj);
    }
    public void visit(ExistsCriteria obj) {}
    public void visit(From obj) {}
    public void visit(GroupBy obj) {}
    public void visit(Insert obj) {
        visit((ProcedureContainer)obj);
    }
    public void visit(IsNullCriteria obj) {}
    public void visit(JoinPredicate obj) {}
    public void visit(JoinType obj) {}
    public void visit(Limit obj) {}
    public void visit(MatchCriteria obj) {}
    public void visit(NotCriteria obj) {}
    public void visit(Option obj) {}
    public void visit(OrderBy obj) {}
    public void visit(Query obj) {}
    public void visit(SearchedCaseExpression obj) {}
    public void visit(Select obj) {}
    public void visit(SetCriteria obj) {}
    public void visit(SetQuery obj) {}
    public void visit(StoredProcedure obj) {
        visit((ProcedureContainer)obj);
    }
    public void visit(SubqueryCompareCriteria obj) {}
    public void visit(SubqueryFromClause obj) {}
    public void visit(SubquerySetCriteria obj) {}
    public void visit(UnaryFromClause obj) {}
    public void visit(Update obj) {
        visit((ProcedureContainer)obj);
    }
    public void visit(Into obj) {}
    public void visit(DependentSetCriteria obj) {}
    public void visit(Create obj) {}
    public void visit(Drop obj) {}

    // Visitor methods for symbol objects
    public void visit(AggregateSymbol obj) {}
    public void visit(AliasSymbol obj) {}
    public void visit(AllInGroupSymbol obj) {}
    public void visit(AllSymbol obj) {}
    public void visit(Constant obj) {}
    public void visit(ElementSymbol obj) {}
    public void visit(ExpressionSymbol obj) {}
    public void visit(Function obj) {}
    public void visit(GroupSymbol obj) {}
    public void visit(Reference obj) {}
    public void visit(ScalarSubquery obj) {}
    
    // Visitor methods for procedure language objects    
    public void visit(AssignmentStatement obj) {}
    public void visit(Block obj) {}
    public void visit(CommandStatement obj) {}
    public void visit(CreateUpdateProcedureCommand obj) {}
    public void visit(CriteriaSelector obj) {}
    public void visit(DeclareStatement obj) {
        visit((AssignmentStatement)obj);
    }    
    public void visit(HasCriteria obj) {}
    public void visit(IfStatement obj) {}
    public void visit(RaiseErrorStatement obj) {}
    public void visit(TranslateCriteria obj) {}
    public void visit(BreakStatement obj) {}
    public void visit(ContinueStatement obj) {}
    public void visit(WhileStatement obj) {}
    public void visit(LoopStatement obj) {}
    public void visit(DynamicCommand obj) {}
    public void visit(ProcedureContainer obj) {}
    public void visit(SetClauseList obj) {}
    public void visit(SetClause obj) {}
    public void visit(OrderByItem obj) {}
    public void visit(XMLElement obj) {}
    public void visit(XMLAttributes obj) {}
    public void visit(XMLForest obj) {}
    public void visit(XMLNamespaces obj) {}
    public void visit(TextTable obj) {}
    public void visit(TextLine obj) {}
    public void visit(XMLTable obj) {}
    public void visit(DerivedColumn obj) {}
    public void visit(XMLSerialize obj) {}
    public void visit(XMLQuery obj) {}
    public void visit(QueryString obj) {}
    public void visit(XMLParse obj) {}
    public void visit(ExpressionCriteria obj) {}
    public void visit(WithQueryCommand obj) {}
    public void visit(TriggerAction obj) {}
    public void visit(ArrayTable obj) {}

	public void visit(AlterView obj) {}
	public void visit(AlterProcedure obj) {}
    public void visit(AlterTrigger obj) {}
}
