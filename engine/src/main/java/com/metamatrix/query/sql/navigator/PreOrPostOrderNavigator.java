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

package com.metamatrix.query.sql.navigator;

import java.util.Collection;
import java.util.Iterator;

import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.lang.BatchedUpdateCommand;
import com.metamatrix.query.sql.lang.BetweenCriteria;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.CompoundCriteria;
import com.metamatrix.query.sql.lang.Create;
import com.metamatrix.query.sql.lang.Delete;
import com.metamatrix.query.sql.lang.DependentSetCriteria;
import com.metamatrix.query.sql.lang.Drop;
import com.metamatrix.query.sql.lang.DynamicCommand;
import com.metamatrix.query.sql.lang.ExistsCriteria;
import com.metamatrix.query.sql.lang.From;
import com.metamatrix.query.sql.lang.GroupBy;
import com.metamatrix.query.sql.lang.Insert;
import com.metamatrix.query.sql.lang.Into;
import com.metamatrix.query.sql.lang.IsNullCriteria;
import com.metamatrix.query.sql.lang.JoinPredicate;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.lang.Limit;
import com.metamatrix.query.sql.lang.MatchCriteria;
import com.metamatrix.query.sql.lang.NotCriteria;
import com.metamatrix.query.sql.lang.Option;
import com.metamatrix.query.sql.lang.OrderBy;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.SPParameter;
import com.metamatrix.query.sql.lang.Select;
import com.metamatrix.query.sql.lang.SetClause;
import com.metamatrix.query.sql.lang.SetClauseList;
import com.metamatrix.query.sql.lang.SetCriteria;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.lang.SubqueryCompareCriteria;
import com.metamatrix.query.sql.lang.SubqueryFromClause;
import com.metamatrix.query.sql.lang.SubquerySetCriteria;
import com.metamatrix.query.sql.lang.UnaryFromClause;
import com.metamatrix.query.sql.lang.Update;
import com.metamatrix.query.sql.lang.XQuery;
import com.metamatrix.query.sql.proc.AssignmentStatement;
import com.metamatrix.query.sql.proc.Block;
import com.metamatrix.query.sql.proc.BreakStatement;
import com.metamatrix.query.sql.proc.CommandStatement;
import com.metamatrix.query.sql.proc.ContinueStatement;
import com.metamatrix.query.sql.proc.CreateUpdateProcedureCommand;
import com.metamatrix.query.sql.proc.CriteriaSelector;
import com.metamatrix.query.sql.proc.DeclareStatement;
import com.metamatrix.query.sql.proc.HasCriteria;
import com.metamatrix.query.sql.proc.IfStatement;
import com.metamatrix.query.sql.proc.LoopStatement;
import com.metamatrix.query.sql.proc.RaiseErrorStatement;
import com.metamatrix.query.sql.proc.TranslateCriteria;
import com.metamatrix.query.sql.proc.WhileStatement;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.AliasSymbol;
import com.metamatrix.query.sql.symbol.AllInGroupSymbol;
import com.metamatrix.query.sql.symbol.AllSymbol;
import com.metamatrix.query.sql.symbol.CaseExpression;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.ExpressionSymbol;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.symbol.ScalarSubquery;
import com.metamatrix.query.sql.symbol.SearchedCaseExpression;


/** 
 * @since 4.2
 */
public class PreOrPostOrderNavigator extends AbstractNavigator {

    public static final boolean PRE_ORDER = true;
    public static final boolean POST_ORDER = false;
    
    private boolean order;
    
    public PreOrPostOrderNavigator(LanguageVisitor visitor, boolean order) {
        super(visitor);
        this.order = order;
    }

    protected void preVisitVisitor(LanguageObject obj) {
        if(order == PRE_ORDER) {
            visitVisitor(obj);
        }
    }

    protected void postVisitVisitor(LanguageObject obj) {
        if(order == POST_ORDER) {
            visitVisitor(obj);
        }
    }

    
    public void visit(AggregateSymbol obj) {
        preVisitVisitor(obj);
        visitNode(obj.getExpression());
        postVisitVisitor(obj);
    }
    public void visit(AliasSymbol obj) {
        preVisitVisitor(obj);
        visitNode(obj.getSymbol());
        postVisitVisitor(obj);
    }
    public void visit(AllInGroupSymbol obj) {
        preVisitVisitor(obj);
        postVisitVisitor(obj);
    }
    public void visit(AllSymbol obj) {
        preVisitVisitor(obj);
        postVisitVisitor(obj);
    }
    public void visit(AssignmentStatement obj) {
        preVisitVisitor(obj);
        visitNode(obj.getVariable());
        visitNode(obj.getValue());
        postVisitVisitor(obj);
    }
    public void visit(BatchedUpdateCommand obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getUpdateCommands());
        postVisitVisitor(obj);
    }
    public void visit(BetweenCriteria obj) {
        preVisitVisitor(obj);
        visitNode(obj.getExpression());
        visitNode(obj.getLowerExpression());
        visitNode(obj.getUpperExpression());
        postVisitVisitor(obj);
    }
    public void visit(Block obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getStatements());
        postVisitVisitor(obj);
    }
    public void visit(BreakStatement obj) {
        preVisitVisitor(obj);
        postVisitVisitor(obj);
    }
    public void visit(CaseExpression obj) {
        preVisitVisitor(obj);
        visitNode(obj.getExpression());
        for(int i=0; i<obj.getWhenCount(); i++) {
            visitNode(obj.getWhenExpression(i));
            visitNode(obj.getThenExpression(i));
        }
        visitNode(obj.getElseExpression());
        postVisitVisitor(obj);
    }
    public void visit(CommandStatement obj) {
        preVisitVisitor(obj);
        visitNode(obj.getCommand());
        postVisitVisitor(obj);
    }
    public void visit(CompareCriteria obj) {
        preVisitVisitor(obj);
        visitNode(obj.getLeftExpression());
        visitNode(obj.getRightExpression());
        postVisitVisitor(obj);
    }
    public void visit(CompoundCriteria obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getCriteria());
        postVisitVisitor(obj);
    }
    public void visit(Constant obj) {
        preVisitVisitor(obj);
        postVisitVisitor(obj);
    }
    public void visit(ContinueStatement obj) {
        preVisitVisitor(obj);
        postVisitVisitor(obj);
    }
    public void visit(CreateUpdateProcedureCommand obj) {
        preVisitVisitor(obj);
        visitNode(obj.getBlock());
        postVisitVisitor(obj);
    }
    public void visit(CriteriaSelector obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getElements());
        postVisitVisitor(obj);
    }
    public void visit(DeclareStatement obj) {
        preVisitVisitor(obj);
        visitNode(obj.getVariable());
        visitNode(obj.getValue());
        postVisitVisitor(obj);
    }
    public void visit(Delete obj) {
        preVisitVisitor(obj);
        visitNode(obj.getGroup());
        visitNode(obj.getCriteria());
        visitNode(obj.getOption());
        postVisitVisitor(obj);
    }
    public void visit(DependentSetCriteria obj) {
        preVisitVisitor(obj);
        visitNode(obj.getExpression());
        postVisitVisitor(obj);
    }
    public void visit(ElementSymbol obj) {
        preVisitVisitor(obj);
        postVisitVisitor(obj);
    }
    public void visit(ExistsCriteria obj) {
        preVisitVisitor(obj);
        postVisitVisitor(obj);        
    }
    public void visit(ExpressionSymbol obj) {
        preVisitVisitor(obj);
        visitNode(obj.getExpression());
        postVisitVisitor(obj);
    }
    public void visit(From obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getClauses());
        postVisitVisitor(obj);
    }
    public void visit(Function obj) {
        preVisitVisitor(obj);
        Expression[] args = obj.getArgs();
        if(args != null) {
            for(int i=0; i<args.length; i++) {
                visitNode(args[i]);
            }
        }
        postVisitVisitor(obj);
    }
    public void visit(GroupBy obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getSymbols());
        postVisitVisitor(obj);
    }
    public void visit(GroupSymbol obj) {
        preVisitVisitor(obj);
        postVisitVisitor(obj);
    }
    public void visit(HasCriteria obj) {
        preVisitVisitor(obj);
        visitNode(obj.getSelector());
        postVisitVisitor(obj);
    }
    public void visit(IfStatement obj) {
        preVisitVisitor(obj);
        visitNode(obj.getCondition());
        visitNode(obj.getIfBlock());
        visitNode(obj.getElseBlock());
        postVisitVisitor(obj);
    }
    public void visit(Insert obj) {
        preVisitVisitor(obj);
        visitNode(obj.getGroup());
        visitNodes(obj.getVariables());
        visitNodes(obj.getValues());
        if(obj.getQueryExpression()!=null) {
        	visitNode(obj.getQueryExpression());
        }
        visitNode(obj.getOption());
        postVisitVisitor(obj);
    }
    public void visit(Create obj) {
        preVisitVisitor(obj);
        visitNode(obj.getTable());
        visitNodes(obj.getColumns());
        postVisitVisitor(obj);
    }
    public void visit(Drop obj) {
        preVisitVisitor(obj);
        visitNode(obj.getTable());
        postVisitVisitor(obj);
    }
    public void visit(Into obj) {
        preVisitVisitor(obj);
        visitNode(obj.getGroup());
        postVisitVisitor(obj);
    }
    public void visit(IsNullCriteria obj) {
        preVisitVisitor(obj);
        visitNode(obj.getExpression());
        postVisitVisitor(obj);
    }
    public void visit(JoinPredicate obj) {
        preVisitVisitor(obj);
        visitNode(obj.getLeftClause());
        visitNode(obj.getJoinType());
        visitNode(obj.getRightClause());
        visitNodes(obj.getJoinCriteria());
        postVisitVisitor(obj);
    }
    public void visit(JoinType obj) {
        preVisitVisitor(obj);
        postVisitVisitor(obj);
    }
    public void visit(Limit obj) {
        preVisitVisitor(obj);
        visitNode(obj.getOffset());
        visitNode(obj.getRowLimit());
        postVisitVisitor(obj);
    }
    public void visit(LoopStatement obj) {
        preVisitVisitor(obj);
        visitNode(obj.getCommand());
        visitNode(obj.getBlock());
        postVisitVisitor(obj);
    }
    public void visit(MatchCriteria obj) {
        preVisitVisitor(obj);
        visitNode(obj.getLeftExpression());
        visitNode(obj.getRightExpression());
        postVisitVisitor(obj);
    }
    public void visit(NotCriteria obj) {
        preVisitVisitor(obj);
        visitNode(obj.getCriteria());
        postVisitVisitor(obj);
    }
    public void visit(Option obj) {
        preVisitVisitor(obj);
        postVisitVisitor(obj);
    }
    public void visit(OrderBy obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getVariables());
        postVisitVisitor(obj);
    }
    public void visit(Query obj) {
        preVisitVisitor(obj);
        visitNode(obj.getSelect());
        visitNode(obj.getInto());
        visitNode(obj.getFrom());
        visitNode(obj.getCriteria());
        visitNode(obj.getGroupBy());
        visitNode(obj.getHaving());
        visitNode(obj.getOrderBy());
        visitNode(obj.getLimit());
        visitNode(obj.getOption());
        postVisitVisitor(obj);
    }
    public void visit(RaiseErrorStatement obj) {
        preVisitVisitor(obj);
        visitNode(obj.getExpression());
        postVisitVisitor(obj);
    }
    public void visit(Reference obj) {
        preVisitVisitor(obj);
        postVisitVisitor(obj);
    }
    public void visit(ScalarSubquery obj) {
        preVisitVisitor(obj);
        postVisitVisitor(obj);
    }
    public void visit(SearchedCaseExpression obj) {
        preVisitVisitor(obj);
        for(int i=0; i<obj.getWhenCount(); i++) {
            visitNode(obj.getWhenCriteria(i));
            visitNode(obj.getThenExpression(i));
        }
        visitNode(obj.getElseExpression());
        postVisitVisitor(obj);
    }
    public void visit(Select obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getSymbols());
        postVisitVisitor(obj);
    }
    public void visit(SetCriteria obj) {
        preVisitVisitor(obj);
        visitNode(obj.getExpression());
        visitNodes(obj.getValues());
        postVisitVisitor(obj);
    }
    public void visit(SetQuery obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getQueryCommands());
        visitNode(obj.getOrderBy());
        visitNode(obj.getLimit());
        visitNode(obj.getOption());
        postVisitVisitor(obj);
    }

    public void visit(StoredProcedure obj) {
        preVisitVisitor(obj);
        
        Collection params = obj.getParameters();
        if(params != null && !params.isEmpty()) {
            for(final Iterator iter = params.iterator(); iter.hasNext();) {
                SPParameter parameter = (SPParameter) iter.next();
                Expression expression = parameter.getExpression();
                visitNode(expression);
            }
        }
        
        visitNode(obj.getOption());
        postVisitVisitor(obj);
    }
    public void visit(SubqueryCompareCriteria obj) {
        preVisitVisitor(obj);
        visitNode(obj.getLeftExpression());
        postVisitVisitor(obj);
    }
    public void visit(SubqueryFromClause obj) {
        preVisitVisitor(obj);
        visitNode(obj.getGroupSymbol());
        postVisitVisitor(obj);
    }
    public void visit(SubquerySetCriteria obj) {
        preVisitVisitor(obj);
        visitNode(obj.getExpression());
        postVisitVisitor(obj);
    }
    public void visit(TranslateCriteria obj) {
        preVisitVisitor(obj);
        visitNode(obj.getSelector());
        visitNodes(obj.getTranslations());
        postVisitVisitor(obj);
    }
    public void visit(UnaryFromClause obj) {
        preVisitVisitor(obj);
        visitNode(obj.getGroup());
        postVisitVisitor(obj);
    }
    public void visit(Update obj) {
        preVisitVisitor(obj);
        visitNode(obj.getGroup());
        visitNode(obj.getChangeList());
        visitNode(obj.getCriteria());
        visitNode(obj.getOption());
        postVisitVisitor(obj);
    }
    public void visit(WhileStatement obj) {
        preVisitVisitor(obj);
        visitNode(obj.getCondition());
        visitNode(obj.getBlock());
        postVisitVisitor(obj);
    }
    public void visit(XQuery obj) {
        preVisitVisitor(obj);
        visitNode(obj.getOption());
        postVisitVisitor(obj);
    }
    
    /**
     * NOTE: we specifically don't need to visit the as columns or the using identifiers.
     * These will be resolved by the dynamic command resolver instead.
     * 
     * @see com.metamatrix.query.sql.LanguageVisitor#visit(com.metamatrix.query.sql.lang.DynamicCommand)
     */
    public void visit(DynamicCommand obj) {
        preVisitVisitor(obj);
        visitNode(obj.getSql());
        visitNode(obj.getIntoGroup());
        if (obj.getUsing() != null) {
            for (SetClause setClause : obj.getUsing().getClauses()) {
				visitNode(setClause.getValue());
			}
        }
        postVisitVisitor(obj);
    }
    
    public void visit(SetClauseList obj) {
    	preVisitVisitor(obj);
    	visitNodes(obj.getClauses());
        postVisitVisitor(obj);
    }
    
    public void visit(SetClause obj) {
    	preVisitVisitor(obj);
    	visitNode(obj.getSymbol());
    	visitNode(obj.getValue());
        postVisitVisitor(obj);
    }
    
    public static void doVisit(LanguageObject object, LanguageVisitor visitor, boolean order) {
        PreOrPostOrderNavigator nav = new PreOrPostOrderNavigator(visitor, order);
        object.acceptVisitor(nav);
    }

}
