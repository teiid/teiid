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

package org.teiid.query.sql.navigator;

import java.util.Collection;

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
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
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.SPParameter;
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
import org.teiid.query.sql.symbol.Expression;
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
 * @since 4.2
 */
public class PreOrPostOrderNavigator extends AbstractNavigator {

    public static final boolean PRE_ORDER = true;
    public static final boolean POST_ORDER = false;
    
    private boolean order;
    private boolean deep;
    
    public PreOrPostOrderNavigator(LanguageVisitor visitor, boolean order, boolean deep) {
        super(visitor);
        this.order = order;
        this.deep = deep;
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
        visitNode(obj.getOrderBy());
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
        visitNode(obj.getExpression());
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
        if (deep) {
        	visitNode(obj.getCommand());
        }
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
        visitNode(obj.getExpression());
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
        if (deep) {
        	visitNode(obj.getCommand());
        }
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
        if(deep && obj.getQueryExpression()!=null) {
        	visitNode(obj.getQueryExpression());
        }
        visitNode(obj.getOption());
        postVisitVisitor(obj);
    }
    public void visit(Create obj) {
        preVisitVisitor(obj);
        visitNode(obj.getTable());
        visitNodes(obj.getColumnSymbols());
        visitNodes(obj.getPrimaryKey());
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
        if (deep) {
        	visitNode(obj.getCommand());
        }
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
        visitNodes(obj.getOrderByItems());
        postVisitVisitor(obj);
    }
    @Override
    public void visit(OrderByItem obj) {
    	preVisitVisitor(obj);
        visitNode(obj.getSymbol());
        postVisitVisitor(obj);
    }
    public void visit(Query obj) {
        preVisitVisitor(obj);
        visitNodes(obj.getWith());
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
        if (deep) {
        	visitNode(obj.getCommand());
        }
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
        visitNodes(obj.getWith());
        visitNodes(obj.getQueryCommands());
        visitNode(obj.getOrderBy());
        visitNode(obj.getLimit());
        visitNode(obj.getOption());
        postVisitVisitor(obj);
    }

    public void visit(StoredProcedure obj) {
        preVisitVisitor(obj);
        
        Collection<SPParameter> params = obj.getParameters();
        if(params != null && !params.isEmpty()) {
        	for (SPParameter parameter : params) {
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
        if (deep) {
        	visitNode(obj.getCommand());
        }
        postVisitVisitor(obj);
    }
    public void visit(SubqueryFromClause obj) {
        preVisitVisitor(obj);
        if (deep) {
        	visitNode(obj.getCommand());
        }
        visitNode(obj.getGroupSymbol());
        postVisitVisitor(obj);
    }
    public void visit(SubquerySetCriteria obj) {
        preVisitVisitor(obj);
        visitNode(obj.getExpression());
        if (deep) {
        	visitNode(obj.getCommand());
        }
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
    
    /**
     * NOTE: we specifically don't need to visit the as columns or the using identifiers.
     * These will be resolved by the dynamic command resolver instead.
     * 
     * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.lang.DynamicCommand)
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
    
    @Override
    public void visit(TextLine obj) {
    	preVisitVisitor(obj);
    	visitNodes(obj.getExpressions());
        postVisitVisitor(obj);
    }    
    
    @Override
    public void visit(XMLForest obj) {
    	preVisitVisitor(obj);
    	visitNode(obj.getNamespaces());
    	visitNodes(obj.getArgs());
        postVisitVisitor(obj);
    }
    
    @Override
    public void visit(XMLAttributes obj) {
    	preVisitVisitor(obj);
    	visitNodes(obj.getArgs());
        postVisitVisitor(obj);
    }
    
    @Override
    public void visit(XMLElement obj) {
    	preVisitVisitor(obj);
    	visitNode(obj.getNamespaces());
    	visitNode(obj.getAttributes());
    	visitNodes(obj.getContent());
    	postVisitVisitor(obj);
    }
    
    @Override
    public void visit(XMLNamespaces obj) {
    	preVisitVisitor(obj);
    	postVisitVisitor(obj);
    }
    
    @Override
    public void visit(TextTable obj) {
        preVisitVisitor(obj);
        visitNode(obj.getFile());
        visitNode(obj.getGroupSymbol());
        postVisitVisitor(obj);
    }
    
    @Override
    public void visit(XMLTable obj) {
    	preVisitVisitor(obj);
    	visitNode(obj.getNamespaces());
    	visitNodes(obj.getPassing());
    	for (XMLTable.XMLColumn column : obj.getColumns()) {
			visitNode(column.getDefaultExpression());
		}
        visitNode(obj.getGroupSymbol());
        postVisitVisitor(obj);
    }
    
    @Override
    public void visit(XMLQuery obj) {
    	preVisitVisitor(obj);
    	visitNode(obj.getNamespaces());
    	visitNodes(obj.getPassing());
        postVisitVisitor(obj);
    }
    
    @Override
    public void visit(DerivedColumn obj) {
    	preVisitVisitor(obj);
    	visitNode(obj.getExpression());
    	postVisitVisitor(obj);
    }
    
    @Override
    public void visit(XMLSerialize obj) {
    	preVisitVisitor(obj);
    	visitNode(obj.getExpression());
    	postVisitVisitor(obj);
    }
    
    @Override
    public void visit(QueryString obj) {
    	preVisitVisitor(obj);
    	visitNode(obj.getPath());
    	visitNodes(obj.getArgs());
    	postVisitVisitor(obj);
    }
    
    @Override
    public void visit(XMLParse obj) {
    	preVisitVisitor(obj);
    	visitNode(obj.getExpression());
    	postVisitVisitor(obj);
    }
    
    @Override
    public void visit(ExpressionCriteria obj) {
    	preVisitVisitor(obj);
    	visitNode(obj.getExpression());
    	postVisitVisitor(obj);
    }
    
    @Override
    public void visit(WithQueryCommand obj) {
    	preVisitVisitor(obj);
    	visitNodes(obj.getColumns());
    	if (deep) {
    		visitNode(obj.getCommand());
    	}
    	postVisitVisitor(obj);
    }
    
    @Override
    public void visit(TriggerAction obj) {
    	preVisitVisitor(obj);
    	visitNode(obj.getBlock());
    	postVisitVisitor(obj);
    }
    
    @Override
    public void visit(ArrayTable obj) {
        preVisitVisitor(obj);
        visitNode(obj.getArrayValue());
        visitNode(obj.getGroupSymbol());
        postVisitVisitor(obj);
    }
    
    @Override
    public void visit(AlterProcedure obj) {
    	preVisitVisitor(obj);
        visitNode(obj.getTarget());
        visitNode(obj.getDefinition());
        postVisitVisitor(obj);
    }
    
    @Override
    public void visit(AlterTrigger obj) {
    	preVisitVisitor(obj);
        visitNode(obj.getTarget());
        visitNode(obj.getDefinition());
        postVisitVisitor(obj);
    }
    
    @Override
    public void visit(AlterView obj) {
    	preVisitVisitor(obj);
        visitNode(obj.getTarget());
        visitNode(obj.getDefinition());
        postVisitVisitor(obj);
    }
    
    public static void doVisit(LanguageObject object, LanguageVisitor visitor, boolean order) {
    	doVisit(object, visitor, order, false);
    }
    
    public static void doVisit(LanguageObject object, LanguageVisitor visitor, boolean order, boolean deep) {
        PreOrPostOrderNavigator nav = new PreOrPostOrderNavigator(visitor, order, deep);
        object.acceptVisitor(nav);
    }

}
