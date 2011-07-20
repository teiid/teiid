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
import org.teiid.language.BatchedUpdates;
import org.teiid.language.Call;
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
import org.teiid.language.Join;
import org.teiid.language.Like;
import org.teiid.language.Not;
import org.teiid.language.OrderBy;
import org.teiid.language.QueryExpression;
import org.teiid.language.ScalarSubquery;
import org.teiid.language.SearchedCase;
import org.teiid.language.SearchedWhenClause;
import org.teiid.language.Select;
import org.teiid.language.SetClause;
import org.teiid.language.SetQuery;
import org.teiid.language.SubqueryComparison;
import org.teiid.language.SubqueryIn;
import org.teiid.language.Update;
import org.teiid.language.With;
import org.teiid.language.WithItem;

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

	private boolean visitSubcommands;
	
	public HierarchyVisitor() {
		this(true);
	}
	
    public HierarchyVisitor(boolean visitSubcommands) {
    	this.visitSubcommands = visitSubcommands;
    }
    
    public void visit(AggregateFunction obj) {
        visitNode(obj.getExpression());
        visitNode(obj.getCondition());
    }
    
    public void visit(BatchedUpdates obj) {
        visitNodes(obj.getUpdateCommands());
    }
    
    public void visit(Comparison obj) {
        visitNode(obj.getLeftExpression());
        visitNode(obj.getRightExpression());
    }
    
    public void visit(AndOr obj) {
        visitNode(obj.getLeftCondition());
        visitNode(obj.getRightCondition());
    }
    
    public void visit(Delete obj) {
        visitNode(obj.getTable());
        visitNode(obj.getWhere());
    }
    
    public void visit(Call obj) {
        visitNodes(obj.getArguments());
    }
    
    public void visit(Exists obj) {
        if (visitSubcommands) {
        	visitNode(obj.getSubquery());
        }
    }
    
    public void visit(Function obj) {
        visitNodes(obj.getParameters());
    }

    public void visit(GroupBy obj) {
        visitNodes(obj.getElements());
    }
    
    public void visit(In obj) {
        visitNode(obj.getLeftExpression());
        visitNodes(obj.getRightExpressions());
    }
    
    public void visit(Insert obj) {
        visitNode(obj.getTable());
        visitNodes(obj.getColumns());
        if (!(obj.getValueSource() instanceof QueryExpression) || visitSubcommands) {
    		visitNode(obj.getValueSource());
        }
    }
    
    @Override
    public void visit(ExpressionValueSource obj) {
    	visitNodes(obj.getValues());
    }
    
    public void visit(IsNull obj) {
        visitNode(obj.getExpression());
    }
    
    public void visit(Join obj) {
        visitNode(obj.getLeftItem());
        visitNode(obj.getRightItem());
        visitNode(obj.getCondition());
    }
    
    public void visit(Like obj) {
        visitNode(obj.getLeftExpression());
        visitNode(obj.getRightExpression());
    }

    public void visit(Not obj) {
        visitNode(obj.getCriteria());
    }
    
    public void visit(OrderBy obj) {
        visitNodes(obj.getSortSpecifications());
    }

    public void visit(Select obj) {
    	visitNode(obj.getWith());
    	visitNodes(obj.getDerivedColumns());
        visitNodes(obj.getFrom());
        visitNode(obj.getWhere());
        visitNode(obj.getGroupBy());
        visitNode(obj.getHaving());
        visitNode(obj.getOrderBy());
        visitNode(obj.getLimit());
    }

    public void visit(ScalarSubquery obj) {
    	if (visitSubcommands) {
    		visitNode(obj.getSubquery());
    	}
    }
    
    public void visit(SearchedCase obj) {
    	visitNodes(obj.getCases());
        visitNode(obj.getElseExpression());
    }
    
    @Override
    public void visit(SearchedWhenClause obj) {
    	visitNode(obj.getCondition());
    	visitNode(obj.getResult());
    }
    
    public void visit(DerivedColumn obj) {
        visitNode(obj.getExpression());
    }

    public void visit(SubqueryComparison obj) {
        visitNode(obj.getLeftExpression());
        if (visitSubcommands) {
        	visitNode(obj.getSubquery());
        }
    }

    public void visit(SubqueryIn obj) {
        visitNode(obj.getLeftExpression());        
        if (visitSubcommands) {
        	visitNode(obj.getSubquery());
        }
    }
    
    public void visit(SetQuery obj) {
    	visitNode(obj.getWith());
    	if (visitSubcommands) {
	    	visitNode(obj.getLeftQuery());
	        visitNode(obj.getRightQuery());
    	}
        visitNode(obj.getOrderBy());
        visitNode(obj.getLimit());
    }
    
    public void visit(Update obj) {
        visitNode(obj.getTable());
        visitNodes(obj.getChanges());
        visitNode(obj.getWhere());
    }
    
    @Override
    public void visit(DerivedTable obj) {
    	if (visitSubcommands) {
    		visitNode(obj.getQuery());
    	}
    }
    
    @Override
    public void visit(SetClause obj) {
    	visitNode(obj.getSymbol());
    	visitNode(obj.getValue());
    }
    
    @Override
    public void visit(With obj) {
    	visitNodes(obj.getItems());
    }
    
    @Override
    public void visit(WithItem obj) {
    	visitNode(obj.getTable());
    	visitNodes(obj.getColumns());
    	if (visitSubcommands) {
    		visitNode(obj.getSubquery());
    	}
    }
    

}
