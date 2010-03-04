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

import org.teiid.connector.language.AggregateFunction;
import org.teiid.connector.language.AndOr;
import org.teiid.connector.language.BatchedUpdates;
import org.teiid.connector.language.Call;
import org.teiid.connector.language.Comparison;
import org.teiid.connector.language.Delete;
import org.teiid.connector.language.DerivedColumn;
import org.teiid.connector.language.DerivedTable;
import org.teiid.connector.language.Exists;
import org.teiid.connector.language.ExpressionValueSource;
import org.teiid.connector.language.Function;
import org.teiid.connector.language.GroupBy;
import org.teiid.connector.language.In;
import org.teiid.connector.language.Insert;
import org.teiid.connector.language.IsNull;
import org.teiid.connector.language.Join;
import org.teiid.connector.language.Like;
import org.teiid.connector.language.Not;
import org.teiid.connector.language.OrderBy;
import org.teiid.connector.language.QueryExpression;
import org.teiid.connector.language.ScalarSubquery;
import org.teiid.connector.language.SearchedCase;
import org.teiid.connector.language.SearchedWhenClause;
import org.teiid.connector.language.Select;
import org.teiid.connector.language.SetClause;
import org.teiid.connector.language.SetQuery;
import org.teiid.connector.language.SubqueryComparison;
import org.teiid.connector.language.SubqueryIn;
import org.teiid.connector.language.Update;

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

}
