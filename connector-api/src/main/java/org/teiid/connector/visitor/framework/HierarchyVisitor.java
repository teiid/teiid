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

import org.teiid.connector.language.IAggregate;
import org.teiid.connector.language.IBatchedUpdates;
import org.teiid.connector.language.ICompareCriteria;
import org.teiid.connector.language.ICompoundCriteria;
import org.teiid.connector.language.IDelete;
import org.teiid.connector.language.IExistsCriteria;
import org.teiid.connector.language.IFrom;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.IGroupBy;
import org.teiid.connector.language.IInCriteria;
import org.teiid.connector.language.IInlineView;
import org.teiid.connector.language.IInsert;
import org.teiid.connector.language.IInsertExpressionValueSource;
import org.teiid.connector.language.IIsNullCriteria;
import org.teiid.connector.language.IJoin;
import org.teiid.connector.language.ILikeCriteria;
import org.teiid.connector.language.INotCriteria;
import org.teiid.connector.language.IOrderBy;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.language.IQueryCommand;
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
    
    public void visit(IAggregate obj) {
        visitNode(obj.getExpression());
    }
    
    public void visit(IBatchedUpdates obj) {
        visitNodes(obj.getUpdateCommands());
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
    
    public void visit(IProcedure obj) {
        visitNodes(obj.getParameters());
    }
    
    public void visit(IExistsCriteria obj) {
        if (visitSubcommands) {
        	visitNode(obj.getQuery());
        }
    }
    
    public void visit(IFrom obj) {
        visitNodes(obj.getItems());
    }
    
    public void visit(IFunction obj) {
        visitNodes(obj.getParameters());
    }

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
        if (!(obj.getValueSource() instanceof IQueryCommand) || visitSubcommands) {
    		visitNode(obj.getValueSource());
        }
    }
    
    @Override
    public void visit(IInsertExpressionValueSource obj) {
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

    public void visit(INotCriteria obj) {
        visitNode(obj.getCriteria());
    }
    
    public void visit(IOrderBy obj) {
        visitNodes(obj.getItems());
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
    	if (visitSubcommands) {
    		visitNode(obj.getQuery());
    	}
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
        if (visitSubcommands) {
        	visitNode(obj.getQuery());
        }
    }

    public void visit(ISubqueryInCriteria obj) {
        visitNode(obj.getLeftExpression());        
        if (visitSubcommands) {
        	visitNode(obj.getQuery());
        }
    }
    
    public void visit(ISetQuery obj) {
    	if (visitSubcommands) {
	    	visitNode(obj.getLeftQuery());
	        visitNode(obj.getRightQuery());
    	}
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
    	if (visitSubcommands) {
    		visitNode(obj.getQuery());
    	}
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

}
