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
package org.teiid.odata;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.jboss.resteasy.spi.NotFoundException;
import org.odata4j.expression.*;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.Table;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SubqueryCompareCriteria;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.translator.SourceSystemFunctions;

/**
 * http://host/service.svc/Orders?$filter=OrderLines/all(ol: ol/Quantity gt 10)
 * select * from orders where ALL (select quantity from orderlines where fk = pk) > 10
 * 
 */
public class ODataAggregateAnyBuilder extends ODataHierarchyVisitor {
	private Stack<Expression> stack = new Stack<Expression>();
	private AggregateAllFunction parentExpr;
	private GroupSymbol childGroup;
	private GroupSymbol parentGroup;
	private Table parent; 
	private Table childTable;

	public ODataAggregateAnyBuilder(AggregateAllFunction expr, Table parent, GroupSymbol parentGroup, Table childTable, GroupSymbol childGroup) {
		this.parentExpr = expr;
		this.parent = parent;
		this.parentGroup = parentGroup;
		this.childTable = childTable;
		this.childGroup = childGroup;
		visitNode(expr.getPredicate());
	}

	public Criteria getCriteria() {
		return (Criteria) stack.pop();
	}

	@Override
	public void visit(EqExpression expr) {
		buildCriteria(expr, SubqueryCompareCriteria.EQ);
	}

	@Override
	public void visit(GeExpression expr) {
		buildCriteria(expr, SubqueryCompareCriteria.LE);
	}

	private QueryCommand buildSubquery(Expression projected) {
		Criteria criteria = null;
    	for (ForeignKey fk:this.childTable.getForeignKeys()) {
    		if (fk.getPrimaryKey().getParent().equals(this.parent)) {
				List<String> refColumns = fk.getReferenceColumns();
				if (refColumns == null) {
					refColumns = ODataSQLBuilder.getColumnNames(childTable.getPrimaryKey().getColumns());
				}    				
				
				List<String> pkColumns = ODataSQLBuilder.getColumnNames(parent.getPrimaryKey().getColumns());
				List<Criteria> critList = new ArrayList<Criteria>();

				for (int i = 0; i < refColumns.size(); i++) {
					critList.add(new CompareCriteria(new ElementSymbol(pkColumns.get(i), this.parentGroup), CompareCriteria.EQ, new ElementSymbol(refColumns.get(i), this.childGroup)));
				}         			
				
				criteria = critList.get(0);
				for (int i = 1; i < critList.size(); i++) {
					criteria = new CompoundCriteria(CompoundCriteria.AND, criteria, critList.get(i));
				}		        			
    		}
    	}		
        Select s1 = new Select();
        s1.addSymbol(projected); 
        From f1 = new From();
        f1.addGroup(this.childGroup);
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);	   
        q1.setCriteria(criteria);
        
        return q1;
	}
	
	@Override
	public void visit(GtExpression expr) {
		buildCriteria(expr, SubqueryCompareCriteria.LT);
	}

	public void visit(IsofExpression expr) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void visit(LeExpression expr) {
		buildCriteria(expr, SubqueryCompareCriteria.GE);		
	}

	@Override
	public void visit(LtExpression expr) {
		buildCriteria(expr, SubqueryCompareCriteria.GT);	
	}

	@Override
	public void visit(NeExpression expr) {
		buildCriteria(expr, SubqueryCompareCriteria.NE);	
	}

	@Override
	public void visit(NegateExpression expr) {
		visitNode(expr.getExpression());
		Expression ex = stack.pop();
		stack.push(new Function(SourceSystemFunctions.MULTIPLY_OP, new Expression[] {new Constant(-1), ex}));
	}

	@Override
	public void visit(NotExpression expr) {
	}

	@Override
	public void visit(AndExpression expr) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public void visit(OrExpression expr) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void visit(BoolParenExpression expr) {
		throw new UnsupportedOperationException();
	}

	private void buildCriteria(BinaryCommonExpression expr, int op) {
		visitNode(expr.getLHS());
		visitNode(expr.getRHS());
		Expression rhs = stack.pop();
		Expression lhs = stack.pop();
		Criteria critera = new SubqueryCompareCriteria(rhs, buildSubquery(lhs), op, SubqueryCompareCriteria.ALL); //$NON-NLS-1$
		stack.push(critera);
	}
	
	@Override
	public void visit(EntitySimpleProperty expr) {
		String property = expr.getPropertyName();
		
		int idx = property.indexOf('/');
		if (!property.substring(0, idx).equals(this.parentExpr.getVariable())) {
			throw new NotFoundException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16010));
		}
		stack.push(new ElementSymbol(property.substring(idx+1), this.childGroup));
	}
	
	@Override
	public void visit(BooleanLiteral expr) {
		stack.add(new Constant(expr.getValue())); //$NON-NLS-1$
	}

	@Override
	public void visit(DateTimeLiteral expr) {
		stack.add(new Constant(new Timestamp(expr.getValue().toDateTime().getMillis()))); //$NON-NLS-1$
	}

	@Override
	public void visit(DecimalLiteral expr) {
		stack.add(new Constant(expr.getValue())); //$NON-NLS-1$
	}

	@Override
	public void visit(GuidLiteral expr) {
		stack.add(new Constant(expr.getValue().toString())); //$NON-NLS-1$
	}

	@Override
	public void visit(BinaryLiteral expr) {
		stack.add(new Constant(expr.getValue())); //$NON-NLS-1$
	}

	@Override
	public void visit(ByteLiteral expr) {
		stack.add(new Constant(expr.getValue())); //$NON-NLS-1$
	}

	@Override
	public void visit(SByteLiteral expr) {
		stack.add(new Constant(expr.getValue())); //$NON-NLS-1$
	}

	@Override
	public void visit(SingleLiteral expr) {
		stack.add(new Constant(expr.getValue())); //$NON-NLS-1$
	}

	@Override
	public void visit(DoubleLiteral expr) {
		stack.add(new Constant(expr.getValue())); //$NON-NLS-1$
	}

	@Override
	public void visit(IntegralLiteral expr) {
		stack.add(new Constant(expr.getValue())); //$NON-NLS-1$
	}

	@Override
	public void visit(Int64Literal expr) {
		stack.add(new Constant(expr.getValue())); //$NON-NLS-1$
	}

	@Override
	public void visit(NullLiteral expr) {
		stack.push(new Constant(null));
	}

	@Override
	public void visit(StringLiteral expr) {
		stack.push(new Constant(expr.getValue()));
	}

	@Override
	public void visit(TimeLiteral expr) {
		stack.push(new Constant(new Time(expr.getValue().toDateTimeToday().getMillis())));
	}
}
