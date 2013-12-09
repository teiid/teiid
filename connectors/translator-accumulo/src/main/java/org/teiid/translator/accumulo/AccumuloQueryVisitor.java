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
package org.teiid.translator.accumulo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Stack;

import org.apache.accumulo.core.data.Range;
import org.teiid.language.AndOr;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.DerivedColumn;
import org.teiid.language.In;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.Select;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;

public class AccumuloQueryVisitor extends HierarchyVisitor {
	
	protected Stack<Object> onGoingExpression  = new Stack<Object>();
	protected List<Range> ranges = new ArrayList<Range>();
	protected Table scanTable;
	protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
	private HashMap<String, Column> keybasedColumnMap = new HashMap<String, Column>();
	private ArrayList<Column> selectColumns = new ArrayList<Column>();
	
    
	public List<Range> getRanges(){
		return this.ranges;
	}
	
	public Table getScanTable() {
		return this.scanTable;
	}
	
	public Column lookupColumn(String key) {
		return this.keybasedColumnMap.get(key);
	}
	
	public ArrayList<Column> projectedColumns(){
		return this.selectColumns;
	}
	
	@Override
	public void visit(Select obj) {
    	visitNodes(obj.getFrom());
    	visitNodes(obj.getDerivedColumns());        
        visitNode(obj.getWhere());
        visitNode(obj.getGroupBy());
        visitNode(obj.getHaving());
        visitNode(obj.getOrderBy());
        visitNode(obj.getLimit());
    }
	
	@Override
	public void visit(DerivedColumn obj) {
		visitNode(obj.getExpression());
		
		Column column = (Column)this.onGoingExpression.pop();
		String CF = column.getProperty(AccumuloMetadataProcessor.CF, false);
		String CQ = column.getProperty(AccumuloMetadataProcessor.CQ, false);
		if (CQ != null) {
			this.keybasedColumnMap.put(CF+"/"+CQ, column); //$NON-NLS-1$
		}
		else {
			this.keybasedColumnMap.put(CF, column);
		}
		
		// no expressions in select are allowed.
		this.selectColumns.add(column);
	}	
	@Override
	public void visit(ColumnReference obj) {
		this.onGoingExpression.push(obj.getMetadataObject());
	}
	
	@Override
    public void visit(AndOr obj) {
        // only AND is allowed through capabilities
        visitNode(obj.getLeftCondition());
        visitNode(obj.getRightCondition());
        
       	this.ranges = Range.mergeOverlapping(this.ranges);
    }	
	
	@Override
	public void visit(Comparison obj) {
		visitNode(obj.getLeftExpression());
		this.onGoingExpression.pop();
		
		visitNode(obj.getRightExpression());
		Object rightExpr = this.onGoingExpression.pop();
		
    	switch(obj.getOperator()) {
        case EQ:
        	this.ranges.add(Range.exact(rightExpr.toString()));
        	break;
        case NE:
        	this.ranges.add(new Range(null, true, rightExpr.toString(), false));
        	this.ranges.add(new Range(rightExpr.toString(), false, null, true));
        	break;
        case LT:
        	this.ranges.add(new Range(null, true, rightExpr.toString(), false));        	
        	break;
        case LE:
        	this.ranges.add(new Range(null, true, rightExpr.toString(), true));
        	break;
        case GT:
        	this.ranges.add(new Range(rightExpr.toString(), false, null, true));        	
        	break;
        case GE:
        	this.ranges.add(new Range(rightExpr.toString(), true, null, true));
        	break;
        }	
	}
	
	@Override
	public void visit(In obj) {
		visitNode(obj.getLeftExpression());
		this.onGoingExpression.pop();

		visitNodes(obj.getRightExpressions());

		Object prevExpr = null;
		// NOTE: we are popping in reverse order to IN stmt
        for (int i = 0; i < obj.getRightExpressions().size(); i++) {
        	Object rightExpr = this.onGoingExpression.pop();
        	Range range = Range.exact(rightExpr.toString());
        	if (obj.isNegated()) {
        		if (prevExpr == null) {
        			this.ranges.add(new Range(rightExpr.toString(), false, null, true));
        			this.ranges.add(new Range(null, true, rightExpr.toString(), false));
        		}
        		else {
        			this.ranges.remove(this.ranges.size()-1);
        			this.ranges.add(new Range(rightExpr.toString(), false, prevExpr.toString(), false));
        			this.ranges.add(new Range(null, true, rightExpr.toString(), false));
        		}
        		prevExpr = rightExpr;
        	}
        	else {
        		this.ranges.add(range);
        	}
        }
	}
	
	@Override
	public void visit(Literal obj) {
		this.onGoingExpression.push(obj.getValue());
	}
	
	@Override
	public void visit(NamedTable obj) {
		this.scanTable = obj.getMetadataObject();
	}	

}
