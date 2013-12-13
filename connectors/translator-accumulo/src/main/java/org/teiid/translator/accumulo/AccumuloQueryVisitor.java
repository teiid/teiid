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

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.teiid.core.types.TransformationException;
import org.teiid.core.util.Base64;
import org.teiid.language.*;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;

public class AccumuloQueryVisitor extends HierarchyVisitor {
	
	protected Stack<Object> onGoingExpression  = new Stack<Object>();
	protected List<Range> ranges = new ArrayList<Range>();
	protected Table scanTable;
	protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
	private HashMap<String, Column> keybasedColumnMap = new HashMap<String, Column>();
	private ArrayList<Column> selectColumns = new ArrayList<Column>();
	private ArrayList<IteratorSetting>  scanIterators = new ArrayList<IteratorSetting>();
	private String onGoingAlias;
	private int aliasIdx = 0;
	private int iteratorPriority = 1;
    
	public List<Range> getRanges(){
		return this.ranges;
	}
	
	public Table getScanTable() {
		return this.scanTable;
	}
	
	public Column lookupColumn(String key) {
		return this.keybasedColumnMap.get(key);
	}
	
	public List<Column> projectedColumns(){
		return this.selectColumns;
	}
	
	public List<IteratorSetting> scanIterators(){
		return this.scanIterators;
	}
	
	@Override
	public void visit(Select obj) {
		// this iterator will force the results to be bundled as single row
		this.scanIterators.add(new IteratorSetting(this.iteratorPriority++, RowFilterIterator.class, new HashMap<String, String>()));
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
		this.onGoingAlias = buildAlias(obj.getAlias());
		visitNode(obj.getExpression());
		
		Object expr = this.onGoingExpression.pop();
		
		if (expr instanceof Column) {
			Column column = (Column)expr;
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
		else if (expr instanceof IteratorSetting) {
			this.scanIterators.add((IteratorSetting)expr);
		}
	}	
	private String buildAlias(String alias) {
		if (alias != null) {
			return alias;
		}
		return "_m"+this.aliasIdx; //$NON-NLS-1$
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
		Column column = (Column)this.onGoingExpression.pop();
		
		visitNode(obj.getRightExpression());
		Object rightExpr = this.onGoingExpression.pop();
		
		if (isPartOfPrimaryKey(column)) {
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
		else {
			try {
				// insert iterarator here..
				HashMap<String, String> options = buildColumnOptions(column);
				options.put(ComparatorFilterIterator.OPERATOR, obj.getOperator().name());
				options.put(ComparatorFilterIterator.VALUE, AccumuloDataTypeManager.convertToStringType(rightExpr));
				options.put(ComparatorFilterIterator.VALUETYPE, column.getDatatype().getJavaClassName());
				IteratorSetting it = new IteratorSetting(this.iteratorPriority++, ComparatorFilterIterator.class, options);
				this.scanIterators.add(it);
			} catch (TransformationException e) {
				this.exceptions.add(new TranslatorException(e));
			}			
		}
	}
	
	@Override
	public void visit(In obj) {
		visitNode(obj.getLeftExpression());
		Column column = (Column)this.onGoingExpression.pop();
		
		visitNodes(obj.getRightExpressions());
		
		if (isPartOfPrimaryKey(column)) {
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
		else {
			// regular column
	        HashMap<String, String> options = buildColumnOptions(column);
	        options.put(InFilterIterator.VALUES_COUNT, Integer.toString(obj.getRightExpressions().size()));
	        for (int i = 0; i < obj.getRightExpressions().size(); i++) {
	        	Object rightExpr = this.onGoingExpression.pop();
	        	byte[] value = AccumuloDataTypeManager.convertToAccumuloType(rightExpr);
	        	options.put(InFilterIterator.VALUES+i, Base64.encodeBytes(value));
	        }
	        
			IteratorSetting it = new IteratorSetting(this.iteratorPriority++, InFilterIterator.class, options);
			this.scanIterators.add(it);
		}
	}
	
	public boolean isPartOfPrimaryKey(Column column) {
		KeyRecord pk = ((Table)column.getParent()).getPrimaryKey();
		if (pk != null) {
			for (Column col:pk.getColumns()) {
				if (col.getName().equals(column.getName())) {
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public void visit(AggregateFunction obj) {
    	if (!obj.getParameters().isEmpty()) {
    		visitNodes(obj.getParameters());
    	}

		if (obj.getName().equals(AggregateFunction.COUNT)) {
			HashMap<String, String> options = new HashMap<String, String>();
			options.put(CountStarIterator.ALIAS, this.onGoingAlias);
			IteratorSetting it = new IteratorSetting(this.iteratorPriority++, CountStarIterator.class, options);
			
			// expression expects a column
			Column c = new Column();
			c.setName(this.onGoingAlias);
			c.setProperty(AccumuloMetadataProcessor.CF, this.onGoingAlias);
			
			this.scanIterators.add(it);
			this.onGoingExpression.push(c) ;
		}
		else if (obj.getName().equals(AggregateFunction.AVG)) {
		}
		else if (obj.getName().equals(AggregateFunction.SUM)) {
		}
		else if (obj.getName().equals(AggregateFunction.MIN)) {
		}
		else if (obj.getName().equals(AggregateFunction.MAX)) {
		}
		else {
		}
    }
	
    @Override
	public void visit(IsNull obj) {
        visitNode(obj.getExpression());
        Column column = (Column)onGoingExpression.pop();
		
        HashMap<String, String> options = buildColumnOptions(column);
        options.put(IsNullFilterIterator.NEGATE, Boolean.toString(obj.isNegated()));
        
		IteratorSetting it = new IteratorSetting(this.iteratorPriority++, IsNullFilterIterator.class, options);
		this.scanIterators.add(it);
    }

	private HashMap<String, String> buildColumnOptions(Column column) {
		HashMap<String, String> options = new HashMap<String, String>();
		String CF = column.getProperty(AccumuloMetadataProcessor.CF, false);
		String CQ = column.getProperty(AccumuloMetadataProcessor.CQ, false);
		String valueIn = column.getProperty(AccumuloMetadataProcessor.VALUE_IN, false);
		if (CF != null) {
			options.put(AccumuloMetadataProcessor.CF, CF);
		}
		if (CQ != null) {
			options.put(AccumuloMetadataProcessor.CQ, CQ);
		}
		if (valueIn != null) {
			options.put(AccumuloMetadataProcessor.VALUE_IN, valueIn);
		}
		return options;
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
