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
import org.teiid.language.*;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.translator.TranslatorException;

public class AccumuloQueryVisitor extends HierarchyVisitor {
	
	protected Stack<Object> onGoingExpression  = new Stack<Object>();
	protected List<Range> ranges = new ArrayList<Range>();
	protected Table scanTable;
	protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
	private HashMap<String, Column> keybasedColumnMap = new HashMap<String, Column>();
	private ArrayList<Column> selectColumns = new ArrayList<Column>();
	private ArrayList<IteratorSetting>  scanIterators = new ArrayList<IteratorSetting>();
	private String currentAlias;
	private int aliasIdx = 0;
	private int iteratorPriority = 2;
	private boolean doScanEvaluation = false;
	private AccumuloExecutionFactory ef;
    
	public AccumuloQueryVisitor(AccumuloExecutionFactory ef) {
		this.ef = ef;
	}
	
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
    	visitNodes(obj.getFrom());
    	visitNodes(obj.getDerivedColumns());
        visitNode(obj.getWhere());
        visitNode(obj.getGroupBy());
        visitNode(obj.getHaving());
        visitNode(obj.getOrderBy());
        visitNode(obj.getLimit());
        
        if (this.doScanEvaluation) {
        	HashMap<String, String> options = buildTableMetadata(this.scanTable.getName(), this.scanTable.getColumns(), this.ef.getEncoding());
        	options.put(EvaluatorIterator.QUERYSTRING, SQLStringVisitor.getSQLString(obj.getWhere()));
        	IteratorSetting it = new IteratorSetting(1, EvaluatorIterator.class, options);
        	this.scanIterators.add(it);
        }
        
        if (this.selectColumns.size() < this.scanTable.getColumns().size()) {
        	HashMap<String, String> options = buildTableMetadata(this.scanTable.getName(), this.selectColumns, this.ef.getEncoding());
        	IteratorSetting it = new IteratorSetting(iteratorPriority++, LimitProjectionIterator.class, options);
        	this.scanIterators.add(it);
        }
    }
	
	@Override
	public void visit(DerivedColumn obj) {
		this.currentAlias = buildAlias(obj.getAlias());
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
			this.doScanEvaluation = true;
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
			this.doScanEvaluation = true;
		}
	}
	
	public static boolean isPartOfPrimaryKey(Column column) {
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
			options.put(CountStarIterator.ALIAS, this.currentAlias);
			options.put(CountStarIterator.ENCODING, this.ef.getEncoding());
			IteratorSetting it = new IteratorSetting(this.iteratorPriority++, CountStarIterator.class, options);
			
			// expression expects a column
			Column c = new Column();
			c.setName(this.currentAlias);
			c.setDatatype(SystemMetadata.getInstance().getSystemStore().getDatatypes().get("integer"));//$NON-NLS-1$
			c.setProperty(AccumuloMetadataProcessor.CF, this.currentAlias);
			
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
        // this will never be part of the rowid, as it can never be null, so scan
        this.doScanEvaluation = true;
    }
	
	@Override
	public void visit(Literal obj) {
		this.onGoingExpression.push(obj.getValue());
	}
	
	@Override
	public void visit(NamedTable obj) {
		this.scanTable = obj.getMetadataObject();
	}
	
	private static HashMap<String, String> buildTableMetadata(String tableName, List<Column> columns, String encoding) {
        HashMap<String, String> options = new HashMap<String, String>();
        options.put(EvaluatorIterator.COLUMNS_COUNT, String.valueOf(columns.size()));
        options.put(EvaluatorIterator.TABLENAME, tableName);
        options.put(EvaluatorIterator.ENCODING, encoding);
        
        for (int i = 0; i < columns.size(); i++) {
        	Column column = columns.get(i);
        	options.put(EvaluatorIterator.createColumnName(EvaluatorIterator.NAME, i), column.getName());
        	if (!SQLStringVisitor.getRecordName(column).equals(AccumuloMetadataProcessor.ROWID)) {
	        	options.put(EvaluatorIterator.createColumnName(EvaluatorIterator.CF, i), column.getProperty(AccumuloMetadataProcessor.CF, false));
	        	if (column.getProperty(AccumuloMetadataProcessor.CQ, false) != null) {
	        		options.put(EvaluatorIterator.createColumnName(EvaluatorIterator.CQ, i), column.getProperty(AccumuloMetadataProcessor.CQ, false));
	        	}
	        	if (column.getProperty(AccumuloMetadataProcessor.VALUE_IN, false) != null) {
	        		options.put(EvaluatorIterator.createColumnName(EvaluatorIterator.VALUE_IN, i), column.getProperty(AccumuloMetadataProcessor.VALUE_IN, false));
	        	}
        	}
        	options.put(EvaluatorIterator.createColumnName(EvaluatorIterator.DATA_TYPE, i), column.getDatatype().getJavaClassName());
        }
		return options;
	}
}
