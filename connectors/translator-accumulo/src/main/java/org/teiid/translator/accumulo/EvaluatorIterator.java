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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.conf.ColumnSet;
import org.apache.accumulo.core.iterators.user.RowFilter;
import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.translator.accumulo.AccumuloMetadataProcessor.ValueIn;

/**
 * This iterator makes uses of Teiid engine for criteria evaluation. For this to work, the teiid libraries 
 * need to be copied over to the accumulo classpath.
 */
public class EvaluatorIterator extends RowFilter{
	public static final String QUERYSTRING = "QUERYSTRING"; //$NON-NLS-1$
	public static final String COLUMNS_COUNT = "COLUMN_COUNT"; //$NON-NLS-1$
	public static final String COLUMN = "COLUMN"; //$NON-NLS-1$
	public static final String NAME = "NAME"; //$NON-NLS-1$
	public static final String CF = "CF"; //$NON-NLS-1$
	public static final String CQ = "CQ"; //$NON-NLS-1$
	public static final String VALUE_IN = "VALUE_IN"; //$NON-NLS-1$
	public static final String DATA_TYPE = "DATA_TYPE"; //$NON-NLS-1$
	public static final String TABLENAME = "TABLENAME";//$NON-NLS-1$
	
	private Criteria criteria;
	private Evaluator evaluator;
	
	private Collection<ElementSymbol> elementsInExpression;
	private ExpressionUtil util = new ExpressionUtil();
	
	@Override
	public void init(SortedKeyValueIterator<Key, Value> source,
			Map<String, String> options, IteratorEnvironment env)
			throws IOException {
		super.init(source, options, env);
		
		String query = options.get(QUERYSTRING);
		QueryParser parser = QueryParser.getQueryParser();
		try {
			this.criteria = parser.parseCriteria(query);
			this.elementsInExpression = ElementCollectorVisitor.getElements(this.criteria, true);
			
			String tableName = options.get(TABLENAME);
			GroupSymbol table = new GroupSymbol(tableName);
			int columnCount = Integer.parseInt(options.get(COLUMNS_COUNT));
			
			for (int i = 0; i < columnCount; i++) {
				String name = options.get(createColumnName(NAME, i));
				String cf = options.get(createColumnName(CF, i));
				String cq = options.get(createColumnName(CQ, i));
				String type = options.get(createColumnName(DATA_TYPE, i));				
				String valueIn = options.get(createColumnName(VALUE_IN, i));
				util.addColumn(table, name, cf, cq, type, valueIn);
			}
		} catch (QueryParserException e) {
			throw new IOException(e);
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}

	public static String createColumnName(String prop, int index) {
		return COLUMN+"."+index+"."+prop;//$NON-NLS-1$ //$NON-NLS-2$
	}
	
	@Override
	public boolean acceptRow(SortedKeyValueIterator<Key, Value> rowIterator)
			throws IOException {
		ArrayList tuple = new ArrayList();
		
		int idx = 0;
		while(rowIterator.hasTop()) {
			Key key = rowIterator.getTopKey();

			// since the order of the rows coming out of accumulo is not known, need to 
			// figure out how the values are aligned.
			if (this.evaluator == null) {
				this.util.buildElementMap(key, idx);
			}
			
			ValueIn valueIn = util.getValuIn(idx);
			if (ValueIn.CQ.equals(valueIn)) {
				tuple.add(key.getColumnQualifier().getBytes());
			}
			else {
				tuple.add(rowIterator.getTopValue().get());
			}
			rowIterator.next();
			idx++;
		}
		
		if (this.evaluator == null) {
			this.evaluator = new Evaluator(this.util.getElementMap(), null, null);
		}
		
		// convert the values to native types for evaluation.
		for (Expression expr: this.util.getElementMap().keySet()) {
			if (this.elementsInExpression.contains(expr)) {
				Integer position = this.util.getElementMap().get(expr);
				tuple.set(position, AccumuloDataTypeManager.convertFromAccumuloType((byte[])tuple.get(position), expr.getType()));
			}
		}
		
		try {
			return this.evaluator.evaluate(this.criteria, tuple);
		} catch (ExpressionEvaluationException e) {
			throw new IOException(e);
		} catch (BlockedException e) {
			throw new IOException(e);			
		} catch (TeiidComponentException e) {
			throw new IOException(e);			
		}		
	}
	
	private static class ExpressionUtil {
		private ArrayList<ElementSymbol> elements = new ArrayList<ElementSymbol>();
		private ArrayList<ColumnSet> filterColumns =  new ArrayList<ColumnSet>();
		private ArrayList<AccumuloMetadataProcessor.ValueIn> valueIns = new ArrayList<AccumuloMetadataProcessor.ValueIn>();
		private Map<ElementSymbol, Integer> elementMap = new HashMap<ElementSymbol, Integer>();
		private Map<Integer, AccumuloMetadataProcessor.ValueIn> sortedValueIns = new HashMap<Integer, AccumuloMetadataProcessor.ValueIn>();
		
		public void addColumn(GroupSymbol table, String name, String cf, String cq, String type, String valueIn) throws ClassNotFoundException {
			ElementSymbol element = new ElementSymbol(name, table, Class.forName(type));
			this.elements.add(element);
			if (cf != null && cq != null) {
				this.filterColumns.add(new ColumnSet(Arrays.asList(cf + ":" + cq))); //$NON-NLS-1$
			} 
			else {
				if (cf == null) {
					cf = AccumuloMetadataProcessor.ROWID;
				}
				this.filterColumns.add(new ColumnSet(Arrays.asList(cf)));
			}		
			AccumuloMetadataProcessor.ValueIn valueInEnum = AccumuloMetadataProcessor.ValueIn.VALUE;
			if (valueIn != null) {
				valueInEnum = AccumuloMetadataProcessor.ValueIn.valueOf(valueIn.substring(1, valueIn.length()-1));
			} 
			
			this.valueIns.add(valueInEnum);		
		}
		
		private int findMatch(Key key) {
			for (int i = 0; i < this.filterColumns.size(); i++) {
				ColumnSet cs = this.filterColumns.get(i);
				if (cs.contains(key)) {
					return i;
				}
			}
			return -1;
		}
		
		public void buildElementMap(Key key, Integer position) {
			int idx = findMatch(key);
			if (idx != -1) {
				this.elementMap.put(this.elements.get(idx), position);
				this.sortedValueIns.put(position, this.valueIns.get(idx));
			}
		}
		
		public Map<ElementSymbol, Integer> getElementMap() {
			return this.elementMap;
		}
		
		public ValueIn getValuIn(Integer position) {
			return this.sortedValueIns.get(position);
		}
	}
}
