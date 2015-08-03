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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.hadoop.io.Text;
import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.translator.accumulo.AccumuloMetadataProcessor.ValueIn;

/**
 * This iterator makes uses of Teiid engine for criteria evaluation. For this to work, the teiid libraries 
 * need to be copied over to the accumulo classpath.
 * 
 * RowFilter based implemention fails with "java.lang.RuntimeException: Setting interrupt 
 * flag after calling deep copy not supported", this is copy of WholeRowIterator
 */
public class EvaluatorIterator extends WrappingIterator {
	public static final String QUERYSTRING = "QUERYSTRING"; //$NON-NLS-1$
	public static final String COLUMNS_COUNT = "COLUMN_COUNT"; //$NON-NLS-1$
	public static final String COLUMN = "COLUMN"; //$NON-NLS-1$
	public static final String NAME = "NAME"; //$NON-NLS-1$
	public static final String CF = "CF"; //$NON-NLS-1$
	public static final String CQ = "CQ"; //$NON-NLS-1$
	public static final String VALUE_IN = "VALUE_IN"; //$NON-NLS-1$
	public static final String DATA_TYPE = "DATA_TYPE"; //$NON-NLS-1$
	public static final String TABLENAME = "TABLENAME";//$NON-NLS-1$
	public static final String ENCODING = "ENCODING";//$NON-NLS-1$
	
	static class KeyValuePair{
		Key key;
		Value value;
	}
	
	private Criteria criteria;
	private Evaluator evaluator;
	private Collection<ElementSymbol> elementsInExpression;
	private EvaluatorUtil evaluatorUtil;
	private ArrayList<KeyValuePair> currentValues;
	
	private Iterator<KeyValuePair> rowIterator;
	private Key topKey;
	private Value topValue;
	
	@Override
	public void init(SortedKeyValueIterator<Key, Value> source,
			Map<String, String> options, IteratorEnvironment env)
			throws IOException {
		super.init(source, options, env);
		
		this.evaluatorUtil = new EvaluatorUtil(Charset.forName(options.get(ENCODING)));
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
				evaluatorUtil.addColumn(i, table, name, cf, cq, type, valueIn);
			}
		} catch (QueryParserException e) {
			throw new IOException(e);
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
		this.evaluator = new Evaluator(this.evaluatorUtil.getElementMap(), null, null);
	}
	
	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		EvaluatorIterator newInstance;
		try {
			newInstance = this.getClass().newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		newInstance.setSource(getSource().deepCopy(env));
		newInstance.criteria = this.criteria;
		newInstance.currentValues = this.currentValues;
		newInstance.elementsInExpression = this.elementsInExpression;
		newInstance.evaluator = this.evaluator;
		newInstance.evaluatorUtil = this.evaluatorUtil;
		newInstance.topKey = this.topKey;
		newInstance.topValue = this.topValue;
		newInstance.rowIterator = this.rowIterator;
		return newInstance;
	}
	
	  @Override
	  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
	    
	    Key sk = range.getStartKey();
	    
	    if (sk != null && sk.getColumnFamilyData().length() == 0 && sk.getColumnQualifierData().length() == 0 && sk.getColumnVisibilityData().length() == 0
	        && sk.getTimestamp() == Long.MAX_VALUE && !range.isStartKeyInclusive()) {
	      // assuming that we are seeking using a key previously returned by this iterator
	      // therefore go to the next row
	      Key followingRowKey = sk.followingKey(PartialKey.ROW);
	      if (range.getEndKey() != null && followingRowKey.compareTo(range.getEndKey()) > 0)
	        return;
	      
	      range = new Range(sk.followingKey(PartialKey.ROW), true, range.getEndKey(), range.isEndKeyInclusive());
	    }
	    
	    getSource().seek(range, columnFamilies, inclusive);
	    prepKeys();
	  }	
	  
	private void prepKeys() throws IOException {
		this.currentValues = new ArrayList<EvaluatorIterator.KeyValuePair>();
		Text currentRow;
		do {
			this.currentValues.clear();
			this.rowIterator = null;
			if (getSource().hasTop() == false) {
				this.currentValues = null;				
				return;
			}
			currentRow = new Text(getSource().getTopKey().getRow());
			while (getSource().hasTop() && getSource().getTopKey().getRow().equals(currentRow)) {
				KeyValuePair kv = new KeyValuePair();
				kv.key = getSource().getTopKey();
				kv.value = new Value(getSource().getTopValue());
				this.currentValues.add(kv);
				getSource().next();
			}
		} while (!filter(this.currentValues));
	}	  
	
	protected boolean filter(ArrayList<KeyValuePair> values) throws IOException {
		if (acceptRow(values)) {
			this.rowIterator = values.iterator();
			advanceRow();				
			return true;
		}
		return false;
	}
	
	@Override
	public Key getTopKey() {
		return this.topKey;
	}

	@Override
	public Value getTopValue() {
		return this.topValue;
	}	

	@Override
	public boolean hasTop() {
		return this.topKey != null;
	}	
	
	@Override
	public void next() throws IOException {
		if (!advanceRow()) {
			prepKeys();
		}
	}

	private boolean advanceRow() {
		if (this.rowIterator != null && this.rowIterator.hasNext()) {
			KeyValuePair kv = this.rowIterator.next();
			this.topKey = kv.key;
			this.topValue = kv.value;
			return true;
		}
		this.topKey = null;
		this.topValue = null;
		this.rowIterator = null;
		return false;
	}
	
	private boolean acceptRow(ArrayList<KeyValuePair> values) throws IOException {
		try {
			return this.evaluator.evaluate(this.criteria, this.evaluatorUtil.buildTuple(values));
		} catch (ExpressionEvaluationException e) {
			throw new IOException(e);
		} catch (BlockedException e) {
			throw new IOException(e);			
		} catch (TeiidComponentException e) {
			throw new IOException(e);			
		}		
	}
	
	public static String createColumnName(String prop, int index) {
		return COLUMN+"."+index+"."+prop;//$NON-NLS-1$ //$NON-NLS-2$
	}
	
	private static class ColumnInfo {
		ElementSymbol es;
		int pos;
		AccumuloMetadataProcessor.ValueIn in;
	}
	
	private static class ColumnSet extends org.apache.accumulo.core.iterators.conf.ColumnSet {
		private Text colf; 
		private Text colq;
		public ColumnSet(Text colf, Text colq) {
			super.add(colf, colq);
			this.colf = colf;
			this.colq = colq;
		}
		public ColumnSet(Text colf) {
			super.add(colf);
			this.colf = colf;
		}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((colf == null) ? 0 : colf.hashCode());
			result = prime * result + ((colq == null) ? 0 : colq.hashCode());
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ColumnSet other = (ColumnSet) obj;
			if (colf == null) {
				if (other.colf != null)
					return false;
			} else if (!colf.equals(other.colf))
				return false;
			if (colq == null) {
				if (other.colq != null)
					return false;
			} else if (!colq.equals(other.colq))
				return false;
			return true;
		}
		
	}
	
	private static class EvaluatorUtil {
		private Map<ColumnSet, ColumnInfo> columnMap =  new HashMap<ColumnSet, ColumnInfo>();		
		private Map<ElementSymbol, Integer> elementMap = new HashMap<ElementSymbol, Integer>();
		private Charset encoding;
		
		public EvaluatorUtil(Charset encoding) {
			this.encoding = encoding;
		}
		
		public void addColumn(int position, GroupSymbol table, String name, String cf, String cq, String type, String valueIn) throws ClassNotFoundException {
			ElementSymbol element = new ElementSymbol(name, table, Class.forName(type));
			this.elementMap.put(element, position);

			AccumuloMetadataProcessor.ValueIn valueInEnum = AccumuloMetadataProcessor.ValueIn.VALUE;
			if (valueIn != null) {
				valueInEnum = AccumuloMetadataProcessor.ValueIn.valueOf(valueIn.substring(1, valueIn.length()-1));
			} 
			
			ColumnInfo col = new ColumnInfo();
			col.es = element;
			col.in = valueInEnum;
			col.pos = position;
			
			ColumnSet cs = null;
			if (cf != null && cq != null) {
				cs = new ColumnSet(new Text(cf), new Text(cq)); 
			} 
			else {
				if (cf == null) {
					cf = AccumuloMetadataProcessor.ROWID;
				}
				cs = new ColumnSet(new Text(cf));
			}		
			this.columnMap.put(cs, col);
		}
		
		public List<?> buildTuple (ArrayList<KeyValuePair> values) {
			Object[] tuple = new Object[this.elementMap.size()];
			
			for (KeyValuePair kv:values) {
				ColumnInfo info = findColumnInfo(kv.key);
				if (info != null) {
					Value v = kv.value;					
					if (ValueIn.CQ.equals(info.in)) {
						tuple[info.pos] = convert(kv.key.getColumnQualifier().getBytes(), info.es, this.encoding);
					}
					else {
						tuple[info.pos] = convert(v.get(), info.es, this.encoding);
					}					
				}
			}
			return Arrays.asList(tuple);
		}
		
		private Object convert(byte[] content, ElementSymbol es, Charset enc) {
			return AccumuloDataTypeManager.convertFromAccumuloType(content, es.getType(), enc);			
		}
		
		private ColumnInfo findColumnInfo(Key key) {
			// could not to do hash look up, as colums may be just based on CF or CF+CQ 
			for(ColumnSet cs:columnMap.keySet()){
				if (cs.contains(key)) {
					return this.columnMap.get(cs);
				}
			}
			return null;
		}
		
		public Map<ElementSymbol, Integer> getElementMap() {
			return this.elementMap;
		}		
	}
}
