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
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
/**
 * Implements aggregate function Count(*) over Accumulo
 */
public class CountStarIterator extends WrappingIterator {
	public static final String ALIAS = "alias"; //$NON-NLS-1$
	public static final String ENCODING = "ENCODING"; //$NON-NLS-1$
	private Key topKey;
	private Value topValue;
	private String alias;
	private Charset encoding = Charset.defaultCharset();
	
	@Override
	public void init(SortedKeyValueIterator<Key, Value> source,
			Map<String, String> options, IteratorEnvironment env)
			throws IOException {
		super.init(source, options, env);
		this.alias = options.get(ALIAS);
		this.encoding = Charset.forName(options.get(ENCODING));
	}
	
	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		CountStarIterator newInstance;
		try {
			newInstance = this.getClass().newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		newInstance.setSource(getSource().deepCopy(env));
		newInstance.alias = alias;
		newInstance.topKey = topKey;
		newInstance.topValue = topValue;
		return newInstance;
	}
	
	@Override
	public void seek(Range range, Collection<ByteSequence> columnFamilies,
			boolean inclusive) throws IOException {
		super.seek(range, columnFamilies, inclusive);	
		
		if (getSource().hasTop()) {
			int count = 0;
			ByteSequence prevRowId  = null; 
			while (getSource().hasTop()) {
				Key key = getSource().getTopKey();				
				ByteSequence rowId = key.getRowData();
				if (prevRowId == null || !prevRowId.equals(rowId)) {
					count++;
					prevRowId = rowId;
				}
				getSource().next();
			}
			this.topKey = new Key("1", this.alias, this.alias);//$NON-NLS-1$
			this.topValue = new Value(AccumuloDataTypeManager.convertToAccumuloType(Long.valueOf(count), this.encoding));
		}
	}
	
	@Override
	public Value getTopValue() {
		return topValue;
	}

	@Override
	public Key getTopKey() {
		return topKey;
	}

	@Override
	public boolean hasTop() {
		return topKey != null;
	}

	@Override
	public void next() throws IOException {
		this.topKey = null;
		this.topValue = null;
	}
}