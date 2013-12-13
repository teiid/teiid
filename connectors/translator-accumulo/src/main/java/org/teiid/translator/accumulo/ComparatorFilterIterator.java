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
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.language.Comparison;

/**
 * Implements <, <=, >, >=, ==, <> operators
 */
public class ComparatorFilterIterator extends BaseFilterIterator {
	public static final String OPERATOR = "OPERATOR"; //$NON-NLS-1$
	public static final String VALUE = "VALUE"; //$NON-NLS-1$
	public static final String VALUETYPE= "VALUETYPE"; //$NON-NLS-1$
	
	private Comparison.Operator operator;
	private Object value;
	private Class valueType;
	
	@Override
	public void init(SortedKeyValueIterator<Key, Value> source,
			Map<String, String> options, IteratorEnvironment env)
			throws IOException {
		super.init(source, options, env);
		
		try {
			this.operator = Comparison.Operator.valueOf(options.get(OPERATOR));
			this.valueType = Class.forName(options.get(VALUETYPE));
			this.value = DataTypeManager.transformValue(options.get(VALUE), valueType);
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		} catch (TransformationException e) {
			throw new IOException(e);
		}
	}
	
	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		ComparatorFilterIterator instance = (ComparatorFilterIterator) super.deepCopy(env);
		instance.operator= this.operator;
		instance.valueType = this.valueType;
		instance.value = this.value;
		return instance;
	}	
	
	@Override
	public boolean accept(byte[] value) {
		try {
 			Object o1 = DataTypeManager.transformValue(new String(value), this.valueType);
			Object o2 = this.value;
			int compare = ((Comparable<Object>)o1).compareTo(o2);
			switch(this.operator) {
			case EQ:
				return Boolean.valueOf(compare == 0);
			case NE:
				return Boolean.valueOf(compare != 0);
			case LT:
				return Boolean.valueOf(compare < 0);
			case LE:
				return Boolean.valueOf(compare <= 0);
			case GT:
				return Boolean.valueOf(compare > 0);
			case GE:
				return Boolean.valueOf(compare >= 0);
			}
		} catch (TransformationException e) {
			throw new TeiidRuntimeException(e);
		}		
		return false;
	}
}
