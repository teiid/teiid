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
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.teiid.core.util.Base64;

/**
 * Implements "In" criteria over the Accumulo data.
 */
public class InFilterIterator extends BaseFilterIterator {
	public static final String VALUES_COUNT = "VALUES_COUNT"; //$NON-NLS-1$
	public static final String VALUES = "VALUES"; //$NON-NLS-1$
	private int valueCount = 0;
	List<byte[]> values = new ArrayList<byte[]>(); 
	
	@Override
	public void init(SortedKeyValueIterator<Key, Value> source,
			Map<String, String> options, IteratorEnvironment env)
			throws IOException {
		super.init(source, options, env);
		
		this.valueCount = Integer.parseInt(options.get(VALUES_COUNT));
		for (int i = 0; i < this.valueCount; i++) {
			this.values.add(Base64.decode(options.get(VALUES+i)));
		}
	}
	
	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		InFilterIterator instance = (InFilterIterator) super.deepCopy(env);
		instance.valueCount = this.valueCount;
		instance.values = this.values;
		return instance;
	}	
	
	@Override
	public boolean accept(byte[] value) {
		for (byte[] match:values) {
			if (Arrays.equals(match, value)) {
				return true;
			}
		}
		return false;
	}	
}
