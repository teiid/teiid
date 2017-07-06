/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.teiid.query.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.GeneratedKeys;

public class GeneratedKeysImpl implements GeneratedKeys {
	private List<List<?>> keys = new ArrayList<List<?>>();
	private String[] colNames;
	private Class<?>[] types;
	
	protected GeneratedKeysImpl(String[] colNames, Class<?>[] types) {
		this.colNames = colNames;
		this.types = types;
	}
	
	@Override
	public void addKey(List<?> vals) {
		if (vals != null) {
			keys.add(vals);
		}
	}
	
	public List<List<?>> getKeys() {
		return keys;
	}
	
	public String[] getColumnNames() {
		return colNames;
	}
	
	public Class<?>[] getColumnTypes() {
		return types;
	}
	
	public Iterator<List<?>> getKeyIterator() {
		return keys.iterator();
	}

}
