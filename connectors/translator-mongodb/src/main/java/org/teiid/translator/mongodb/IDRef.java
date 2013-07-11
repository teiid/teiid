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
package org.teiid.translator.mongodb;

import java.util.HashMap;

import com.mongodb.BasicDBObject;

public class IDRef implements Cloneable {
	HashMap<String, Object> pk = new HashMap<String, Object>();

	public void addColumn(String key, Object value) {
		// only add if not added before
		if (this.pk.get(key) == null) {
			this.pk.put(key, value);
		}
	}

	public Object getValue() {
		if (this.pk.size() == 1) {
			for (String key:this.pk.keySet()) {
				return this.pk.get(key);
			}
		}
		BasicDBObject value = new BasicDBObject();
		for (String key:this.pk.keySet()) {
			value.append(key, this.pk.get(key));
		}
		return value;
	}

	@Override
	public String toString() {
		Object obj =  getValue();
		if (obj != null) {
			return obj.toString();
		}
		return null;
	}

	@Override
	public IDRef clone() {
		IDRef clone = new IDRef();
		clone.pk.putAll(this.pk);
		return clone;
	}
}
