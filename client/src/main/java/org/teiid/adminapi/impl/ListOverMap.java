/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.teiid.adminapi.impl;

import java.io.Serializable;
import java.util.AbstractList;
import java.util.LinkedHashMap;
import java.util.Set;

class ListOverMap<E> extends AbstractList<E> implements Serializable {
	
	private static final long serialVersionUID = 5171741731121210240L;
	
	protected LinkedHashMap<String, E> map = new LinkedHashMap<String, E>();
	protected KeyBuilder<E> builder;
	
	public ListOverMap(KeyBuilder<E> builder) {
		this.builder = builder;
	}
	
	public LinkedHashMap<String, E> getMap() {
		return map;
	}
	
	@Override
	public void add(int index, E element) {
		this.map.put(builder.getKey(element), element);
	}

	@Override
	public E remove(int index) {
		String key = getKey(index);
		if (key == null) {
			throw new IndexOutOfBoundsException("Index: "+index+", Size: "+size());	//$NON-NLS-1$ //$NON-NLS-2$
		}
		return this.map.remove(key);
	}

	@Override
	public E get(int index) {
		String key = getKey(index);
		if (key == null) {
			throw new IndexOutOfBoundsException("Index: "+index+", Size: "+size());	//$NON-NLS-1$ //$NON-NLS-2$				
		}
		return this.map.get(key);
	}

	private String getKey(int index) {
		Set<String> keys = this.map.keySet();
		int i = 0;
		for (String key:keys) {
			if (i == index) {
				return key;
			}
			i++;
		}
		return null;
	}

	@Override
	public int size() {
		return this.map.size();
	}	
}

interface KeyBuilder<E> extends Serializable {
	String getKey(E entry);
}

