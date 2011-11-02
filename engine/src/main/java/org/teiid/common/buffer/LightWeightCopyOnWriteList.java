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

package org.teiid.common.buffer;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.RandomAccess;

import org.teiid.client.ResizingArrayList;

/**
 * Creates a copy of a reference list when modified.
 * 
 * @param <T>
 */
public class LightWeightCopyOnWriteList<T> extends AbstractList<T> implements RandomAccess {

	private List<T> list;
	private boolean modified;
	
	public LightWeightCopyOnWriteList(List<T> list) {
		this.list = list;
	}
	
	@Override
	public T get(int index) {
		return list.get(index);
	}
	
	public List<T> getList() {
		return list;
	}
	
	public void add(int index, T element) {
		if (!modified) {
			List<T> next = new ArrayList<T>(list.size() + 1);
			next.addAll(list);
			list = next;
			modified = true;
		}
		list.add(index, element);
	}
	
	public T set(int index, T element) {
		checkModified();
		return list.set(index, element);
	}

	private void checkModified() {
		if (!modified) {
			list = new ArrayList<T>(list);
			modified = true;
		}
	}
	
	public boolean addAll(Collection<? extends T> c) {
		return addAll(size(), c);
	}
	
	@Override
	public boolean addAll(int index, Collection<? extends T> c) {
		checkModified();
		return list.addAll(index, c);
	}

	@Override
	public T remove(int index) {
		checkModified();
		return list.remove(index);
	}
	
	@Override
	public Object[] toArray() {
		return list.toArray();
	}
	
	public <U extends Object> U[] toArray(U[] a) {
		return list.toArray(a);
	}
	
	@Override
	public void clear() {
		if (!modified) {
			list = new ResizingArrayList<T>();
			modified = true;
		} else {
			list.clear();
		}
	}

	@Override
	public int size() {
		return list.size();
	}

}
