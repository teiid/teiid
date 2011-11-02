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

package org.teiid.client;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.RandomAccess;

/**
 * Modified {@link ArrayList} that resizes up and down by powers of 2.
 * 
 * @param <T>
 */
@SuppressWarnings("unchecked")
public class ResizingArrayList<T> extends AbstractList<T> implements RandomAccess {

	public static final int MIN_SHRINK_SIZE = 32;
	
	protected Object[] elementData;
	protected int size;
	
	public ResizingArrayList() {
		this(MIN_SHRINK_SIZE);
	}
	
	public ResizingArrayList(int initialCapacity) {
		this.elementData = new Object[initialCapacity];
	}
	
    public ResizingArrayList(Collection<? extends T> c) {
    	elementData = c.toArray();
    	size = elementData.length;
    	// c.toArray might (incorrectly) not return Object[] (see 6260652)
    	if (elementData.getClass() != Object[].class)
    	    elementData = Arrays.copyOf(elementData, size, Object[].class);
    }
	
	@Override
	public T get(int index) {
		rangeCheck(index, false);
		return (T) elementData[index];
	}
	
	public int getModCount() {
		return modCount;
	}
	
	public void add(int index, T element) {
		rangeCheck(index, true);
		modCount++;
		ensureCapacity(size+1); 
		System.arraycopy(elementData, index, elementData, index + 1,
				 size - index);
		elementData[index] = element;
		size++;
	}
	
	protected void ensureCapacity(int capacity) {
		if (capacity <= elementData.length) {
			return;
		}
	    int newCapacity = 1 << (32 - Integer.numberOfLeadingZeros(capacity - 1));
	    int lowerCapacity = newCapacity*70/99; //SQRT(2)
	    if (lowerCapacity > capacity) {
	    	newCapacity = lowerCapacity;
	    }
        elementData = Arrays.copyOf(elementData, newCapacity);
	}

	public T set(int index, T element) {
		rangeCheck(index, false);
		T old = (T) elementData[index];
		elementData[index] = element;
		return old;
	}
	
	@Override
	public boolean addAll(Collection<? extends T> c) {
		return addAll(size, c);
	}

	@Override
	public boolean addAll(int index, Collection<? extends T> c) {
		rangeCheck(index, true);
		modCount++;
        int numNew = c.size();
        ensureCapacity(size + numNew);
        for (T t : c) {
			elementData[index++] = t;
		}
        size += numNew;
        return numNew != 0;
	}

	@Override
	public T remove(int index) {
		T oldValue = get(index);
		modCount++;
		int numMoved = size - index - 1;
		if (numMoved > 0) {
		    System.arraycopy(elementData, index+1, elementData, index, numMoved);
		}
		elementData[--size] = null;
		int halfLength = elementData.length/2;
		if (size <= halfLength && elementData.length > MIN_SHRINK_SIZE) {
			int newSize = Math.max(halfLength*99/70, MIN_SHRINK_SIZE);
			Object[] next = new Object[newSize];
		    System.arraycopy(elementData, 0, next, 0, size);
		    elementData = next;
		}
		return oldValue;
	}
	
    private void rangeCheck(int index, boolean inclusive) {
		if (index > size || (!inclusive && index == size)) {
		    throw new IndexOutOfBoundsException("Index: "+index+", Size: "+size); //$NON-NLS-1$ //$NON-NLS-2$
		}
    }
	
	@Override
	public void clear() {
		modCount++;
		if (size <= MIN_SHRINK_SIZE) {
			for (int i = 0; i < size; i++) {
				elementData[i] = null;
			}
		} else {
			elementData = new Object[MIN_SHRINK_SIZE];
		}
		size = 0;
	}
	
	@Override
	public Object[] toArray() {
		return Arrays.copyOf(elementData, size);
	}
	
	public <U extends Object> U[] toArray(U[] a) {
		if (a.length < size) {
			return (U[]) Arrays.copyOf(elementData, size, a.getClass());
		}
		System.arraycopy(elementData, 0, a, 0, size);
        if (a.length > size) {
            a[size] = null;
        }
        return a;
	}

	@Override
	public int size() {
		return size;
	}

}
