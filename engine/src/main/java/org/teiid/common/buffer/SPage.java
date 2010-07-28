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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.common.buffer.BatchManager.ManagedBatch;
import org.teiid.core.TeiidComponentException;

/**
 * A linked list Page entry in the tree
 * 
 * TODO: return the tuplebatch from getvalues, since that is what we're tracking
 * 
 */
@SuppressWarnings("unchecked")
class SPage {
	
	static final int MIN_PERSISTENT_SIZE = 16;

	static class SearchResult {
		int index;
		SPage page;
		TupleBatch values;
		public SearchResult(int index, SPage page, TupleBatch values) {
			this.index = index;
			this.page = page;
			this.values = values;
		}
	}
	
	private static AtomicInteger counter = new AtomicInteger();

	STree stree;
	
	protected SPage next;
	protected SPage prev;
	protected ManagedBatch managedBatch;
	protected TupleBatch values;
	protected ArrayList<SPage> children;
	
	SPage(STree stree, boolean leaf) {
		this.stree = stree;
		this.values = new TupleBatch(counter.getAndIncrement(), new ArrayList(stree.pageSize/4));
		if (!leaf) {
			children = new ArrayList<SPage>(stree.pageSize/4);
		}
	}
	
	static SearchResult search(SPage page, List k, LinkedList<SearchResult> parent) throws TeiidComponentException {
		TupleBatch previousValues = null;
		for (;;) {
			TupleBatch values = page.getValues();
			int index = Collections.binarySearch(values.getTuples(), k, page.stree.comparator);
			int flippedIndex = - index - 1;
			if (previousValues != null) {
				if (flippedIndex == 0) {
					//systemic weakness of the algorithm
					return new SearchResult(-previousValues.getTuples().size() - 1, page.prev, previousValues);
				}
				if (parent != null && index != 0) {
					//for non-matches move the previous pointer over to this page
					SPage childPage = page;
					List oldKey = null;
					List newKey = page.stree.extractKey(values.getTuples().get(0));
					for (Iterator<SearchResult> desc = parent.descendingIterator(); desc.hasNext();) {
						SearchResult sr = desc.next();
						int parentIndex = Math.max(0, -sr.index - 2);
						if (oldKey == null) {
							oldKey = sr.values.getTuples().set(parentIndex, newKey); 
						} else if (page.stree.comparator.compare(oldKey, sr.values.getTuples().get(parentIndex)) == 0 ) {
							sr.values.getTuples().set(parentIndex, newKey);
						} else {
							break;
						}
						sr.page.children.set(parentIndex, childPage);
						sr.page.setValues(sr.values);
						childPage = sr.page;
					}
				}
			}
			if (flippedIndex != values.getTuples().size() || page.next == null) {
				return new SearchResult(index, page, values);
			}
			previousValues = values; 
			page = page.next;
		}
	}
	
	protected void setValues(TupleBatch values) throws TeiidComponentException {
		if (managedBatch != null) {
			managedBatch.remove();
		}
		if (values.getTuples().size() < MIN_PERSISTENT_SIZE) {
			this.values = values;
			return;
		} 
		this.values = null;
		if (children != null) {
			values.setDataTypes(stree.keytypes);
		} else {
			values.setDataTypes(stree.types);
		}
		if (children != null) {
			managedBatch = stree.keyManager.createManagedBatch(values);
		} else {
			managedBatch = stree.leafManager.createManagedBatch(values);
		}
	}
	
	protected void remove() {
		if (managedBatch != null) {
			managedBatch.remove();
			managedBatch = null;
		}
		values = null;
		children = null;
	}

	protected TupleBatch getValues() throws TeiidComponentException {
		if (values != null) {
			return values;
		}
		if (managedBatch == null) {
			throw new AssertionError("Batch removed"); //$NON-NLS-1$
		}
		if (children != null) {
			return managedBatch.getBatch(true, stree.keytypes);
		}
		return managedBatch.getBatch(true, stree.types);
	}
	
	static void merge(LinkedList<SearchResult> places, TupleBatch nextValues, SPage current, TupleBatch currentValues)
	throws TeiidComponentException {
		SearchResult parent = places.peekLast();
		if (parent != null) {
			correctParents(parent.page, nextValues.getTuples().get(0), current.next, current);
		}
		currentValues.getTuples().addAll(nextValues.getTuples());
		if (current.children != null) {
			current.children.addAll(current.next.children);
		}
		current.next.remove();
		current.next = current.next.next;
		if (current.next != null) {
			current.next.prev = current;
		}
		current.setValues(currentValues);
	}

	/**
	 * Remove the usage of page in favor of nextPage
	 * @param parent
	 * @param page
	 * @param nextPage
	 * @throws TeiidComponentException
	 */
	static void correctParents(SPage parent, List key, SPage page, SPage nextPage) throws TeiidComponentException {
		SearchResult location = SPage.search(parent, key, null);
		while (location.index == -1 && location.page.prev != null ) {
			parent = location.page.prev;
			location = SPage.search(parent, key, null);
		}
		parent = location.page;
		int index = location.index;
		if (index < 0) {
			index = -index - 1;
		}
		while (parent != null) {
			while (index < parent.children.size()) {
				if (parent.children.get(index) != page) {
					return;
				}
				parent.children.set(index++, nextPage);
			}
			index = 0;
			parent = parent.next;
		}
	}
	
	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		try {
			TupleBatch tb = getValues();
			result.append(tb.getBeginRow());
			if (children == null) {
				if (tb.getTuples().size() <= 1) {
					result.append(tb.getTuples());
				} else {
					result.append("[").append(tb.getTuples().get(0)).append(" . ").append(tb.getTuples().size()). //$NON-NLS-1$ //$NON-NLS-2$
					append(" . ").append(tb.getTuples().get(tb.getTuples().size() - 1)).append("]"); //$NON-NLS-1$ //$NON-NLS-2$ 
				}
			} else {
				result.append("["); //$NON-NLS-1$
				for (int i = 0; i < children.size(); i++) {
					result.append(tb.getTuples().get(i)).append("->").append(children.get(i).getValues().getBeginRow()); //$NON-NLS-1$
					if (i < children.size() - 1) {
						result.append(", "); //$NON-NLS-1$
					}
				}
				result.append("]");//$NON-NLS-1$
			}
		} catch (Throwable e) {
			result.append("Removed"); //$NON-NLS-1$
		}
		return result.toString();
	}

}
