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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.common.buffer.BatchManager.ManagedBatch;
import org.teiid.core.TeiidComponentException;

/**
 * A linked list Page entry in the tree
 */
@SuppressWarnings("unchecked")
class SPage {
	
	static final int MIN_PERSISTENT_SIZE = 16;

	static class SearchResult {
		int index;
		SPage page;
		List values;
		public SearchResult(int index, SPage page, List values) {
			this.index = index;
			this.page = page;
			this.values = values;
		}
	}
	
	private static AtomicInteger counter = new AtomicInteger();

	STree stree;
	
	protected SPage next; 
	protected ManagedBatch managedBatch;
	protected List values;
	protected ArrayList<SPage> children;
	
	SPage(STree stree, boolean leaf) {
		this.stree = stree;
		this.values = new ArrayList<SPage>(stree.pageSize);
		if (!leaf) {
			children = new ArrayList<SPage>(stree.pageSize/4);
		}
	}
	
	static SearchResult search(SPage page, List k, SearchResult parent, List parentKey) throws TeiidComponentException {
		SPage previous = null;
		List previousValues = null;
		for (;;) {
			List values = page.getValues();
			int index = Collections.binarySearch(values, k, page.stree.comparator);
			int actual = - index - 1;
			if (previous != null) {
				if (actual == 0) {
					if (values.isEmpty()) {
						page.remove();
						previous.next = page.next;
					}
					return new SearchResult(-previousValues.size() - 1, previous, previousValues);
				}
				if (parentKey != null) {
					if (page.stree.comparator.compare(parentKey, values.get(0)) >= 0) {
						//TODO: the entries after this point may also need moved forward
						//TODO: this should be done as part of insert
						parent.page.children.set(Math.max(0, -parent.index - 2), page);
					} else {
						//parentKey < page.keys.get(0)
						//TODO: this circumvents the normal probabilistic process, but
						//ensures there is an index entry.
						page.stree.insert(page.stree.extractKey((List) values.get(0)), parent, page);
						parent.index--;
					}
				}
			}
			if (actual != values.size() || page.next == null) {
				return new SearchResult(index, page, values);
			}
			previous = page;
			previousValues = values; 
			page = page.next;
		}
	}
	
	protected void setValues(List values) throws TeiidComponentException {
		if (managedBatch != null) {
			managedBatch.remove();
		}
		if (values.size() < MIN_PERSISTENT_SIZE) {
			this.values = values;
			return;
		} 
		this.values = null;
		TupleBatch batch = TupleBatch.directBatch(counter.getAndIncrement(), values);
		if (children != null) {
			batch.setDataTypes(stree.keytypes);
		} else {
			batch.setDataTypes(stree.types);
		}
		managedBatch = stree.manager.createManagedBatch(batch);
	}
	
	protected void remove() {
		if (managedBatch != null) {
			managedBatch.remove();
			managedBatch = null;
		}
	}

	protected List getValues() throws TeiidComponentException {
		if (values != null) {
			return values;
		}
		if (managedBatch == null) {
			return Collections.emptyList();
		}
		TupleBatch batch = managedBatch.getBatch(true, stree.types);
		return batch.getTuples();
	}

}
