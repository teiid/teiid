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

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.teiid.common.buffer.BatchManager.CleanupHook;
import org.teiid.common.buffer.BatchManager.ManagedBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;

/**
 * A linked list Page entry in the tree
 * 
 * TODO: return the tuplebatch from getvalues, since that is what we're tracking
 * 
 * State cloning allows a single storage reference to be shared in many trees.
 * A phantom reference is used for proper cleanup once cloned.
 * 
 * TODO: a better purging strategy for managedbatchs.
 * 
 */
@SuppressWarnings("unchecked")
class SPage implements Cloneable {
	
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
	
	private static final Set<PhantomReference<Object>> REFERENCES = Collections.newSetFromMap(new IdentityHashMap<PhantomReference<Object>, Boolean>());
	private static ReferenceQueue<Object> QUEUE = new ReferenceQueue<Object>();
	static class CleanupReference extends PhantomReference<Object> {
		
		private CleanupHook batch;
		
		public CleanupReference(Object referent, CleanupHook batch) {
			super(referent, QUEUE);
			this.batch = batch;
		}
		
		public void cleanup() {
			try {
				this.batch.cleanup();
			} finally {
				this.clear();
			}
		}
	}

	private static AtomicLong counter = new AtomicLong();

	STree stree;
	
	private long id;
	protected SPage next;
	protected SPage prev;
	protected ManagedBatch managedBatch;
	private Object trackingObject;
	protected TupleBatch values;
	protected ArrayList<SPage> children;
	protected boolean cloned; 
	
	SPage(STree stree, boolean leaf) {
		this.stree = stree;
		this.id = counter.getAndIncrement();
		stree.pages.put(this.id, this);
		this.values = new TupleBatch(0, new ArrayList(stree.pageSize/4));
		if (!leaf) {
			children = new ArrayList<SPage>(stree.pageSize/4);
		}
	}
	
	public SPage clone(STree tree) {
		try {
			if (this.managedBatch != null && trackingObject == null) {
				cloned = true;
				this.trackingObject = new Object();
				CleanupReference managedBatchReference  = new CleanupReference(trackingObject, managedBatch.getCleanupHook());
				REFERENCES.add(managedBatchReference);
			}
			SPage clone = (SPage) super.clone();
			clone.stree = tree;
			if (children != null) {
				clone.children = new ArrayList<SPage>(children);
			}
			if (values != null) {
				clone.values = new TupleBatch(0, new ArrayList<List<?>>(values.getTuples()));
			}
			return clone;
		} catch (CloneNotSupportedException e) {
			throw new TeiidRuntimeException(e);
		}
	}
	
	public long getId() {
		return id;
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
					page.stree.updateLock.lock();
					try {
						index = Collections.binarySearch(values.getTuples(), k, page.stree.comparator);
						if (index != 0) {
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
					} finally {
						page.stree.updateLock.unlock();
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
		if (managedBatch != null && !cloned) {
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
		if (cloned) {
			cloned = false;
			trackingObject = null;
		}
		if (children != null) {
			managedBatch = stree.keyManager.createManagedBatch(values, true);
		} else {
			managedBatch = stree.leafManager.createManagedBatch(values, stree.preferMemory);
		}
	}

	protected void remove(boolean force) {
		if (managedBatch != null) {
			if (force || !cloned) {
				managedBatch.remove();
			}
			managedBatch = null;
			trackingObject = null;
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
		for (int i = 0; i < 10; i++) {
			CleanupReference ref = (CleanupReference)QUEUE.poll();
			if (ref == null) {
				break;
			}
			REFERENCES.remove(ref);
			ref.cleanup();
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
		current.next.remove(false);
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
