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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.teiid.common.buffer.SPage.SearchResult;
import org.teiid.core.TeiidComponentException;

/**
 * Implements intelligent browsing over a {@link STree}
 * 
 * TODO: when using other values we can be more efficient 
 * with paging.
 */
public class TupleBrowser {
	
	private final STree tree;
	
	private List<List<?>> otherValues;
	
	private SPage page;
	private int index;
	
	private SPage bound;
	private int boundIndex = -1;
	
	private TupleBatch values;
	private boolean updated;
	private boolean direction;
	private boolean range;
	
	public TupleBrowser(STree sTree, List<?> lowerBound, List<?> upperBound, List<List<?>> otherValues, boolean range, boolean direction) throws TeiidComponentException {
		this.tree = sTree;
		this.direction = direction;
		this.otherValues = otherValues;
		this.range = range;
		
		if (lowerBound != null) {
			setPage(lowerBound);
		} else {
			page = sTree.header[0];
		}
		
		boolean valid = true;
		
		if (upperBound != null) {
			if (this.tree.comparator.compare(upperBound, lowerBound) < 0) {
				valid = false;
			}
			LinkedList<SearchResult> places = new LinkedList<SearchResult>();
			this.tree.find(upperBound, places);
			SearchResult upper = places.getLast();
			bound = upper.page;
			boundIndex = upper.index;
			if (boundIndex < 0) {
				boundIndex = Math.min(upper.values.getTuples().size() - 1, -boundIndex -1);
			}
			if (!direction) {
				values = upper.values;
			}
		} else if (range) {
			//this is a range query
			//TODO: this could also be signaled with an all null key
			bound = tree.header[0];
			while (bound.next != null) {
				bound = bound.next;
			}
		} else {
			bound = page;
			boundIndex = index;
		}
				
		if (!direction) {
			SPage swap = page;
			page = bound;
			bound = swap;
			int upperIndex = boundIndex;
			boundIndex = index;
			index = upperIndex;
		}
		
		if (!valid) {
			page = null;
		}
	}

	private void setPage(List<?> lowerBound) throws TeiidComponentException {
		if (values != null) {
			int possibleIndex = Collections.binarySearch(values.getTuples(), lowerBound, tree.comparator);
			if (possibleIndex != -1 && possibleIndex != -values.getTuples().size() -1) {
				index = possibleIndex;
				if (possibleIndex < 0) {
					index = -index -1;
				}
				return;
			}
		}
		resetState();
		LinkedList<SearchResult> places = new LinkedList<SearchResult>();
		this.tree.find(lowerBound, places);
		
		SearchResult sr = places.getLast();
		page = sr.page;
		index = sr.index;
		if (index < 0) {
			index = -index - 1;
		}
		values = sr.values;
	}
	
	public List<?> next() throws TeiidComponentException {
		for (;;) {
			if (page == null) {
				return null;
			}
			if (values == null) {
				values = page.getValues();
				if (direction) {
					index = 0;
				} else {
					index = values.getTuples().size() - 1;
				}
			}
			if (index >= 0 && index < values.getTuples().size()) {
				List<?> result = values.getTuples().get(index);
				if (page == bound && index == boundIndex) {
					resetState();
					page = null; //terminate
				} else if (otherValues != null && !range) {
					if (!otherValues.isEmpty()) {
						List newBound = direction?otherValues.remove(0):otherValues.remove(otherValues.size() -1);
						setPage(newBound);
					} else {
						otherValues = null;
						if (page != bound) {
							resetState();
						}
						page = bound;
						index = boundIndex;
						values = bound.values;
					}
				} else {
					index+=getOffset();
				}
				return result;
			}
			resetState();
			if (direction) {
				page = page.next;
			} else {
				page = page.prev;
			}
		}
	}

	private void resetState() throws TeiidComponentException {
		if (updated) {
			page.setValues(values);
		}
		updated = false;
		values = null;
	}
	
	private int getOffset() {
		return direction?1:-1;
	}
	
	/**
	 * Perform an in-place update of the tuple just returned by the next method
	 * WARNING - this must not change the key value
	 * @param tuple
	 * @throws TeiidComponentException
	 */
	public void update(List<?> tuple) throws TeiidComponentException {
		values.getTuples().set(index - getOffset(), tuple);
		updated = true;
	}
	
	/**
	 * Notify the browser that the last value was deleted.
	 */
	public void removed() {
		index-=getOffset();
	}
}