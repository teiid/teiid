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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.common.buffer.SPage.SearchResult;
import org.teiid.core.TeiidComponentException;

@SuppressWarnings("unchecked")

/**
 * Self balancing search tree using skip list like logic
 * This has similar performance similar to a B+/-Tree 
 * 
 * TODO: reserve additional memory for delete/update operations
 * TODO: double link to support desc key access
 */
public class STree {

	private static final Random seedGenerator = new Random();

	private int randomSeed;
	private SPage[] header = new SPage[] {new SPage(this, true)};
    protected BatchManager manager;
    protected Comparator comparator;
    protected int pageSize;
    protected int keyLength;
    protected String[] types;
    protected String[] keytypes;
    
    private AtomicInteger rowCount = new AtomicInteger();
	
	public STree(BatchManager recman,
            final Comparator comparator,
            int pageSize,
            int keyLength,
            String[] types) {
		randomSeed = seedGenerator.nextInt() | 0x00000100; // ensure nonzero
		this.manager = recman;
		this.comparator = comparator;
		this.pageSize = Math.max(pageSize, SPage.MIN_PERSISTENT_SIZE);
		this.keyLength = keyLength;
		this.types = types;
		this.keytypes = Arrays.copyOf(types, keyLength);
	}

	/**
	 * Determine a new random level using an XOR rng.
	 * 
	 * This uses the simplest of the generators described in George
     * Marsaglia's "Xorshift RNGs" paper.  This is not a high-quality
     * generator but is acceptable here.
	 * 
	 * See also the JSR-166 working group ConcurrentSkipListMap implementation.
	 * 
	 * @return
	 */
    private int randomLevel() {
        int x = randomSeed;
        x ^= x << 13;
        x ^= x >>> 17;
        randomSeed = x ^= x << 5;
        int level = 0;
        int shift = 8;
        while ((x & 0xff) == 0xff) { 
        	++level;
        	x >>>= shift;
        }
        return level;
    }
    
	/**
	 * Search each level to find the pointer to the next level
	 * @param n
	 * @param places
	 * @return
	 * @throws IOException
	 * @throws TeiidComponentException 
	 */
	private List find(List n, List<SearchResult> places) throws TeiidComponentException {
		SPage x = null;
		List parentKey = null;
		SearchResult parent = null;
		for (int i = header.length - 1; i >= 0; i--) {
			if (x == null) {
				x = header[i];
			}
			SearchResult s = SPage.search(x, n, parent, parentKey);
			if (places != null) {
				places.add(s);
			}
			if ((s.index == -1 && s.page == header[i]) || s.values.isEmpty()) {
				x = null;
				continue; //start at the beginning of the next level
			}
			x = s.page;
			int index = s.index;
			boolean matched = true;
			if (index < 0) {
				matched = false;
				index = Math.max(0, -index - 2);
			}
			if (i == 0) {
				if (!matched) {
					return null;
				}
				return (List) s.values.get(index);
			}
			parentKey = (List) s.values.get(index);
			parent = s;
			x = x.children.get(index);
		}
		return null;
	}
	
	public List find(List k) throws TeiidComponentException {
		return find(k, null);
	}
	
	public List insert(List tuple, boolean replace) throws TeiidComponentException {
		LinkedList<SearchResult> places = new LinkedList<SearchResult>();
		List match = find(tuple, places);
		if (match != null) {
			if (!replace) {
				return match;
			}
			SearchResult last = places.getLast();
			SPage page = last.page;
			last.values.set(last.index, tuple);
			page.setValues(last.values);
			return match;
		}
		List key = extractKey(tuple);
		int level = randomLevel(); 
		assert header.length == places.size();
		if (level >= header.length) {
			header = Arrays.copyOf(header, level + 1);
		}
		rowCount.addAndGet(1);
		SPage page = null;
		for (int i = 0; i <= level; i++) {
			if (places.isEmpty()) {
				SPage newHead = new SPage(this, false);
				ArrayList newValues = new ArrayList();
				newValues.add(key);
				newHead.setValues(newValues);
				newHead.children.add(page);
				header[i] = newHead;
				page = newHead;
			} else {
				SearchResult result = places.removeLast();
				Object value = (i == 0 ? tuple : page);
				page = insert(key, result, value);
			}
		}
		return null;
	}
	
	List extractKey(List tuple) {
		return tuple.subList(0, keyLength);
	}

	SPage insert(List k, SearchResult result, Object value) throws TeiidComponentException {
		SPage page = result.page;
		int index = -result.index - 1;
		if (result.values.size() == pageSize) {
			boolean leaf = !(value instanceof SPage);
			SPage nextPage = new SPage(this, leaf);
			ArrayList nextValues = new ArrayList(result.values.subList(pageSize/2, pageSize));
			result.values.subList(pageSize/2, pageSize).clear();
			if (!leaf) {
				nextPage.children.addAll(page.children.subList(pageSize/2, pageSize));
				page.children.subList(pageSize/2, pageSize).clear();
			}
			nextPage.next = page.next;
			page.next = nextPage;
			boolean inNext = false;
			if (index <= pageSize/2) {
				setValue(index, k, value, result.values, page);
			} else {
				inNext = true;
				setValue(index - pageSize/2, k, value, nextValues, nextPage);
			}
			nextPage.setValues(nextValues);
			page.setValues(result.values);
			if (inNext) {
				page = nextPage;
			}
		} else {
			setValue(index, k, value, result.values, page);
			page.setValues(result.values);
		}
		return page;
	}
	
	static void setValue(int index, List key, Object value, List values, SPage page) {
		if (value instanceof SPage) {
			values.add(index, key);
			page.children.add(index, (SPage) value);
		} else {
			values.add(index, value);
		}
	}
	
	public List remove(List key) throws TeiidComponentException {
		LinkedList<SearchResult> places = new LinkedList<SearchResult>();
		List tuple = find(key, places);
		if (tuple == null) {
			return null;
		}
		rowCount.addAndGet(-1);
		for (int i = header.length -1; i >=0; i--) {
			SearchResult searchResult = places.remove();
			if (searchResult.index < 0) {
				continue;
			}
			boolean cleanup = false;
			searchResult.values.remove(searchResult.index);
			int size = searchResult.values.size();
			if (size == 0) {
				if (header[i] == searchResult.page && (i != 0 || header[i].next != null)) {
					header[i].remove();
					header[i] = header[i].next;
					if (header[i] == null) {
						//remove the layer
						header = Arrays.copyOf(header, header.length - 1);
					}
				} else if (i == 0 && header.length > 1) {
					cleanup = true;
				}
			} else if (searchResult.page.next != null && size < pageSize/4) {
				List nextValues = searchResult.page.next.getValues();
				SPage next = searchResult.page.next;
				if (nextValues.size() + size < pageSize/2) {
					searchResult.page.next = next.next;
					searchResult.values.addAll(nextValues);
					nextValues.clear();
					next.remove();
					//any references to toMerge are now invalid
					//setting back to the header will self heal
					//TODO: this can take advantage of a previous link
					next.next = header[i];
				}
			}
			searchResult.page.setValues(searchResult.values);
			if (cleanup) {
				find(key, null); //trigger cleanup
			}
		}
		return tuple;
	}
	
	public void remove() {
		truncate();
		this.manager.remove();
	}

	public int getRowCount() {
		return this.rowCount.get();
	}
	
	public TupleBrowser browse(List lowerBound, List upperBound, boolean direction) {
		return new TupleBrowser();
	}

	public int truncate() {
		int oldSize = rowCount.getAndSet(0);
		for (int i = 0; i < header.length; i++) {
			SPage page = header[i];
			while (page != null) {
				page.remove();
				page = page.next;
			}
		}
		header = new SPage[] {new SPage(this, true)};
		return oldSize;
	}
	
	//TODO: support update/delete from the browser
	public class TupleBrowser {
		
		SPage page = header[0];
		int index;
		List values;
		
		public boolean matchedLower() {
			return false;
		}
		
		public boolean matchedUpper() {
			return false;
		}
		
		public List next() throws TeiidComponentException {
			for (;;) {
				if (values == null) {
					values = page.getValues();
				}
				if (index < values.size()) {
					return (List) values.get(index++);
				}
				index = 0;
				page = page.next;
				if (page == null) {
					return null;
				}
			}
		}
	}
	
	public int getKeyLength() {
		return keyLength;
	}
	
}