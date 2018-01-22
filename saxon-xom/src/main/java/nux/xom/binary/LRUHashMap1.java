/*
 * Copyright (c) 2005, The Regents of the University of California, through
 * Lawrence Berkeley National Laboratory (subject to receipt of any required
 * approvals from the U.S. Dept. of Energy). All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * (1) Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 
 * (2) Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * 
 * (3) Neither the name of the University of California, Lawrence Berkeley
 * National Laboratory, U.S. Dept. of Energy nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 * You are under no obligation whatsoever to provide any bug fixes, patches, or
 * upgrades to the features, functionality or performance of the source code
 * ("Enhancements") to anyone; however, if you choose to make your Enhancements
 * available either publicly, or directly to Lawrence Berkeley National
 * Laboratory, without imposing a separate written license agreement for such
 * Enhancements, then you hereby grant the following license: a non-exclusive,
 * royalty-free perpetual license to install, use, modify, prepare derivative
 * works, incorporate into other computer software, distribute, and sublicense
 * such enhancements or derivative works thereof, in binary and source code
 * form.
 */
package nux.xom.binary;

/**
 * Bounded LinkedHashMap with least-recently-used (LRU) eviction policy.
 * Typically used to implement efficient caches. Null keys are not permitted,
 * null values are permitted but discouraged. Runs on any JDK, including
 * historic ones such as JDK 1.2.
 * 
 * @author whoschek.AT.lbl.DOT.gov
 * @author $Author: hoschek3 $
 * @version $Revision: 1.2 $, $Date: 2005/12/10 01:05:49 $
 */
final class LRUHashMap1 { // not a public class
	
	private static final float LOAD_FACTOR = 0.75f;
//	private static final float LOAD_FACTOR = 0.3f;
	private static final int INITIAL_CAPACITY = 16;
	private Entry[] entries = new Entry[INITIAL_CAPACITY];
	private int threshold = (int) (INITIAL_CAPACITY * LOAD_FACTOR);
	private int size = 0;
	private final int maxSize;
	private final Entry header;
	
	/**
	 * Constructs a map that can hold at most <code>maxSize</code>
	 * associations, removing old associations beyond that point according to a
	 * least-recently-used (LRU) eviction policy.
	 */
	LRUHashMap1(int maxSize) {
		this.maxSize = maxSize;
		this.header = new Entry(null, -1, null, null);
		header.before = header.after = header;
	}
	
	/** Removes all entries, retaining the current capacity. */
	public void clear() {
		size = 0;
		header.before = header.after = header;
		Entry[] src = entries;
		for (int i=src.length; --i >= 0; ) src[i] = null;
	}
	
	/**
	 * Returns the value associated with the given key, or null if there is no
	 * such association.
	 */
	public Object get(String key) {
		int hash = hash(key);
		int i = hash & (entries.length - 1);
		Entry entry = findEntry(key, entries[i], hash);
		if (entry == null) return null;
		entry.remove();
		entry.insert(header);
		return entry.value;
	}
	
	/** Associates the given value with the given key. */
	public void put(String key, Object value) {
		int hash = hash(key);
		int i = hash & (entries.length - 1);
		Entry entry = findEntry(key, entries[i], hash);
		if (entry != null) {
			entry.value = value;
			entry.remove();
			entry.insert(header);
		} else {		
			entries[i] = new Entry(key, hash, entries[i], value);
			entries[i].insert(header);
			size++;
			if (size > maxSize) { // remove eldest (least recently used) entry
				removeEntry(header.after.key);
			}
			if (size >= threshold) { // expand table capacity
				rehash();
			}
		}
	}
	
	/** Returns the current number of associations. */
	public int size() {
		return size;
	}
	
	private static Entry findEntry(String key, Entry cursor, int hash) {
		while (cursor != null) { // scan collision chain
			if (hash == cursor.hash && eq(key, cursor.key)) { 
//				cursor.key = key; // speeds up future lookups: equals() vs. ==
				return cursor;
			}
			cursor = cursor.next;
		}
		return null;		
	}
	
	private void removeEntry(String key) {
		int hash = hash(key);
		int i = hash & (entries.length - 1);
		Entry previous = null;
		Entry entry = entries[i];
		while (entry != null) { // scan collision chain
			if (hash == entry.hash && eq(key, entry.key)) {
				if (previous == null) {
					entries[i] = entry.next;
				} else {
					previous.next = entry.next;
				}
				size--;
				entry.remove();
				return;
			}
			previous = entry;
			entry = entry.next;
		}
	}
	
	/**
	 * Expands the capacity of this table, rehashing all entries into
	 * corresponding new slots.
	 */
	private void rehash() {
		Entry[] src = entries;
		int capacity = 2 * src.length;
		Entry[] dst = new Entry[capacity];
		
		for (int i = src.length; --i >= 0; ) {
			Entry entry = src[i];
			while (entry != null) { // walk collision chain
				int j = entry.hash & (capacity - 1);
				Entry next = entry.next;
				entry.next = dst[j];
				dst[j] = entry; // insert entry at head of chain
				entry = next;
			}
		}
		entries = dst;
		threshold = (int) (capacity * LOAD_FACTOR);
	}

	private static boolean eq(String x, String y) {
		return x == y || x.equals(y);
	}
		
	private static int hash(String key) {
		return auxiliaryHash(key.hashCode());
	}

	/**
	 * Auxiliary hash function that defends against poor base hash
	 * functions. Ensures more uniform hash distribution, hence reducing the
	 * probability of pathologically long collision chains, in particular
	 * for short key symbols that are quite similar to each other, or XML 
	 * boundary whitespace (worst case scenario).
	 */
	private static int auxiliaryHash(int h) {
		h += ~(h << 9);
		h ^= (h >>> 14);
		h += (h << 4);
		h ^= (h >>> 10);
		return h;
	}


	///////////////////////////////////////////////////////////////////////////////
	// Nested classes:
	///////////////////////////////////////////////////////////////////////////////
	
	/**
	 * LRU hash table entry. Maintains a linked list in least-recently-used to
	 * most-recently-used order.
	 */
	private static final class Entry {

		String key; 
		Object value;
		final int hash; // cache 
		Entry next; // successor in collision chain, mapping to the same hash slot	
		Entry before, after; // LRU linked list pointers

		Entry(String key, int hash, Entry next, Object value) {
			this.key = key;
			this.hash = hash;
			this.next = next;
			this.value = value;
		}

		/** Removes entry from LRU list. */
		void remove() {
			before.after = after;
			after.before = before;
		}

		/** Inserts entry before the given successor in the LRU list. */
		void insert(Entry successor) {
			after = successor;
			before = successor.before;
			before.after = this;
			after.before = this;
		}
		
	}
}
