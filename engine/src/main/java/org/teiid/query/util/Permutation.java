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

package org.teiid.query.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.teiid.core.util.ArgCheck;


public class Permutation {

	private Object[] items;

	public Permutation(Object[] items) {
		ArgCheck.isNotNull(items);
		this.items = items;
	}

	/**
	 * Create list of arrays of items, in all possible permutations (that's n! permutations).
	 * @return Iterator where each thing returned by the iterator is a permutation Object[]
	 */
	public Iterator<Object[]> generate() {
		return new PermutationIterator(this.items, this.items.length);
	}

	/**
	 * Create list of arrays of items, in all possible permutations (that's n! permutations).
	 * @return Iterator where each thing returned by the iterator is a permutation Object[] of
	 * length size
	 */
	public Iterator<Object[]> generate(int size) {
		if(size > items.length) {
			throw new IllegalArgumentException("Size is larger than length"); //$NON-NLS-1$
		} else if(size < 0) {
			throw new IllegalArgumentException("Size is negative"); //$NON-NLS-1$
		}

		return new PermutationIterator(this.items, size);
	}

	private static class PermutationIterator implements Iterator<Object[]> {
		// Given state
		private Object[] items;
		private int k;

		// Derived state
		private int n;
		private int[] j;
		private boolean more = true;
		private int[] last;

		private PermutationIterator(Object[] items, int k) {
			this.items = items;
			this.k = k;
			this.n = items.length;

			if(n == 0 || k == 0) {
				more = false;
			} else {
				// Initialize
				j = new int[n];
				j[0] = -1;

				// terminator
				last = new int[n];
				for(int i=n-1; i>=0; i--) {
					last[n-1-i] = i;
				}
			}
		}

		public boolean hasNext() {
			return more;
		}

		public Object[] next() {
			if(! more) {
				throw new NoSuchElementException();
			}

			if(j[0] < 0) {
				for(int i=0; i<n; i++) {
					j[i] = i;
				}
				int start = k;
				int end = n-1;
				while(start<end) {
					int t = j[start];
					j[start++] = j[end];
					j[end--] = t;
				}
			} else {
				int i;
				for(i=n-2; i >= 0 && j[i] >= j[i+1]; --i) {}

				if(i >= 0) {
					int least = i+1;
					for(int m=i+2; m<n; ++m) {
						if(j[m] < j[least] && j[m] > j[i]) {
							least = m;
						}
					}
					int t = j[i];
					j[i] = j[least];
					j[least] = t;
					if(k-1 > i) {
						int start = i+1;
						int end = n-1;
						while(start < end) {
							t = j[start];
							j[start++] = j[end];
							j[end--] = t;
						}
						start = k;
						end = n-1;
						while(start < end) {
							t = j[start];
							j[start++] = j[end];
							j[end--] = t;
						}
					}
				}
			}

			// Check for end
			more = false;
			for(int x=0; x<n; x++) {
				if(j[x] != last[x]) {
					more = true;
				}
			}

			return getPermutation(j);
		}

		private Object[] getPermutation(int[] index) {
			Object[] perm = new Object[k];
			for(int i=0; i<k; i++) {
				perm[i] = items[index[i]];
			}
			return perm;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

}

