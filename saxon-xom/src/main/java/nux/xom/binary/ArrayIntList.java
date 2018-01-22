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
 * Efficient resizable auto-expanding list holding <code>int</code> elements;
 * implemented with arrays.
 * 
 * @author whoschek.AT.lbl.DOT.gov
 * @author $Author: hoschek3 $
 * @version $Revision: 1.8 $, $Date: 2006/01/08 01:00:08 $
 */
public final class ArrayIntList {

	/**
	 * The array into which the elements of the list are stored. The
	 * capacity of the list is the length of this array.
	 */
	private transient int[] elements;

	/**
	 * The current number of elements contained in this list.
	 */
	private int size;

	/**
	 * Constructs an empty list.
	 */
	public ArrayIntList() {
		this(10);
	}

	/**
	 * Constructs an empty list with the specified initial capacity.
	 * 
	 * @param initialCapacity
	 *            the number of elements the receiver can hold without
	 *            auto-expanding itself by allocating new internal memory.
	 */
	public ArrayIntList(int initialCapacity) {
		elements = new int[initialCapacity];
		size = 0;
	}


	/**
	 * Appends the specified element to the end of this list.
	 * 
	 * @param elem
	 *            element to be appended to this list.
	 */
	public void add(int elem) {
		if (size == elements.length) ensureCapacity(size + 1);
		elements[size++] = elem;
	}
	
	/**
	 * Appends the elements in the range <code>[offset..offset+length)</code> to the end of this list.
	 * 
	 * @param elems the elements to be appended
	 * @param offset the offset of the first element to add (inclusive)
	 * @param length the number of elements to add
	 * @throws IndexOutOfBoundsException if indexes are out of range.
	 */
	public void add(int[] elems, int offset, int length) {
		if (offset < 0 || length < 0 || offset + length > elems.length) 
			throw new IndexOutOfBoundsException("offset: " + offset + 
				", length: " + length + ", elems.length: " + elems.length);

		ensureCapacity(size + length);
		System.arraycopy(elems, offset, this.elements, size, length);
		size += length;
	}

	/**
	 * Returns the elements currently stored, including invalid elements between
	 * size and capacity, if any.
	 * <p>
	 * <b>WARNING: </b> For efficiency reasons and to keep memory usage low,
	 * <b>the array is SHARED, not copied </b>. So if subsequently you modify the
	 * returned array directly via the [] operator, be sure you know what you're
	 * doing.
	 * 
	 * @return the elements currently stored.
	 */
	public int[] asArray() {
		return elements;
	}

	/**
	 * Removes all elements but keeps the current capacity; Afterwards
	 * <code>size()</code> will yield zero.
	 */
	public void clear() {
		size = 0;
	}
		
	/**
	 * Ensures that the receiver can hold at least the specified number of
	 * elements without needing to allocate new internal memory. If necessary,
	 * allocates new internal memory and increases the capacity of the receiver.
	 * 
	 * @param minCapacity
	 *            the desired minimum capacity.
	 */
	public void ensureCapacity(int minCapacity) {
		if (minCapacity > elements.length) {
			int newCapacity = Math.max(minCapacity, 2 * elements.length + 1);
			elements = subArray(0, size, newCapacity);
		}
	}
	
	/**
	 * Returns the number of contained elements.
	 *
	 * @return  the number of elements contained in the receiver.
	 */
	public int size() {
		return size;
	}
	
	/** Small helper method eliminating redundancy. */
	private int[] subArray(int from, int length, int capacity) {
		int[] subArray = new int[capacity];
		System.arraycopy(elements, from, subArray, 0, length);
		return subArray;
	}

	/**
	 * Returns a copied array of bytes containing all elements; the returned
	 * array has length = this.size().
	 */
	public int[] toArray() {
		return subArray(0, size, size);
	}

	
}