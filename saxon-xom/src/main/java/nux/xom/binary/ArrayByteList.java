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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;


/**
 * Efficient resizable auto-expanding list holding <code>byte</code> elements;
 * implemented with arrays; more practical and efficient than NIO ByteBuffer.
 * 
 * @author whoschek.AT.lbl.DOT.gov
 * @author $Author: hoschek $
 * @version $Revision: 1.27 $, $Date: 2006/06/18 21:25:02 $
 */
final class ArrayByteList {

	/**
	 * The array into which the elements of the list are stored. The
	 * capacity of the list is the length of this array.
	 */
	private transient byte[] elements;

	/**
	 * The current number of elements contained in this list.
	 */
	private int size;

	/**
	 * The current position for relative (cursor based) getXXX methods.
	 */
	private transient int position = 0;

	private static final boolean DEBUG = false;

	
	/**
	 * Constructs an empty list.
	 */
	public ArrayByteList() {
		this(64);
	}

	/**
	 * Constructs an empty list with the specified initial capacity.
	 * 
	 * @param initialCapacity
	 *            the number of elements the receiver can hold without
	 *            auto-expanding itself by allocating new internal memory.
	 */
	public ArrayByteList(int initialCapacity) {
		elements = new byte[initialCapacity];
		size = 0;
	}

	/**
	 * Constructs a list SHARING the specified elements. The initial size and
	 * capacity of the list is the length of the backing array.
	 * <p>
	 * <b>WARNING: </b> For efficiency reasons and to keep memory usage low,
	 * <b>the array is SHARED, not copied </b>. So if subsequently you modify
	 * the specified array directly via the [] operator, be sure you know what
	 * you're doing.
	 * <p>
	 * If you rather need copying behaviour, use
	 * <code>copy = new ArrayByteList(byte[] elems).copy()</code> or similar.
	 * <p>
	 * If you need a list containing a copy of <code>elems[from..to)</code>, use
	 * <code>list = new ArrayByteList(to-from).add(elems, from, to-from)</code> 
	 * or <code>list = new ArrayByteList(ByteBuffer.wrap(elems, from, to-from))</code>
	 * or similar.
	 * 
	 * @param elems
	 *            the array backing the constructed list
	 */
	public ArrayByteList(byte[] elems) {
		elements = elems;
		size = elems.length;
	}

	/**
	 * Appends the specified element to the end of this list.
	 * 
	 * @param elem
	 *            element to be appended to this list.
	 */
	public void add(byte elem) {
		if (size == elements.length) ensureCapacity(size + 1);
		elements[size++] = elem;
	}

	/**
	 * Appends the elements in the range <code>[offset..offset+length)</code>
	 * to the end of this list.
	 * 
	 * @param elems
	 *            the elements to be appended
	 * @param offset
	 *            the offset of the first element to add (inclusive)
	 * @param length
	 *            the number of elements to add
	 * @throws IndexOutOfBoundsException
	 *             if indexes are out of range.
	 */
	public void add(byte[] elems, int offset, int length) {
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
	public byte[] asArray() {
		return elements;
	}

    /**
	 * Removes all elements but keeps the current capacity; Afterwards
	 * <code>size()</code> will yield zero.
	 */
	public void clear() {
		size = 0;
		position = 0;
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
	 * Ensures that there are at least <code>need</code> bytes remaining to be
	 * read. If there are currenty less bytes remaining, this method reads from
	 * the given input stream until there exactly holds
	 * <code>remaining() == need</code>.
	 * 
	 * @param input
	 *            the stream to read from
	 * @param need
	 *            the number of bytes to make available
	 * @return false if a premature end of stream was encountered, and not
	 *         enough bytes could be read as a result. Returns true otherwise.
	 * @throws IOException
	 *             if the underlying output stream encounters an I/O error
	 */
	public boolean ensureRemaining(InputStream input, int need) throws IOException {
		int remaining = remaining();
		need -= remaining;
		if (need <= 0) return true;
		
		int free = elements.length - size;
		if (free < need) { // not enough room available
			if (free + position >= need) { // compaction yields enough room
				System.arraycopy(elements, position, elements, 0, remaining);
			} else { // expand and compact to make room
				int newCapacity = Math.max(2*elements.length, remaining + need);
				byte[] tmp = new byte[newCapacity];
				System.arraycopy(elements, position, tmp, 0, remaining);
				elements = tmp;
			}
			size = remaining;
			position = 0;
		}
		
		// read nomore bytes than necessary (message framing protocol)
		return read(input, need) <= 0;
	}
	
	/**
	 * Reads <code>length</code> bytes from the given input stream, and appends
	 * them.
	 * <p>
	 * Assertion: There is enough room available, i.e. there holds
	 * <code>elements.length - size >= length</code>.
	 * 
	 * @return the number of bytes missing in case of premature end-of-stream
	 */
	private int read(InputStream input, int length) throws IOException {
		int n;
		while (length > 0 && (n = input.read(elements, size, length)) >= 0) {
			size += n;
			length -= n;
		}
		return length;
	}
	
	/**
	 * Writes all contained elements onto the given output stream.
	 * 
	 * @param out
	 *            the output stream to write to
	 * @throws IOException
	 *             if an I/O error occurs. In particular, an
	 *             <code>IOException</code> is thrown if the output stream is
	 *             closed.
	 */
	public void write(OutputStream out) throws IOException {
		if (size > 0) { // minimize I/O calls
			out.write(elements, 0, size);
		}
	}

	/**
	 * Returns the element at the specified index.
	 * 
	 * @param index
	 *            index of element to return.
	 * @throws IndexOutOfBoundsException if index is out of range.
	 */
	public byte get(int index) {
		if (DEBUG && index >= size) throwIndex(index);
		return elements[index];
	}

	/**
	 * Replaces the element at the specified index with the specified
	 * element.
	 * 
	 * @param index
	 *            index of element to replace.
	 * @param element
	 *            element to be stored at the specified index.
	 * @throws IndexOutOfBoundsException
	 *             if index is out of range.
	 */
	public void set(int index, byte element) {
		if (DEBUG && index >= size) throwIndex(index);
		elements[index] = element;
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
	private byte[] subArray(int from, int length, int capacity) {
		byte[] subArray = new byte[capacity];
		System.arraycopy(elements, from, subArray, 0, length);
		return subArray;
	}

	/**
	 * Returns a copied array of bytes containing all elements; the returned
	 * array has length = this.size().
	 */
	public byte[] toArray() {
		return subArray(0, size, size);
	}

	/**
	 * Returns a string representation, containing the numeric String
	 * representation of each element.
	 */
	public String toString() {
		StringBuffer buf = new StringBuffer(4*size);
		buf.append("[");
		for (int i = 0; i < size; i++) {
			buf.append(elements[i]);
			if (i < size-1) buf.append(", ");
		}
		buf.append("]");
		return buf.toString();
	}
	
	/**
	 * Checks if the given index is in range.
	 */
	private void throwIndex(int index) {
		throw new IndexOutOfBoundsException("index: " + index
					+ ", size: " + size);
	}

	/** Returns the current position offset for relative gets. */
	public int position() {
		return position;
	}
	
	/** Sets the current position offset for relative gets. */
	public void position(int position) {
		if (DEBUG && position > size) throwIndex(position);
		this.position = position;
	}
	
	/** Returns the number of elements that are left to be read. */
	public int remaining() {
		return size - position;
	}
	
	/** Reads and returns the byte value at the current position offset. */
	public byte get() {
		if (DEBUG && position >= size) throwIndex(position);
		return elements[position++];
	}
	
	/** Reads and returns the 4 byte big endian value at the current position offset. */
	public int getInt() {
		if (DEBUG && position + 4 > size) throwIndex(position);
		byte b3 = elements[position+0];
		byte b2 = elements[position+1];
		byte b1 = elements[position+2];
		byte b0 = elements[position+3];
		position += 4;
		return ((((b3 & 0xff) << 24) | ((b2 & 0xff) << 16) | 
				((b1 & 0xff) << 8) | ((b0 & 0xff) << 0)));
	}
		
	/** Reads and returns the 2 byte big endian value at the current position offset. */
	public short getShort() {
		if (DEBUG && position + 2 > size) throwIndex(position);
		byte b1 = elements[position+0];
		byte b0 = elements[position+1];
		position += 2;
		return (short) ((b1 << 8) | (b0 & 0xff));
	}
	
	/** Writes the given value at the given index in 4 byte big endian.  */
	public void setInt(int index, int v) { 
		if (DEBUG && index + 4 > size) throwIndex(index);
		elements[index+0] = (byte) (v >> 24);
		elements[index+1] = (byte) (v >> 16);
		elements[index+2] = (byte) (v >> 8);
		elements[index+3] = (byte) (v >> 0);
	}
	
	/** Appends the given value in 4 byte big endian. */
	public void addInt(int v) {
		if (size + 4 > elements.length) ensureCapacity(size + 4);
		elements[size+0] = (byte) (v >> 24);
		elements[size+1] = (byte) (v >> 16);
		elements[size+2] = (byte) (v >> 8);
		elements[size+3] = (byte) (v >> 0);
		size += 4;
	}
	
	/** Appends the given value in 2 byte big endian. */
	public void addShort(short v) { 
		if (size + 2 > elements.length) ensureCapacity(size + 2);
		elements[size+0] = (byte) (v >> 8);
		elements[size+1] = (byte) (v >> 0);
		size += 2;
	}
			
	/**
	 * Removes the elements in the range <code>[from..to)</code>. Shifts any
	 * subsequent elements to the left.  Keeps the current capacity.
	 * Note: To remove a single element use <code>remove(index, index+1)</code>.
	 * 
	 * @param from
	 *            the index of the first element to removed (inclusive).
	 * @param to
	 *            the index of the last element to removed (exclusive).
	 * @throws IndexOutOfBoundsException
	 *             if indexes are out of range
	 */
	public void remove(int from, int to) {
		shrinkOrExpand(from, to, 0);
	}
	
	/**
	 * The powerful work horse for all add/insert/replace/remove methods.
	 * One powerful efficient method does it all :-)
	 */
	private void shrinkOrExpand(int from, int to, int replacementSize) {
		checkRange(from, to);
		int diff = replacementSize - (to - from);
		if (diff != 0) {
			ensureCapacity(size + diff);
			if (size - to > 0) { // check is for performance only (arraycopy is native method)
				// diff > 0 shifts right, diff < 0 shifts left
				System.arraycopy(elements, to, elements, to + diff, size - to);
			}
			size += diff;
		}
	}

	/**
	 * Checks if the given range is within the contained array's bounds.
	 */
	private void checkRange(int from, int to) {
		if (from < 0 || from > to || to > size)
			throw new IndexOutOfBoundsException("from: " + from + ", to: "
						+ to + ", size: " + size);
	}
	
	/** Exchanges the internal state of the two lists. */
	public void swap(ArrayByteList dst) {
		byte[] e = elements; elements = dst.elements; dst.elements = e;
		int s = size; size = dst.size; dst.size = s;
		int p = position; position = dst.position; dst.position = p;
	}
	
	/**
	 * Compresses the elements in the range
	 * <code>src[src.position()..src.size())</code>, appending the compressed
	 * data to the receiver. <code>src</code> is not modified in any way in
	 * the process.
	 */
	public void add(Deflater compressor, ArrayByteList src) {
		compressor.reset();
		compressor.setInput(src.asArray(), src.position(), src.remaining());
		int minBufSize = 256;
		ensureCapacity(size + Math.max(minBufSize, src.remaining() / 3));
		
		do {
			ensureCapacity(size + minBufSize);
			size += compressor.deflate(elements, size, elements.length - size);
		} while (!compressor.needsInput());
		
		if (!compressor.finished()) {
		    compressor.finish();
		    while (!compressor.finished()) {
		    		ensureCapacity(size + minBufSize);
				size += compressor.deflate(elements, size, elements.length - size);
		    }
		}
		
		compressor.reset();
	}
	
	/**
	 * Decompresses the elements in the range
	 * <code>src[src.position()..src.size())</code>, appending the decompressed
	 * data to the receiver. <code>src</code> is not modified in any way in
	 * the process.
	 * <p>
	 * If ZLIB decompression finishes before reaching the end of the range, the
	 * remaining trailing bytes are added to the receiver as well.
	 * 
	 * @param decompressor
	 *            the ZLIB decompressor to use
	 * @param src
	 *            the input data to decompress
	 * @return the number of trailing bytes that were found not to be part of
	 *         the ZLIB data.
	 * 
	 * @throws DataFormatException
	 *             if the compressed data format is invalid
	 */
	public int add(Inflater decompressor, ArrayByteList src) throws DataFormatException {
		decompressor.reset();
		decompressor.setInput(src.asArray(), src.position(), src.remaining());

		int minBufSize = 256;
		ensureCapacity(size + Math.max(minBufSize, src.remaining() * 3));
		
		do {
			ensureCapacity(size + minBufSize);
			size += decompressor.inflate(elements, size, elements.length - size);
		} while (!(decompressor.finished() || decompressor.needsDictionary()));
		
		int remaining = decompressor.getRemaining();
		if (remaining > 0) { // copy trailer that wasn't compressed to begin with
			ensureCapacity(size + remaining);
			int off = src.size() - remaining;
			System.arraycopy(src.asArray(), off, elements, size, remaining);
			size += remaining;
		}
		decompressor.reset();
		return remaining;
	}
	
	private void addUTF16String(String prefix, String localName) {
//		length prefixed string:
//		bit 0..6 [0..127]
//		bit 7: is4ByteInt. if so, then bit 0..6 are ignored and next 4 bytes are signed int big endian length
		int len = localName.length();
		if (prefix.length() != 0) len += prefix.length() + 1;
		
		if (len <= 127) {
			add((byte)len);
		} else {
			add((byte)-1);
			addInt(len);
		}
		
		if (prefix.length() != 0) {
			addUTF16String(prefix);			
			addUTF16String(":");
//			add((byte)0);
//			add((byte)':'); // ASCII 58
//			elements[size++] = (byte)':';
		}
		addUTF16String(localName);						
	}

	private void addUTF16String(String str) {
		int len = str.length();		
		for (int i=0; i < len; i++) {
			char c = str.charAt(i);
			add((byte)(c >> 8));
			add((byte)(c & 0xFF));
		}
	}

	private String getUTF16String(ArrayCharList buf) {
		buf.clear();
		int len = get();
		if (len < 0) len = getInt();
		
		for (int i=0; i < len; i++) {
			int b1 = get() & 0xFF;
			int b2 = get() & 0xFF;
			char c = (char)((b1 << 8) | b2);
			buf.add(c);
		}
		return buf.toString();
	}

	public String[] getUTF16Strings(int count) {
		final String[] dst = new String[count];
		final ArrayCharList buffer = new ArrayCharList(32);
		final byte[] elems = elements;
		int off = position;
		
		for (int k = 0; k < count; k++) {
			dst[k] = getUTF16String(buffer);
		}
		
		return dst;
	}
	
	/** Appends the UTF-8 encoding of the given string, followed by a zero byte terminator. */
	public void addUTF8String(String prefix, String localName) {
		// assert: array capacity is large enough, so there's no need to expand
		if (prefix.length() != 0) {
			addUTF8String(prefix);				
//			add((byte)':'); // ASCII 58
			elements[size++] = (byte)':';
		}
		addUTF8String(localName);
		
		/*
		 * We exploit the facts that 1) the UTF-8 spec guarantees that a zero
		 * byte is never introduced as a side effect of UTF-8 encoding (unless a
		 * zero character 0x00 is part of the input), and that 2) in XML the
		 * zero character 0x00 is illegal, and hence can never occur (e.g.
		 * nu.xom.Verifier.java). It follows, that a zero byte can never occur
		 * anywhere in an UTF-8 encoded XML document. Thus, for compactness and
		 * simplicity we use zero terminated strings instead of length prefixed
		 * strings, without any ambiguity. See
		 * http://www.cl.cam.ac.uk/~mgk25/unicode.html and
		 * http://marc.theaimsgroup.com/?t=112253855300002&r=1&w=2&n=13
		 */
//		add((byte) 0); 
		elements[size++] = (byte) 0;
	}
	
	/** Appends the UTF-8 encoding of the given string. */
	private void addUTF8String(String str) {
		int len = str.length();
//		int max = 4*len;
//		if (size + max > elements.length) ensureCapacity(size + max);
		byte[] elems = elements;
		int s = size;
		for (int i = 0; i < len; i++) {
			char c = str.charAt(i);
			if (c < 0x80) { // Have at most seven bits (7 bit ASCII)
//				add((byte) c);
				elems[s++] = (byte) c;
				continue;
			}
			else if (!UnicodeUtil.isSurrogate(c)) {
				if (c < 0x800) { // 2 bytes, 11 bits
//					add((byte) (0xc0 | ((c >> 06))));
//					add((byte) (0x80 | ((c >> 00) & 0x3f)));
					elems[s++] = (byte) (0xc0 | ((c >> 06)));
					elems[s++] = (byte) (0x80 | ((c >> 00) & 0x3f));
					continue;
				}
				if (c <= '\uFFFF') { // 3 bytes, 16 bits
//					add((byte) (0xe0 | ((c >> 12))));
//					add((byte) (0x80 | ((c >> 06) & 0x3f)));
//					add((byte) (0x80 | ((c >> 00) & 0x3f)));
					elems[s++] = (byte) (0xe0 | ((c >> 12)));
					elems[s++] = (byte) (0x80 | ((c >> 06) & 0x3f));
					elems[s++] = (byte) (0x80 | ((c >> 00) & 0x3f));
					continue;
				}
			}

			size = s;
			i = addUTFSurrogate(c, str, i);
			s = size;
		}

		size = s;
	}
	
	private int addUTFSurrogate(char c, String str, int i) {
		int uc = 0;
		if (UnicodeUtil.isHighSurrogate(c)) {
			if (str.length() - i < 2) charCodingException("underflow", str);			
			char d = str.charAt(++i);
			if (!UnicodeUtil.isLowSurrogate(d)) charCodingException("malformedForLength(1)", str);
			uc = UnicodeUtil.toUCS4(c, d);
		}
		else {
			if (UnicodeUtil.isLowSurrogate(c)) charCodingException("malformedForLength(1)", str);
			uc = c;
		}
		
		if (uc < 0x200000) {
			add((byte) (0xf0 | ((uc >> 18))));
			add((byte) (0x80 | ((uc >> 12) & 0x3f)));
			add((byte) (0x80 | ((uc >> 06) & 0x3f)));
			add((byte) (0x80 | ((uc >> 00) & 0x3f)));
		}
		return i;
	}
	
	private static void charCodingException(String msg, String str) {
		throw new RuntimeException("CharacterCodingException: " + msg
				+ " for string '" + str + "'");
	}
	
	private static void charCodingException(String msg, ArrayCharList str) {
		throw new RuntimeException("CharacterCodingException: " + msg
				+ " for string '" + str + "'");
	}
			
	/**
	 * Reads and returns the next "count" (zero terminated) 7 bit ASCII encoded
	 * strings, starting at the current position offset. Note that 7 bit ASCII
	 * is a proper subset of UTF-8.
	 */
	public String[] getASCIIStrings(int count) {
		final String[] dst = new String[count]; 
		final byte[] elems = elements;
		int off = position;
		for (int k = 0; k < count; k++) {
			int start = off;
			while (elems[off] != 0) { // until bnux string terminator
				off++;
			}
			dst[k] = new String(elems, 0, start, off-start); // intentional and safe!
			off++; // read past zero byte string terminator
		}
			
		position = off;
		if (DEBUG && position > size) throwIndex(position);
		return dst;
	}
	
	/**
	 * Reads and returns the next "count" (zero terminated) UTF-8 encoded
	 * strings, starting at the current position offset.
	 */
	public String[] getUTF8Strings(int count) {
		final String[] dst = new String[count];
		final ArrayCharList buffer = new ArrayCharList(32);
		final byte[] elems = elements;
		int off = position;
		
		for (int k = 0; k < count; k++) {
			int start = off;
			int b;
			while ((b = elems[off]) > 0) { // scan to see if string is pure 7 bit ASCII
				off++; 
			}
			if (b == 0) { // fast path: pure 7 bit ASCII (safe)
				dst[k] = new String(elems, 0, start, off-start); // intentional and safe!
			} else { // slow path: arbitrary UTF-8
				dst[k] = getUTF8String(start, off, buffer);
				off = position;
			}
			off++; // read past zero byte string terminator
		}
		
		position = off;
		if (DEBUG && position > size) throwIndex(position);
		return dst;
	}
	
	private String getUTF8String(int i, int asciiEnd, ArrayCharList buffer) {
		buffer.clear();
		final byte[] src = elements;
		
		// Decode remaining UTF-8 bytes and add corresponding chars. 		
		// We use doubly nested loop so we can use buf[s++] = ... instead of buf.add(char)
		while (true) {
			int s = buffer.size();
			final int max = i + buffer.capacity() - s - 6; // better safe than sorry
			final char[] dst = buffer.asArray();
			
			int b1;
			while (i < max && ((b1 = src[i]) != 0)) { // until bnux string terminator
				int b2, b3;
				switch ((b1 >> 4) & 0x0f) {
	
				case 0:
				case 1:
				case 2:
				case 3:
				case 4:
				case 5:
				case 6:
				case 7: { // 1 byte, 7 bits: 0xxxxxxx
//					buffer.add((char) b1);
//					i++;
					while (i < max && (b1 = src[i]) > 0) { // fast path: pure 7 bit ASCII
//						buffer.add((char)b1);
						dst[s++] = (char) b1;
						i++;
					}
					continue;
				}
				case 12:
				case 13: { // 2 bytes, 11 bits: 110xxxxx 10xxxxxx
//					if (size - i < 2) charCodingException("underflow", buffer);
					b2 = src[i+1];
					if (isLast(b2)) charCodingException("malformedForLength(1)", buffer);
					dst[s++] = (char) (((b1 & 0x1f) << 6) | ((b2 & 0x3f) << 0));
//					buffer.add((char) (((b1 & 0x1f) << 6) | ((b2 & 0x3f) << 0)));
					i += 2;
					continue;
				}
				case 14: { // 3 bytes, 16 bits: 1110xxxx 10xxxxxx 10xxxxxx
//					if (size - i < 3) charCodingException("underflow", buffer);
					b2 = src[i+1];
					if (isLast(b2)) charCodingException("malformedForLength(1)", buffer);
					b3 = src[i+2];
					if (isLast(b3)) charCodingException("malformedForLength(2)", buffer);
					dst[s++] = (char) (((b1 & 0x0f) << 12) 
							| ((b2 & 0x3f) << 06) | ((b3 & 0x3f) << 0));
//					buffer.add((char) (((b1 & 0x0f) << 12)
//							| ((b2 & 0x3f) << 06) | ((b3 & 0x3f) << 0)));
					i += 3;
					continue;
				}
				case 15: { // 4, 5, or 6 bytes
					buffer.setSize(s);
					i = getUTF8String456(i, buffer);
					s = buffer.size();
					continue;
				}
				default:
					charCodingException("malformedForLength(1)", buffer);
				}
	
			} // end while
			
			buffer.setSize(s);
			if (i < max) break; // we're done
			buffer.ensureCapacity(buffer.capacity() << 1);
		} // end while(true)
		
		position = i;
		return buffer.toString();
	}
	
	private int getUTF8String456(int i, ArrayCharList buffer) {
		final byte[] elems = elements;
		int b1 = elems[i];
		int b2, b3;
		int b4;
		int b5;
		int b6;
		int uc = 0;
		int n = 0;
		
		switch (b1 & 0x0f) {

		case 0:
		case 1:
		case 2:
		case 3:
		case 4:
		case 5:
		case 6:
		case 7: { // 4 bytes, 21 bits
//			if (size - i < 4) charCodingException("underflow", buffer);
			b2 = elems[i+1];
			if (isLast(b2)) charCodingException("malformedForLength(1)", buffer);
			b3 = elems[i+2];
			if (isLast(b3)) charCodingException("malformedForLength(2)", buffer);
			b4 = elems[i+3];
			if (isLast(b4)) charCodingException("malformedForLength(3)", buffer);
			uc = (((b1 & 0x07) << 18) | ((b2 & 0x3f) << 12)
					| ((b3 & 0x3f) << 06) | ((b4 & 0x3f) << 00));
			n = 4;
			break;
		}
		case 8:
		case 9:
		case 10:
		case 11: { // 5 bytes, 26 bits
//			if (size - i < 5) charCodingException("underflow", buffer);
			b2 = elems[i+1];
			if (isLast(b2)) charCodingException("malformedForLength(1)", buffer);
			b3 = elems[i+2];
			if (isLast(b3)) charCodingException("malformedForLength(2)", buffer);
			b4 = elems[i+3];
			if (isLast(b4)) charCodingException("malformedForLength(3)", buffer);
			b5 = elems[i+4];
			if (isLast(b5)) charCodingException("malformedForLength(4)", buffer);
			uc = (((b1 & 0x03) << 24) | ((b2 & 0x3f) << 18)
				| ((b3 & 0x3f) << 12) | ((b4 & 0x3f) << 06) 
				| ((b5 & 0x3f) << 00));
			n = 5;
			break;
		}
		case 12:
		case 13: { // 6 bytes, 31 bits
//			if (size - i < 6) charCodingException("underflow", buffer);
			b2 = elems[i+1];
			if (isLast(b2)) charCodingException("malformedForLength(1)", buffer);
			b3 = elems[i+2];
			if (isLast(b3)) charCodingException("malformedForLength(2)", buffer);
			b4 = elems[i+3];
			if (isLast(b4)) charCodingException("malformedForLength(3)", buffer);
			b5 = elems[i+4];
			if (isLast(b5)) charCodingException("malformedForLength(4)", buffer);
			b6 = elems[i+5];
			if (isLast(b6)) charCodingException("malformedForLength(5)", buffer);
			uc = (((b1 & 0x01) << 30) | ((b2 & 0x3f) << 24)
					| ((b3 & 0x3f) << 18) | ((b4 & 0x3f) << 12)
					| ((b5 & 0x3f) << 06) | ((b6 & 0x3f)));
			n = 6;
			break;
		}
		default:
			charCodingException("malformedForLength(1)", buffer);
		}

		addSurrogate(uc, n, buffer);
		i += n;
		return i;
//		continue;
	}
		
	private static boolean isLast(int v) {
	    return DEBUG && (0x80 != (v & 0xc0));
	}	
	
	private static void addSurrogate(int uc, int len, ArrayCharList dst) {
		if (uc <= 0xffff) {
			if (UnicodeUtil.isSurrogate(uc)) charCodingException("malformedForLength(len)", dst);
			dst.add((char) uc);
			return;
		}
		if (uc < UnicodeUtil.UCS4_MIN) charCodingException("malformedForLength(len)", dst);
		if (uc <= UnicodeUtil.UCS4_MAX) {
			dst.add(UnicodeUtil.highSurrogate(uc));
			dst.add(UnicodeUtil.lowSurrogate(uc));
			return;
		}
		charCodingException("unmappableForLength(len)", dst);
	}

}