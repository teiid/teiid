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

import nu.xom.Comment;
import nu.xom.DocType;
import nu.xom.Element;
import nu.xom.IllegalAddException;
import nu.xom.ProcessingInstruction;
import nu.xom.Text;

/**
 * Utility that helps {@link nux.xom.io.StreamingSerializer} implementations to
 * perform basic wellformedness checks. Remaining wellformedness checks are all
 * implicitly performed by XOM beforehand.
 * 
 * @author whoschek.AT.lbl.DOT.gov
 * @author $Author: hoschek3 $
 * @version $Revision: 1.6 $, $Date: 2006/03/24 00:59:07 $
 */
final class StreamingVerifier { // not a public class
	
	private ElementStack elements;	
	private boolean hasXMLDeclaration;
	private boolean hasRootElement;
	private boolean hasDocType;
	
	StreamingVerifier() {
		reset();
	}
	
	public void reset() {
		elements = new ElementStack();
		hasRootElement = false;
		hasDocType = false;
		hasXMLDeclaration = false;
	}
		
	public void writeStartTag(Element elem) {
		if (elem == null) throwNullPointerException();
		if (inPrologOrEpilog()) checkWriteStartRootTag();

		elements.push(elem);
	}
	
	private void checkWriteStartRootTag() {
		checkHasXMLDeclaration();
		if (hasRootElement) throwWellformednessException(
			"Document must not have more than one root element");		

		hasRootElement = true;
	}
	
	public Element writeEndTag() {
		if (inPrologOrEpilog()) throwWellformednessException(
			"Imbalanced element tags; attempted to write an end tag for a nonexistent start tag");
		
		return elements.pop();
	}
	
	public void write(Text text) {
		if (text == null) throwNullPointerException();
		if (inPrologOrEpilog()) throwWellformednessException(
			"Text is not allowed as child of a document");
	}
	
	public void write(Comment comment) {
		if (comment == null) throwNullPointerException();
		checkHasXMLDeclaration();
	}
	
	public void write(ProcessingInstruction instruction) {
		if (instruction == null) throwNullPointerException();
		checkHasXMLDeclaration();
	}
	
	public void write(DocType docType) {
		if (docType == null) throwNullPointerException();
		checkHasXMLDeclaration();
		if (!inPrologOrEpilog()) throwWellformednessException(
			"Document type declaration is not allowed as child of an element");
		if (hasDocType) throwWellformednessException(
			"Document must not have more than one document type declaration");
		if (hasRootElement) throwWellformednessException(
			"Document type declaration is not allowed after the root element");

		hasDocType = true;
	}
	
	public void writeEndDocument() {
		checkHasXMLDeclaration();
		if (!hasRootElement) throwWellformednessException(
			"Missing root element; a document must have a root element");		
	}
	
	public void writeXMLDeclaration() {
		if (hasXMLDeclaration) throwWellformednessException(
			"Document must not have more than one XML declaration");
		
		hasXMLDeclaration = true;
	}
	
	public int depth() {
		return elements.size();
	}

	private boolean inPrologOrEpilog() {
		return depth() == 0;
	}

	private void checkHasXMLDeclaration() {
		if (!hasXMLDeclaration) throwWellformednessException(
			"Missing XML declaration. Probable causes: 1) forgot to call " + 
			"writeXMLDeclaration() or 2) called writeEndDocument() " +
			"before without a subsequent writeXMLDeclaration()");
	}

	private static void throwWellformednessException(String message) {
		throw new IllegalAddException(message);
	}
    	
	private static void throwNullPointerException() {
		throw new NullPointerException("Node argument must not be null");
	}

	
    	///////////////////////////////////////////////////////////////////////////////
    	// Nested classes:
    	///////////////////////////////////////////////////////////////////////////////
    	
	/**
	 * Fast replacement for ArrayList and java.util.Stack. Also improves
	 * readability.
	 */
	private static final class ElementStack {
		
		private Element[] elems = new Element[10];
		private int size = 0;
		
		public Element pop() {
			Element elem = elems[size-1];
			elems[--size] = null; // help gc
			return elem;
		}
		
		public void push(Element elem) {
			if (size == elems.length) ensureCapacity(size + 1);
			elems[size++] = elem;
		}
		
		public int size() {
			return size;
		}
		
		private void ensureCapacity(int minCapacity) {
			if (minCapacity > elems.length) {
				int newCapacity = Math.max(minCapacity, 2 * elems.length + 1);
				elems = subArray(0, size, newCapacity);
			}
		}
		
		/** Small helper method eliminating redundancy. */
		private Element[] subArray(int from, int length, int capacity) {
			Element[] subArray = new Element[capacity];
			System.arraycopy(elems, from, subArray, 0, length);
			return subArray;
		}
		
	}
		
}