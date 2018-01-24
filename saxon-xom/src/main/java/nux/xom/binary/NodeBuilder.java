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

import nu.xom.Attribute;
import nu.xom.Element;


/**
 * EXPERIMENTAL; Not part of published API; Subject to change or removal without
 * notice. Wishing Java had "friends" to make select internal classes visible 
 * across package boundaries...
 * <p>
 * Small fast utility for efficient construction of elements and attributes.
 * Implemented via a LRU cache of the most recent elements and attributes.
 * <p>
 * Avoids repeated XML reverification in XOM Element and Attribute constructor
 * on cache hit, plus avoids generating new String objects by quasi "interning"
 * localName and prefix, which in turn reduces memory consumption. Has overhead
 * close to zero on cache miss.
 * <p>
 * This implementation is not thread-safe.
 * <p>
 * This class would become obsolete if the XOM Element and Attribute classes
 * would have an internal LRU cache of QNames.
 * 
 * @author whoschek.AT.lbl.DOT.gov
 * @author $Author: hoschek3 $
 * @version $Revision: 1.7 $, $Date: 2006/01/02 01:08:51 $
 */
public final class NodeBuilder {

	private final LRUHashMap2 elements;
	private final LRUHashMap2 attributes;
	
	/** Constructs a new instance. */
	public NodeBuilder() {
		this(512);
	}
	
	/** Constructs a new instance that can hold at most maxSize entries. */
	private NodeBuilder(int maxSize) {
		this.elements = new LRUHashMap2(maxSize);
		this.attributes = new LRUHashMap2(maxSize);
	}
	
	/**
	 * Contructs and returns a new element for the given qualified name and
	 * namespace URI.
	 * 
	 * @param qname
	 *            the qualified name of the element (must not be null)
	 * @param uri
	 *            the namespace URI of the element
	 * @return a new element
	 */
	public Element createElement(String qname, String uri) {
		if (uri == null) uri = "";
		
		Element elem = (Element) elements.get(qname, uri);
		if (elem == null) {
			elem = new Element(qname, uri);
			elements.put(qname, uri, elem);
		}
		return new Element(elem); // copy constructor is fast
	}
	
	/**
	 * Contructs and returns a new attribute for the given qualified name and
	 * namespace URI.
	 * 
	 * @param qname
	 *            the qualified name of the attribute (must not be null)
	 * @param uri
	 *            the namespace URI of the attribute
	 * @param value
	 *            the attribute value (must not be null)
	 * @param type
	 *            the attribute type (must not be null)
	 * @return a new attribute
	 */
	public Attribute createAttribute(String qname, String uri, String value, Attribute.Type type) {
		if (uri == null) uri = "";
		if ("xml:id".equals(qname)) {
			type = Attribute.Type.ID; // avoids exception in Attribute.setType()
		}
		
		Attribute attr = (Attribute) attributes.get(qname, uri);
		if (attr == null) {
			attr = new Attribute(qname, uri, value, type);
			attributes.put(qname, uri, attr);
			return new Attribute(attr); // copy constructor is fast
		}
		attr = new Attribute(attr); // copy constructor is fast
		if (attr.getType() != type) attr.setType(type);
		attr.setValue(value);
		return attr;
	}

}