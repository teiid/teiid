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
package nux.xom.xquery;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import nu.xom.Attribute;
import nu.xom.Comment;
import nu.xom.DocType;
import nu.xom.Document;
import nu.xom.Element;
//import nu.xom.Namespace;
import nu.xom.Node;
import nu.xom.Nodes;
import nu.xom.ParentNode;
import nu.xom.ProcessingInstruction;
import nu.xom.Serializer;
import nu.xom.Text;
import nu.xom.XMLException;

/**
 * Serializes an XQuery/XPath result sequence onto a given output stream, using
 * various configurable serialization options such encoding, indentation and
 * algorithm. The semantics of options are identical to the XOM
 * {@link Serializer}, except the "algorithm" option.
 * <p>
 * The <b>W3C algorithm</b> serializes each item in the result sequence
 * according to the XML Output Method of the <a target="_blank"
 * href="http://www.w3.org/TR/xslt-xquery-serialization"> W3C XQuery/XSLT2
 * Serialization Spec</a>, with sequence normalization as defined
 * therein. As such, it may output data that is not a well-formed document. For
 * example, if the result sequence contains more than one element then a
 * document with more than one root element will be output. However, for some
 * use cases the algorithm does indeed output a well-formed XML document. For
 * example, if the result sequence contains a single document or element node.
 * Finally, note that an exception is thrown if the result sequence contains a
 * (top-level) attribute node.
 * <p>
 * In contrast, the <b>wrap algorithm</b> wraps each item in the result
 * sequence into a decorated element wrapper, thereby ensuring that any
 * arbitrary result sequence can always be output as a well-formed XML document.
 * This enables easy processing in subsequent XML processing pipeline stages.
 * Unlike the W3C algorithm, the wrap algorithm does not perform sequence
 * normalization. Thus, wrapping is better suited for XQuery debugging purposes,
 * because one can see exactly what items a query does (or does not) return.
 * <p>
 * Example usage:
 * <pre>
 * Document doc = new Builder().build(new File("samples/data/p2pio-receive.xml"));
 * Nodes results = XQueryUtil.xquery(doc, "//*");
 * // Nodes results = XQueryUtil.xquery(doc, "//node(), //@*, 'Hello World!'");
 * ResultSequenceSerializer ser = new ResultSequenceSerializer();
 * ser.setEncoding("UTF-8");
 * ser.setIndent(4);
 * ser.setAlgorithm(ResultSequenceSerializer.W3C_ALGORITHM);
 * // ser.setAlgorithm(ResultSequenceSerializer.WRAP_ALGORITHM);
 * ser.write(results, System.out);
 * </pre>
 *
 * @author whoschek.AT.lbl.DOT.gov
 * @author $Author: hoschek3 $
 * @version $Revision: 1.49 $, $Date: 2005/12/05 06:53:04 $
 */
public class ResultSequenceSerializer {

	// Note: does not subclass XOM Serializer for maximum impl. flexibility,
	// and to reduce number of public classes (i.e. API complexity).

	// TODO: add lineSeparator, maxLength, preserveBaseURI?
	// TODO: add node ID decorations for wrapped algo?
	// TODO: add write(ResultSequence, OutputStream)? could have minimum overhead by
	// avoiding intermediate node and node list construction by pulling Saxon items
	// from the pipeline, and writing them directly one by one.

	/**
	 * Serializes each item in the result sequence according to the XML Output
	 * Method of the <a target="_blank"
	 * href="http://www.w3.org/TR/xslt-xquery-serialization"> W3C XQuery/XSLT2
	 * Serialization Draft Spec</a>, with sequence normalization as defined
	 * therein.
	 */
	public static final String W3C_ALGORITHM  = "w3c";

	/**
	 * Serializes each item in the result sequence by wrapping it into a
	 * decorated element, without sequence normalization.
	 */
	public static final String WRAP_ALGORITHM = "wrap";

	private String algorithm = W3C_ALGORITHM;
	private int indent = 0;
	private String encoding = "UTF-8";
	private boolean nfc = false;

	/**
	 * Constructs and returns a serializer with default options.
	 */
	public ResultSequenceSerializer() {
	}

	/**
	 * Returns the current serialization algorithm; Can be
	 * {@link #W3C_ALGORITHM} or {@link #WRAP_ALGORITHM}; Defaults to
	 * {@link #W3C_ALGORITHM}.
	 *
	 * @return the current algorithm
	 */
	public String getAlgorithm() {
		return algorithm;
	}

	/**
	 * Returns the number of spaces to insert for each nesting level for pretty
	 * printing purposes; Defaults to zero; For details, see
	 * {@link Serializer#setIndent(int)}.
	 *
	 * @return the current the number of spaces
	 */
	public int getIndent() {
		return indent;
	}

	/**
	 * Returns the current serialization character encoding; Defaults to
	 * "UTF-8"; For details, see
	 * {@link Serializer#Serializer(OutputStream, String)}.
	 *
	 * @return the current encoding
	 */
	public String getEncoding() {
		return encoding;
	}

	/**
	 * Returns whether or not to perform Unicode normalization form C (NFC);
	 * Defaults to false; For details, see
	 * {@link Serializer#setUnicodeNormalizationFormC(boolean)}
	 *
	 * @return whether or not to perform NFC
	 */
	public boolean getUnicodeNormalizationFormC() {
		return nfc;
	}

	/**
	 * Sets the serialization algorithm.
	 *
	 * @param algorithm
	 *            the serialization algorithm to use
	 */
	public void setAlgorithm(String algorithm) {
		if (!(W3C_ALGORITHM.equals(algorithm) || WRAP_ALGORITHM.equals(algorithm))) {
			throw new IllegalArgumentException(
				"Unrecognized XQuery serialization algorithm: " + algorithm);
		}
		this.algorithm = algorithm;
	}

	/**
	 * Sets the character encoding for the serialization.
	 *
	 * @param encoding
	 *            the encoding to use
	 */
	public void setEncoding(String encoding) {
		if (encoding == null)
			throw new NullPointerException("Encoding must not be null");
		this.encoding = encoding;
	}

	/**
	 * Sets the number of spaces to insert for each nesting level.
	 *
	 * @param indent
	 *            the indentation to use (must be &gt;= 0)
	 */
	public void setIndent(int indent) {
		this.indent = indent;
	}

	/**
	 * Sets whether or not to perform Unicode normalization form C (NFC).
	 *
	 * @param nfc
	 *            true to normalize with NFC, false otherwise.
	 */
	public void setUnicodeNormalizationFormC(boolean nfc) {
		this.nfc = nfc;
	}

	/**
	 * Returns a string representation for debugging purposes.
	 *
	 * @return a string representation
	 */
	public String toString() {
		return "[" +
		"algorithm=" + getAlgorithm() +
		", encoding=" + getEncoding() +
		", indent=" + getIndent() +
		", unicodeNormalizationFormC=" + getUnicodeNormalizationFormC() +
		"]";
	}

	/**
	 * Serializes the given result sequence onto the given output stream.
	 * This method does not auto-close the output stream.
	 *
	 * @param nodes
	 *            the result sequence to serialize
	 * @param out
	 *            the stream to write to
	 * @throws IOException
	 *             if an I/O error occured
	 */
	public void write(Nodes nodes, OutputStream out) throws IOException {
		SequenceSerializer ser;
		if (W3C_ALGORITHM.equals(getAlgorithm())) {
			ser = new W3CSerializer(out, getEncoding());
		} else {
			ser = new WrapSerializer(out, getEncoding());
		}

		ser.setIndent(getIndent());
		ser.setUnicodeNormalizationFormC(getUnicodeNormalizationFormC());
//		ser.setLineSeparator(getLineSeparator());
//		ser.setMaxLength(getMaxLength());
//		ser.setPreserveBaseURI(getPreserveBaseURI());

		ser.write(nodes);
	}


	///////////////////////////////////////////////////////////////////////////////
	// Nested classes:
	///////////////////////////////////////////////////////////////////////////////
	private static abstract class SequenceSerializer extends Serializer {

		public SequenceSerializer(OutputStream out, String encoding)
				throws UnsupportedEncodingException {
			super(out, encoding);
		}

		// override for specific implementations
		public abstract void write(Nodes nodes) throws IOException;

	}


	///////////////////////////////////////////////////////////////////////////////
	// Nested classes:
	///////////////////////////////////////////////////////////////////////////////
	private static final class W3CSerializer extends SequenceSerializer {

		private boolean writeNamespaceDeclarationsInScope = false;
//		private static boolean isXom11Plus = true;

		public W3CSerializer(OutputStream out, String encoding)
				throws UnsupportedEncodingException {
			super(out, encoding);
		}

		protected void writeXMLDeclaration() throws IOException {
			writeRaw("<?xml version=\"1.0\" encoding=\"");
			writeRaw(getEncoding());
			writeRaw("\"?>");
//			breakLine();  // subtle but important: omit newline
		}

		/** Includes efficient impl of the W3C result sequence normalization spec. */
		public void write(Nodes nodes) throws IOException {
			final boolean indentYes = getIndent() > 0;
			boolean mayBreakLine = true;
			boolean isPreviousAtomic = false;
			this.writeNamespaceDeclarationsInScope = false;

			writeXMLDeclaration();

			int size = nodes.size();
			for (int i=0; i < size; i++) {
				Node node = nodes.get(i);
				if (node instanceof Attribute) {
					throw new XMLException(
					"SENR0001: W3C XQuery Serialization spec forbids top-level attributes");
//				} else if (node instanceof Namespace) {
//					throw new XMLException(
//					"SENR0001: W3C XQuery Serialization spec forbids top-level namespaces");
				} else if (node instanceof Document) {
					// Replace document with its children
					// Note that a Document can't have an atomic value or Text as child
					Document doc = (Document) node;
					for (int j=0; j < doc.getChildCount(); j++) {
						Node child = doc.getChild(j);
						if (mayBreakLine && indentYes && child instanceof Element) {
							breakLine();
						}
						writeChild(child);
						mayBreakLine = true;
					}
					isPreviousAtomic = false;
				} else if (isAtomicValue(node)) {
					// Replace adjacent atomic values with their string
					// concatenation, separated by a space, forming a Text node
					if (isPreviousAtomic) writeEscaped(" ");
					writeEscaped(node.getChild(0).getValue()); // string value of atomic value
					mayBreakLine = false;
					isPreviousAtomic = true;
				} else if (node instanceof Text) {
					// replace adjacent Texts with their string concatenation, removing empty texts
					String value = node.getValue();
					if (value.length() > 0) {
						writeEscaped(value);
						mayBreakLine = false;
						isPreviousAtomic = false;
					}
				} else { // any other node type
					if (node instanceof Element) {
						if (mayBreakLine && indentYes) breakLine();
						// root elements need no special toplevel namespace treatment
						ParentNode parent = node.getParent();
						this.writeNamespaceDeclarationsInScope =
							parent != null && !(parent instanceof Document);
					}
					writeChild(node);
					this.writeNamespaceDeclarationsInScope = false;
					mayBreakLine = true;
					isPreviousAtomic = false;
				}
			} // end for

			if (mayBreakLine && indentYes) breakLine();
			flush();
		}

		/**
		Ensures namespaces declared on ancestor-or-self are included in output. This is necessary
		because they may actually be in use somewhere in the subtree. Example:
		<pre>

		doc :=
		<SOAP:a xmlns:SOAP="http://schemas.xmlsoap.org/soap/envelope/" xmlns:foo="http://example.com">
			<b>
				<SOAP:c/>
			</b>
		</SOAP:a>

		result sequence := (b)

		The XOM Serializer assumes that an entire document is written, rather than arbitrary
		(subtree) nodes. It would generate this (wrong) output 1:
		<b>
			<SOAP:c/>
		</b>
		This is because its algorithm thinks it has previously already written the <SOAP:a>
		element, including its namespace declaration.

		Our method adds all namespaces-in-scope to the subforest's root, generating the following
		(expected) output 2:
		<b xmlns:SOAP="http://schemas.xmlsoap.org/soap/envelope/" xmlns:foo="http://example.com">
			<SOAP:c/>
		</b>

		Note: According to http://xquery.com/pipermail/talk/2005-November/000883.html,
		incorrect output 3 would be:
		<b>
			<SOAP:c xmlns:SOAP="http://schemas.xmlsoap.org/soap/envelope/"/>
		</b>

		</pre>
		*/
		protected void writeNamespaceDeclarations(Element element) throws IOException {
			if (this.writeNamespaceDeclarationsInScope) {
				this.writeNamespaceDeclarationsInScope = false;
				writeNamespaceDeclarationsInScope(element);
			} else {
				super.writeNamespaceDeclarations(element);
			}
		}

		private void writeNamespaceDeclarationsInScope(Element element) throws IOException {
			Map namespaces = getNamespacePrefixesInScope(element);
			int size = namespaces.size();

			// TODO: enable non-normative cosmetic sort by prefix ???
			// if (size > 1) namespaces = new java.util.TreeMap(namespaces);

			Iterator iter = namespaces.entrySet().iterator();
			for (int i = 0; i < size; i++) {
				Map.Entry entry = (Map.Entry) iter.next();
				String uri = (String) entry.getValue();
				if (uri.length() > 0) { // xmlns="" is unnecessary
					String prefix = (String) entry.getKey();
					writeRaw(" ");
					writeNamespaceDeclaration(prefix, uri);
				}
			}
		}

	    /*
		 * Awkward work-around to use XOM's
		 * Element.getNamespacePrefixesInScope(), or, if that's not available as
		 * a public method, a slow fallback solution.
		 */
		private static Map getNamespacePrefixesInScope(Element element) {
//			if (isXom11Plus) { // fast path
//				try {
//					return getNamespacePrefixesInScopePublic(element);
//				} catch (Error e) {
//					isXom11Plus = false;
//				}
//			}
			// slow path
			return getNamespacePrefixesInScopeNonPublic(element);
		}

//		private static Map getNamespacePrefixesInScopePublic(Element element) {
//			return element.getNamespacePrefixesInScope(); // (xom-1.1+)
//		}


		private static Map getNamespacePrefixesInScopeNonPublic(Element element) {
			HashMap namespaces = new HashMap();

			do {
				int size = element.getNamespaceDeclarationCount();
				for (int i = 0; i < size; i++) {
					String prefix = element.getNamespacePrefix(i);
					if (!namespaces.containsKey(prefix)) {
						String uri = element.getNamespaceURI(prefix);
						namespaces.put(prefix, uri);
					}
				}
				ParentNode parent = element.getParent();
				element = (parent instanceof Element ? (Element) parent : null);
			} while (element != null);

			return namespaces;
		}

	}

	static boolean isAtomicValue(Node node) {
		if (node instanceof Element) {
			Element elem = (Element) node;
			return elem.getLocalName().equals("atomic-value") &&
				elem.getNamespaceURI().equals("http://dsd.lbl.gov/nux");
		}
		return false;
	}

	////////////////////////////////////////////////////////////////////////////////
	// Nested classes:
	////////////////////////////////////////////////////////////////////////////////
	private static final class WrapSerializer extends SequenceSerializer {

		/** Prefabricated template elements for efficient WRAP_ALGORITHM */
		private static final HashMap TEMPLATES = initTemplates();

		public WrapSerializer(OutputStream out, String encoding)
				throws UnsupportedEncodingException {
			super(out, encoding);
		}

		public void write(Nodes nodes) throws IOException {
			Document doc = wrapSequence(nodes);
			write(doc);
		}

		/** Wraps each item in the result sequence into a decorated element wrapper. */
		private static Document wrapSequence(Nodes nodes) {
			// make a copy of the template for sequences:
			Element items = (Element) TEMPLATES.get(Nodes.class.getName());
			items = new Element(items);

			int size = nodes.size();
			for (int i=0; i < size; i++) {
				items.appendChild(wrap(nodes.get(i)));
			}

			return new Document(items);
		}

		/** Wraps (a copy of) an item into a decorated element. */
		private static Element wrap(Node node) {
			if (isAtomicValue(node)) {
				return (Element) node.copy(); // atomic values are already properly wrapped
			}

			// make a copy of the template associated with the given node type:
			Element item = (Element) TEMPLATES.get(node.getClass().getName());
			if (item == null) // FIXME: also allow Node subclasses
				throw new IllegalArgumentException("Unrecognized node type: " + node.getClass());
			item = new Element(item);

			// add copy of content to wrapper:
			if (node instanceof Attribute) {
				Attribute attr = (Attribute) node;
				item.addAttribute((Attribute) attr.copy());
			} else if (node instanceof Document) {
				Document doc = (Document) node;
				for (int j=0; j < doc.getChildCount(); j++) {
					item.appendChild(doc.getChild(j).copy());
				}
//			} else if (node instanceof Namespace) { // xom >= 1.1 only
//				Namespace ns = (Namespace) node;
////				item.addNamespaceDeclaration(ns.getPrefix(), ns.getValue());
//				if (ns.getPrefix().length() > 0) {
//					item.addAttribute(new Attribute("prefix", ns.getPrefix()));
//				}
//				item.addAttribute(new Attribute("uri", ns.getValue()));
			} else if (node instanceof DocType) {
				DocType docType = (DocType) node;
				Element e;

				e = new Element("rootName");
				e.appendChild(docType.getRootElementName());
				item.appendChild(e);

				if (docType.getPublicID() != null) {
					e = new Element("publicID");
					e.appendChild(docType.getPublicID());
					item.appendChild(e);
				}
				if (docType.getSystemID() != null) {
					e = new Element("systemID");
					e.appendChild(docType.getSystemID());
					item.appendChild(e);
				}
				if (docType.getInternalDTDSubset().length() > 0) {
					e = new Element("internalDTDSubset");
					e.appendChild(docType.getInternalDTDSubset());
					item.appendChild(e);
				}
			} else { // Element, Text, Comment, ProcessingInstruction
				item.appendChild(node.copy());
			}

			return item;
		}

		/** Prefabricate template elements for efficient WRAPPED_ALGORITHM. */
		private static HashMap initTemplates() {
			HashMap templates = new HashMap();
			String ns = "http://dsd.lbl.gov/nux";
			Element template;

			template = new Element("item:document", ns);
			templates.put(Document.class.getName(), template);

			template = new Element("item:element", ns);
			templates.put(Element.class.getName(), template);

			template = new Element("item:attribute", ns);
			templates.put(Attribute.class.getName(), template);

			template = new Element("item:text", ns);
			templates.put(Text.class.getName(), template);

			template = new Element("item:comment", ns);
			templates.put(Comment.class.getName(), template);

			template = new Element("item:pi", ns);
			templates.put(ProcessingInstruction.class.getName(), template);

			template = new Element("item:docType", ns);
			templates.put(DocType.class.getName(), template);

//			template = new Element("item:namespace", ns); // xom >= 1.1 only
//			templates.put(Namespace.class.getName(), template);

			template = new Element("item:items", ns);
			template.addNamespaceDeclaration("xsi", "http://www.w3.org/2001/XMLSchema-instance");
			templates.put(Nodes.class.getName(), template);

			return templates;
		}
	}

}
