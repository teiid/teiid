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
package nux.xom.pool;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import javax.xml.bind.JAXBException;
import javax.xml.bind.MarshalException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.UnmarshalException;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.DocumentBuilder;

import nu.xom.Attribute;
import nu.xom.Comment;
import nu.xom.DocType;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.IllegalAddException;
import nu.xom.Node;
import nu.xom.NodeFactory;
import nu.xom.Nodes;
import nu.xom.ParentNode;
import nu.xom.ProcessingInstruction;
import nu.xom.Serializer;
import nu.xom.Text;
import nu.xom.WellformednessException;
import nu.xom.XMLException;
import nu.xom.canonical.Canonicalizer;
import nu.xom.converters.DOMConverter;
import nux.xom.binary.BinaryXMLCodec;
import nux.xom.binary.NodeBuilder;
import nux.xom.io.StreamingSerializer;
import nux.xom.xquery.ResultSequenceSerializer;

import org.w3c.dom.DOMImplementation;

/**
 * Various utilities avoiding redundant code in several classes.
 * 
 * @author whoschek.AT.lbl.DOT.gov
 * @author $Author: hoschek3 $
 * @version $Revision: 1.159 $, $Date: 2006/03/24 01:17:26 $
 */
public class XOMUtil {
	
	// for indentation substring sharing in toDebugString()
	private static final String TABS = repeatString("\t", 128);
	
	/** gc'able BinaryXMLCodec instance (per thread) since a codec is not thread-safe */
	private static final ThreadLocal LOCAL_CODEC = new SoftThreadLocal() {
		protected Object initialSoftValue() { // lazy init
			return new BinaryXMLCodec();
		}
	};

	// constructing a new DocumentBuilder is VERY expensive, so we reuse it time and again
	// in a thread-safe way once it has been constructed.
	private static final ThreadLocal LOCAL_DOC_BUILDER = new SoftThreadLocal() {
		protected Object initialSoftValue() { // lazy init
			javax.xml.parsers.DocumentBuilderFactory factory =
				javax.xml.parsers.DocumentBuilderFactory.newInstance();
			factory.setNamespaceAware(true);
			try {
				DocumentBuilder docBuilder = factory.newDocumentBuilder();
				//System.err.println("found DOM DocumentBuilder="+docBuilder.getClass().getName());
				return docBuilder;
			} catch (javax.xml.parsers.ParserConfigurationException e) {
				throw new XMLException(
					"Can't find or create DOM DocumentBuilder - check your classpath", e);
			}
		}
	};
	
		
	private XOMUtil() {} // not instantiable
	
	/**
	 * Returns a thread local BinaryXMLCodec since a BinaryXMLCodec is not thread-safe.
	 */
	static BinaryXMLCodec getBinaryXMLCodec() { // TODO: make this public?
		return (BinaryXMLCodec) LOCAL_CODEC.get();
	}
	
	/**
	 * Returns a namespace-aware DOMImplementation via the default JAXP lookup mechanism.
	 * 
	 * @return a namespace-aware DOMImplementation
	 */
	public static DOMImplementation getDOMImplementation() {
		return ((DocumentBuilder) LOCAL_DOC_BUILDER.get()).getDOMImplementation();
	}
	
	/**
	 * Returns a pretty-printed String representation of the given node (subtree).
	 * 
	 * @param node the node (subtree) to convert.
	 * @return a pretty-printed String representation
	 * @see Serializer
	 */
	public static String toPrettyXML(Node node) {
		if (!(node instanceof ParentNode)) { 
			return node.toXML(); // not really pretty-printed
		}

		/*
		 * ResultSequenceSerializer ensures correct output of namespace
		 * declarations even if element is not a root element, and it does
		 * so without the potentially expensive node.copy() required by
		 * nu.xom.Serializer
		 */
		ResultSequenceSerializer serializer = new ResultSequenceSerializer();
		serializer.setIndent(4);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		Nodes nodes = new Nodes();
		nodes.append(node);
		String xml;
		try {
			serializer.write(nodes, out);
			xml = out.toString("UTF-8"); // safe: UTF-8 support is required by JDK spec
		} catch (IOException e) {
			throw new RuntimeException("should never happen", e);
		}
		
		// remove XML declaration header <?xml version="1.0" encoding="UTF-8"?>\r\n
		// remove trailing line break, if any
		xml = xml.substring(xml.indexOf('>') + 1);
		if (xml.startsWith("\r\n")) xml = xml.substring(2);
		int j = xml.length();
		if (xml.endsWith("\r\n")) j = j - 2;
		else if (xml.endsWith("\n")) j = j - 1;
		return xml.substring(0, j);
	}
	
	/**
	 * Returns the W3C Canonical XML representation of the given document.
	 * 
	 * @param doc the document to convert.
	 * @return the bytes representing canonical XML
	 * @see Canonicalizer
	 */
	public static byte[] toCanonicalXML(Document doc) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		Canonicalizer canon = new Canonicalizer(out);
		try {
			canon.write(doc);
		} catch (IOException e) {
			throw new RuntimeException("should never happen", e);
		} catch (NoSuchMethodError e) {
			try { // xom < 1.1 uses write(Document) signature
				Canonicalizer.class.getMethod("write", new Class[] {Document.class});
			} catch (Exception e1) {
				throw new RuntimeException(e1);	
			}
		}
		return out.toByteArray();
	}
	
	/**
	 * Returns a properly indented debug level string representation of the
	 * entire given XML node subtree, decorated with node types, node names,
	 * children, etc. For instance, this can be used to find structural diffs,
	 * to detect anomalies wrt. empty texts, whitespace text, etc.
	 * Applications could use this in combination with a {@link Normalizer}.
	 * 
	 * @param node
	 *            the subtree to display
	 * @return a string representation for debugging purposes.
	 */
	public static String toDebugString(Node node) {
		int depth = 0;
		String indent = TABS.substring(0, depth);
		StringBuffer result = new StringBuffer(128);
		
		result.append(indent);
		result.append(node); // also works if node == null
		result.append('\n');
		
		if (node instanceof ParentNode) { // print subtree contents
			toDebugString((ParentNode) node, depth+1, result);
		}
		
		result.deleteCharAt(result.length()-1); // remove trailing '\n'
		return result.toString();
	}

	private static void toDebugString(ParentNode node, int depth, StringBuffer result) {
		String indent = TABS.substring(0, depth); // substring sharing
		
		// print attributes
		if (node instanceof Element) {
			Element elem = (Element) node;
			for (int i=0; i < elem.getAttributeCount(); i++) {
				result.append(indent);
				result.append(elem.getAttribute(i).toString());
				result.append('\n');
			}
		}
		
		// omitting namespace declarations for now
		
		// print children
		for (int i=0; i < node.getChildCount(); i++) {
			Node child = node.getChild(i);
			result.append(indent);
			result.append(child.toString());
			result.append('\n');
			if (child instanceof Element) { // recurse
				toDebugString((Element)child, depth+1, result);
			}
		}
	}
	
	private static String repeatString(String str, int times) {
		StringBuffer buf = new StringBuffer(str.length() * times);
		for (int i=0; i < times; i++) buf.append(str);
		return buf.toString();
	}
	
	/**
	 * Returns the XOM document obtained by parsing from the content of the
	 * given XML string. Useful for quick'n dirty inline examples and tests. The
	 * document is parsed with a non-validating Builder, and the baseURI of the
	 * document will be the empty string.
	 * <p>
	 * Example usage:
	 * <pre>
	 * String xml = 
	 *     "&lt;foo>" +
	 *         "&lt;bar size='123'>" +
	 *             "hello world" +
	 *         "&lt;/bar>" +
	 *     "&lt;/foo>";
	 * Document doc = toDocument(xml);
	 * System.out.println(doc.toXML());
	 * </pre>
	 * 
	 * @param xml
	 *            the string to parse from
	 * @return the corresponding XOM document
	 * @throws XMLException
	 *             if the content of the string to parse is not well-formed XML.
	 * @see nu.xom.Builder#build(String, String)
	 */
	public static Document toDocument(String xml) {
		try { // no need to be inefficient by default
			return BuilderPool.GLOBAL_POOL.getBuilder(false).build(xml, "");
		} catch (Exception e) { // part of the "convenience"
			throw new XMLException(e.getMessage(), e);
		}
	}
			
	/**
	 * Returns a node factory that removes each {@link nu.xom.Text} node that is
	 * empty or consists of whitespace characters only (boundary whitespace).
	 * This method fully preserves narrative <code>Text</code> containing
	 * whitespace along with other characters.
	 * <p>
	 * Otherwise this factory behaves just like the standard {@link NodeFactory}.
	 * <p>
	 * Ignoring whitespace-only nodes reduces memory footprint for documents
	 * that are heavily pretty printed and indented, i.e. human-readable.
	 * Remember that without such a factory, <i>every </i> whitespace sequence
	 * occurring between element tags generates a mostly useless Text node.
	 * <p>
	 * Finally, note that this method's whitespace pruning is appropriate for
	 * many, but not all XML use cases (round-tripping). For example, the blank
	 * between
	 * <code>&lt;p>&lt;strong>Hello&lt;/strong> &lt;em>World!&lt;/em>&lt;/p></code>
	 * will be removed, which might not be what you want. This is because this
	 * method does not look across multiple Text nodes.
	 * 
	 * @return a node factory
	 */
	public static NodeFactory getIgnoreWhitespaceOnlyTextNodeFactory() {
		return new NodeFactory() {
			private final Nodes NONE = new Nodes();
			
			public Nodes makeText(String text) {
				return Normalizer.isWhitespaceOnly(text) ?
					NONE :
					super.makeText(text); 	
			}			
		};
	}
		
	/**
	 * Returns a factory that delegates all calls to the given child factory,
	 * logging each call to the given log stream (typically System.err) for
	 * simple debugging purposes.
	 * 
	 * @param child
	 *            the factory to delegate to
	 * @param log
	 *            the print stream to log to (typically System.err)
	 * @param logName
	 *            a name for this logger (typically "log" or similar)
	 * @return a logging node factory
	 */
	public static NodeFactory getLoggingNodeFactory(final NodeFactory child, 
			final PrintStream log, final String logName) {
		
		if (child == null) 
			throw new IllegalArgumentException("child must not be null");
		if (log == null) 
			throw new IllegalArgumentException("logStream must not be null");
		
		return new NodeFactory() {		
			private int level = 0;
			
			public Nodes makeAttribute(String name, String URI, String value, Attribute.Type type) {
				log("", new Attribute(name, URI, value, type));
				return child.makeAttribute(name, URI, value, type);
			}
	
			public Nodes makeComment(String data) {
				log("", new Comment(data));
				return child.makeComment(data);
			}
	
			public Nodes makeDocType(String rootElementName, String publicID, String systemID) {
				log("", new DocType(rootElementName, publicID, systemID));
				return child.makeDocType(rootElementName, publicID, systemID);
			}
	
			public Nodes makeProcessingInstruction(String target, String data) {
				log("", new ProcessingInstruction(target, data));
				return child.makeProcessingInstruction(target, data);
			}
			
			public Nodes makeText(String text) {
				log("", new Text(text));
				return child.makeText(text);
			}
	
			public Element makeRootElement(String name, String namespace) {
				log("startRoot", new Element(name, namespace));
				level++;
				return child.makeRootElement(name, namespace);
			}
	
			public Element startMakingElement(String name, String namespace) {
				log("start", new Element(name, namespace));
				Element elem = child.startMakingElement(name, namespace);
				if (elem == null) 
					log("SKIP ", new Element(name, namespace));
				else 
					level++;
				return elem;
			}
			
			public Nodes finishMakingElement(Element element) {
				level--;
				log("finish", element);
				
				ParentNode parent = null;
				if (element != null) parent = element.getParent();
				String parents = "{";
				while (parent != null) {
					parents += parent.toString() + ",";
					parent = parent.getParent();
				}
				if (parents.endsWith(",")) parents = parents.substring(0, parents.length()-1);
				parents += "}";
				log("parents", parents);
				
				return child.finishMakingElement(element);
			}
			
			public Document startMakingDocument() {
				level = 0;
				log("startDoc", null);
				return child.startMakingDocument();
			}

			public void finishMakingDocument(Document document) {
				log("finishDoc", document);
				level = 0;
				child.finishMakingDocument(document);
			}
			
			private void log(String msg, Object node) {
				if (msg == null) msg = "";
				String indent;
				if (level <= TABS.length()) 
					indent = TABS.substring(0, Math.max(0, level));
				else 
					indent = repeatString("\t", level);
				
				String s = indent + logName;
				if (msg.length() > 0) s += ":" + msg;
				s += ":" + node;
				log.println(s);
			}
		};
	}
	
	/**
	 * Returns a node factory that removes leading and trailing whitespaces in
	 * each {@link nu.xom.Text} node, altogether removing a Text node that becomes 
	 * empty after said trimming (ala {@link String#trim()}). For example a
	 * text node of <code>"  hello world   "</code> becomes
	 * <code>"hello world"</code>, and a text node of <code>"   "</code> is
	 * removed.
	 * <p>
	 * Otherwise this factory behaves just like the standard {@link NodeFactory}.
	 * <p>
	 * Finally, note that this method's whitespace pruning is appropriate for
	 * many, but not all XML use cases (round-tripping).
	 * 
	 * @return a node factory
	 */
	public static NodeFactory getTextTrimmingNodeFactory() {
		return new NodeFactory() {
			private final Nodes NONE = new Nodes();
			
			public Nodes makeText(String text) {
				text = Normalizer.trim(text);
				return text.length() == 0 ? NONE : super.makeText(text);
			}
		};
	}
	
	/**
	 * Returns a node factory for pure document validation. This factory does
	 * not generate a document on <code>Builder.build(...)</code>, which is
	 * not required anyway for pure validation. Ignores all input and builds an
	 * empty document instead. This improves validation performance.
	 * 
	 * @return a node factory
	 */
	public static NodeFactory getNullNodeFactory() {
		return new NodeFactory() {
			private final Nodes NONE = new Nodes();
						
			public Nodes makeAttribute(String name, String URI, String value, Attribute.Type type) {
				return NONE;
			}
	
			public Nodes makeComment(String data) {
				return NONE;
			}
	
			public Nodes makeDocType(String rootElementName, String publicID, String systemID) {
				return NONE;
			}
	
			public Nodes makeProcessingInstruction(String target, String data) {
				return NONE;
			}
			
			public Element makeRootElement(String name, String namespace) {
				return new Element(name, namespace);
			}
	
			public Nodes makeText(String text) {
				return NONE;
			}
	
			public Element startMakingElement(String name, String namespace) {
				return null;
			}
			
			public Document startMakingDocument() {
				return new Document(new Element("dummy")); // unused dummy
			}
		};
	}

	/**
	 * Returns a node factory that redirects its input onto the output of a
	 * streaming serializer. For example can be used to convert standard textual
	 * XML to and from bnux binary XML. Works in a fully streaming fashion, that
	 * is, without building a complete temporary XOM main memory tree.
	 * <p>
	 * The document returned on <code>finishMakingDocument</code> will be empty.
	 * 
	 * @param serializer
	 *            the streaming serializer to write to
	 * @return a redirecting node factory
	 */
	public static NodeFactory getRedirectingNodeFactory(
			final StreamingSerializer serializer) {
		
		if (serializer == null) throw new IllegalArgumentException(
				"Streaming serializer must not be null");
		
		/*
		 * Buffers a start tag until attributes and namespaces have been attached
		 * by the Builder, and only then calls serializer.writeStartTag(Elem).
		 */
		return new NodeFactory() {
			
			private Element buffer = null;
			private final Nodes NONE = new Nodes();
			private final NodeBuilder nodeBuilder = new NodeBuilder();
						
			public Nodes makeAttribute(String name, String namespace, 
					String value, Attribute.Type type) {
				
				buffer.addAttribute(
					nodeBuilder.createAttribute(name, namespace, value, type));
//				buffer.addAttribute(
//					new Attribute(name, namespace, value, type));
				return NONE;
			}
	
			public Nodes makeComment(String data) {
				flush();
				try {
					serializer.write(new Comment(data));
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				return NONE;
			}
	
			public Nodes makeDocType(String rootElementName, String publicID, String systemID) {
				flush();
				try {
					serializer.write(new DocType(rootElementName, publicID, systemID));
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				return NONE;
			}
	
			public Nodes makeProcessingInstruction(String target, String data) {
				flush();
				try {
					serializer.write(new ProcessingInstruction(target, data));
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				return NONE;
			}
			
			public Nodes makeText(String text) {
				flush();
				try {
					serializer.write(new Text(text));
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				return NONE;
			}
			
			public Element startMakingElement(String name, String namespace) {
				flush();
//				buffer = new Element(name, namespace);
				buffer = nodeBuilder.createElement(name, namespace);
				return buffer;
			}
			
			public Nodes finishMakingElement(Element element) {
				flush();
				try {
					serializer.writeEndTag();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				
				if (element.getParent() instanceof Document) {
					return new Nodes(element);
				}
				return NONE;
			}
			
			public Document startMakingDocument() {
				buffer = null;
				try {
					serializer.writeXMLDeclaration();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				return new Document(new Element("dummy")); // unused dummy
			}
			
			public void finishMakingDocument(Document document) {
				buffer = null;
				try {
					serializer.writeEndDocument();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
			
			private void flush() {
				if (buffer != null) {
					try {
						serializer.writeStartTag(buffer);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
					buffer = null;
				}		
			}
			
		};
	}

	/** little helper for safe reading of string system properties */
	static String getSystemProperty(String key, String defaults) {
		try { 
			return System.getProperty(key, defaults);
		} catch (Throwable e) { // better safe than sorry (applets, security managers, etc.) ...
			return defaults; // we can live with that
		}		
	}

	/** little helper for safe reading of boolean system properties */
	static boolean getSystemProperty(String key, boolean defaults) {
		try { 
			return "true".equalsIgnoreCase(
					System.getProperty(key, String.valueOf(defaults)));
		} catch (Throwable e) { // better safe than sorry (applets, security managers, etc.) ...
			return defaults; // we can live with that
		}		
	}

	/** little helper for safe reading of int system properties */
	static int getSystemProperty(String key, int defaults) {
		try { 
			return Integer.getInteger(key, defaults).intValue();
		} catch (Throwable e) { // better safe than sorry (applets, security managers, etc.) ...
			return defaults; // we can live with that
		}		
	}

	/** little helper for safe reading of long system properties */
	static long getSystemProperty(String key, long defaults) {
		try { 
			return Long.getLong(key, defaults).longValue();
		} catch (Throwable e) { // better safe than sorry (applets, security managers, etc.) ...
			return defaults; // we can live with that
		}		
	}
	
	/**
	 * Returns a reasonable approximation of the main memory [bytes] consumed by the
	 * given XOM subtree. Assumes that qname Strings are interned, but Text
	 * values and Attribute values are not. For simplicity, assumes no VM word
	 * boundary alignment of instance vars. Useful for memory-sensitive caches.
	 */
	static int getMemorySize(Node node) {
		// int PTR = getSystemProperty("sun.arch.data.model", 32) / 8;
		int PTR = 4; // pointer
		int HEADER = 3*PTR; // object header of any java object
		int STR = HEADER + 3*4 + PTR + HEADER + 4; // string
//		int ARRLIST = HEADER + 4 + PTR + HEADER + 4; // ArrayList
		int ARR = HEADER + 4; // Object[]

		int size = HEADER + PTR + 4; // object header + parent + siblingPosition
		if (node instanceof ParentNode) {
			ParentNode parent = (ParentNode) node;
			size += PTR + PTR + 4; // baseURI + childrenPtr + childCount
			int count = parent.getChildCount();
			if (count > 0) size += ARR + count*PTR;
			for (int i = count; --i >= 0; ) {
				size += getMemorySize(parent.getChild(i));
			}
			
			if (node instanceof Element) {
				Element elem = (Element) node;
				size += 5*PTR + 4;
				count = elem.getAttributeCount();
				if (count > 0) size += ARR + count*PTR;
				for (int i = count; --i >= 0; ) {
					size += getMemorySize(elem.getAttribute(i));
				}
				// for the moment assume no additional namespace declarations (common case)
			}
		} else if (node instanceof Attribute) {
			size += 5*PTR;
			size += STR + 2 * node.getValue().length();
		} else { // Text (and Comment, ProcessingInstruction, DocType)
			size += PTR;
			size += STR + 2 * node.getValue().length();
		}

		return size;
	}
	
	/**
	 * Marshals (serializes) the given JAXB object via the given marshaller
	 * into a new XOM Document (convenience method).
	 * <p>
	 * This implementation is somewhat inefficient but correctly does the job.
	 * There is no connection between the JAXB object tree and the XOM object tree;
	 * they are completely independent object trees without any cross-references.
	 * Hence, updates in one tree are not automatically reflected in the other tree.
	 * 
	 * @param marshaller
	 *            a JAXB serializer (note that a marshaller is typically
	 *            not thread-safe and expensive to construct; hence the recommendation 
	 *            is to use a {@link ThreadLocal} to make it thread-safe and efficient)
	 * @param jaxbObj
	 *            the JAXB object to serialize
	 * @return the new XOM document
	 * @throws JAXBException
	 *             If an unexpected problem occurred in the conversion.
	 * @throws MarshalException
	 *             If an error occurred while performing the marshal operation.
	 *             Whereever possible, one should prefer the
	 *             {@link MarshalException} over the {@link JAXBException}.
	 * @see Marshaller#marshal(java.lang.Object, org.w3c.dom.Node)
	 */
	public static Document jaxbMarshal(Marshaller marshaller, Object jaxbObj) 
			throws JAXBException {
		if (jaxbObj == null) 
			throw new IllegalArgumentException("jaxbObj must not be null");
		if (marshaller == null) 
			throw new IllegalArgumentException("marshaller must not be null");
		
		return DOMConverter.convert(jaxbMarshalDOM(marshaller, jaxbObj));
	}
	
	/**
	 * Unmarshals (deserializes) the given XOM node via the given unmarshaller
	 * into a new JAXB object (convenience method).
	 * <p>
	 * This implementation is somewhat inefficient but correctly does the job.
	 * There is no connection between the JAXB object tree and the XOM object tree;
	 * they are completely independent object trees without any cross-references.
	 * Hence, updates in one tree are not automatically reflected in the other tree.
	 * 
	 * @param unmarshaller
	 *            a JAXB deserializer (note that an unmarshaller is typically
	 *            not thread-safe and expensive to construct; hence the recommendation 
	 *            is to use a {@link ThreadLocal} to make it thread-safe and efficient)
	 * @param node
	 *            the XOM node to deserialize
	 * @return the new JAXB object
	 * @throws JAXBException
	 *             If an unexpected problem occurred in the conversion.
	 * @throws UnmarshalException
	 *             If an error occurred while performing the unmarshal operation.
	 *             Whereever possible, one should prefer the
	 *             {@link UnmarshalException} over the {@link JAXBException}.
	 * @see Unmarshaller#unmarshal(org.w3c.dom.Node)
	 */
	public static Object jaxbUnmarshal(Unmarshaller unmarshaller, ParentNode node)
			throws JAXBException {
		if (node == null) 
			throw new IllegalArgumentException("node must not be null");
		if (unmarshaller == null) 
			throw new IllegalArgumentException("unmarshaller must not be null");
		
		Document doc;
		if (node instanceof Document) {
			doc = (Document) node;
		}
		else if (node instanceof Element) {
			// do not modify elem's parent pointer
			doc = new Document((Element) node.copy());
		}
		else {
			throw new IllegalArgumentException("Illegal XOM node type" + node);
		}
		
		return jaxbUnmarshalDOM(unmarshaller, 
				DOMConverter.convert(doc, getDOMImplementation()));
	}
	
	private static org.w3c.dom.Document jaxbMarshalDOM(Marshaller marshaller, Object jaxbObj) throws JAXBException {
		DocumentBuilder docBuilder = (DocumentBuilder) LOCAL_DOC_BUILDER.get();
		org.w3c.dom.Document doc = docBuilder.newDocument();
		marshaller.marshal(jaxbObj, doc);
		return doc;
	}
	
	private static Object jaxbUnmarshalDOM(Unmarshaller unmarshaller, org.w3c.dom.Node node) throws JAXBException {
		return unmarshaller.unmarshal(node);
	}
		
//	/**
//	 * Streams (pushes) the given document through the given node factory,
//	 * returning a new result document, filtered according to the policy
//	 * implemented by the node factory. This method exactly mimics the
//	 * NodeFactory based behaviour of the XOM {@link nu.xom.Builder}. Intended
//	 * to filter a document that is already held in a main memory XOM tree,
//	 * rather than held in a file.
//	 * 
//	 * @param doc
//	 *            the document to push into the node factory
//	 * @param factory
//	 *            the node factory to stream into (may be <code>null</code>).
//	 * @return a new result document
//	 */
//	public static Document build(Document doc, NodeFactory factory) {
//		return new NodeFactoryPusher().build(doc, factory);
//	}
	
	///////////////////////////////////////////////////////////////////////////////
	// Nested classes:
	///////////////////////////////////////////////////////////////////////////////
	
	/**
	 * Streams (pushes) the given document through the given node factory.
	 */
	private static final class NodeFactoryPusher {
		
		public Document build(Document doc, NodeFactory factory) {
			if (doc == null) 
				throw new IllegalArgumentException("doc must not be null");
			if (factory == null || factory.getClass() == NodeFactory.class) {
				return new Document(doc); // no need to pipe through the default factory
			}
			
			Document result = factory.startMakingDocument();
			boolean hasRootElement = false;
			int k = 0;
			
			for (int i=0; i < doc.getChildCount(); i++) {
				Node child = doc.getChild(i);
				Nodes nodes;
				if (child instanceof Element) {
					Element elem = (Element) child;
					Element root = factory.makeRootElement(
							elem.getQualifiedName(), elem.getNamespaceURI());
					if (root == null) {
						throw new NullPointerException("Factory failed to create root element.");
					}
					result.setRootElement(root);
					appendNamespaces(elem, root);
					appendAttributes(elem, factory, root);
					build(elem, factory, root);
					nodes = factory.finishMakingElement(root);
				} else if (child instanceof Comment) {
					nodes = factory.makeComment(child.getValue());
				} else if (child instanceof ProcessingInstruction) {
					ProcessingInstruction pi = (ProcessingInstruction) child;
					nodes = factory.makeProcessingInstruction(
						pi.getTarget(), pi.getValue());
				} else if (child instanceof DocType) {
					DocType docType = (DocType) child;
					nodes = factory.makeDocType(
							docType.getRootElementName(), 
							docType.getPublicID(), 
							docType.getSystemID());
				} else {
					throw new IllegalArgumentException("Unrecognized node type");
				}
				
				// append nodes:
				for (int j=0; j < nodes.size(); j++) {
					Node node = nodes.get(j);
					if (node instanceof Element) { // replace fake root with real root
						if (hasRootElement) {
							throw new IllegalAddException(
								"Factory returned multiple root elements");
						}
						result.setRootElement((Element) node); 
						hasRootElement = true;
					} else {
						result.insertChild(node, k);
					}
					k++;
				}
			}
			
			if (!hasRootElement) throw new WellformednessException(
				"Factory attempted to remove the root element");
			factory.finishMakingDocument(result);
			return result;
		}
		
		private static void build(Element parent, NodeFactory factory, Element result) {
			for (int i=0; i < parent.getChildCount(); i++) {
				Nodes nodes;
				Node child = parent.getChild(i);
				if (child instanceof Element) {
					Element elem = (Element) child;
					Element copy = factory.startMakingElement(
							elem.getQualifiedName(), elem.getNamespaceURI());
					
					if (copy != null) {
						result.appendChild(copy);
						result = copy;
						appendNamespaces(elem, result);
						appendAttributes(elem, factory, result);
					}

					build(elem, factory, result); // recurse down
					
					if (copy == null) continue; // skip element
					result = (Element) copy.getParent(); // recurse up
					nodes = factory.finishMakingElement(copy);
					if (nodes.size()==1 && nodes.get(0)==copy) { // same node? (common case)
						continue; // optimization: no need to remove and then readd same element
					}				
					if (result.getChildCount()-1 < 0) {
						throw new XMLException("Factory has tampered with a parent pointer " + 
							"of ancestor-or-self in finishMakingElement()");
					}
					result.removeChild(result.getChildCount()-1);				
				} else if (child instanceof Text) {
					nodes = factory.makeText(child.getValue());
				} else if (child instanceof Comment) {
					nodes = factory.makeComment(child.getValue());
				} else if (child instanceof ProcessingInstruction) {
					ProcessingInstruction pi = (ProcessingInstruction) child;
					nodes = factory.makeProcessingInstruction(
						pi.getTarget(), pi.getValue());
				} else {
					throw new IllegalArgumentException("Unrecognized node type");
				}
				
				appendNodes(result, nodes);
			}		
		}
		
		// could be replaced with BinaryXMLCodec.writeNamespaceDeclarationsFast()
		private static void appendNamespaces(Element elem, Element result) {
			int count = elem.getNamespaceDeclarationCount();
			if (count == 1) 
				return; // elem.getNamespaceURI() has already been written

			for (int i = 0; i < count; i++) {
				String prefix = elem.getNamespacePrefix(i);
				String uri = elem.getNamespaceURI(prefix);
				if (prefix.equals(elem.getNamespacePrefix()) && uri.equals(elem.getNamespaceURI())) {
//					if (DEBUG) System.err.println("********** NAMESPACE IGNORED ON WRITE ***************\n");
					continue;
				}
				result.addNamespaceDeclaration(prefix, uri);
			}
		}
		
		private static void appendAttributes(Element elem, NodeFactory factory, Element result) {
			for (int i=0; i < elem.getAttributeCount(); i++) {
				Attribute attr = elem.getAttribute(i);
				appendNodes(result, 
					factory.makeAttribute(
						attr.getQualifiedName(), 
						attr.getNamespaceURI(), 
						attr.getValue(), 
						attr.getType()));
			}
		}
		
		private static void appendNodes(Element elem, Nodes nodes) {
			if (nodes != null) {
				int size = nodes.size();
				for (int i=0; i < size; i++) {
					Node node = nodes.get(i);
					if (node instanceof Attribute) {
						elem.addAttribute((Attribute) node);
					} else {
						elem.insertChild(node, elem.getChildCount());
					}
				}
			}
		}
				
	}
	
	
	///////////////////////////////////////////////////////////////////////////////
	// Nested classes:
	///////////////////////////////////////////////////////////////////////////////
	
	/**
	 * Standard XML algorithms for text and whitespace normalization (but not
	 * for Unicode normalization); type safe enum. XML whitespace is
	 * <code>' ', '\t', '\r', '\n'</code>.
	 * <p>
	 * This class is rarely needed by applications, but when it is needed it's
	 * pretty useful.
	 */
	public static class Normalizer {
		
		/**
		 * Whitespace normalization returns the string unchanged; hence
		 * indicates no whitespace normalization should be performed at all;
		 * This is typically the default for applications.
		 */
		public static final Normalizer PRESERVE = new Normalizer();
		
		/**
		 * Whitespace normalization replaces <i>each</i> whitespace character in the
		 * string with a <code>' '</code> space character.
		 */
		public static final Normalizer REPLACE = new ReplaceNormalizer();
				
		/**
		 * Whitespace normalization replaces each
		 * sequence of whitespace in the string by a single <code>' '</code>
		 * space character; Further, leading and trailing whitespaces are removed,
		 * if present, ala <code>String.trim()</code>.
		 */
		public static final Normalizer COLLAPSE = new CollapseNormalizer();
		
		/**
		 * Whitespace normalization removes leading and trailing whitespaces,
		 * if present, ala <code>String.trim()</code>.
		 */
		public static final Normalizer TRIM = new TrimNormalizer();
		
		/**
		 * Whitespace normalization removes strings that consist of
		 * whitespace-only (boundary whitespace), retaining other strings
		 * unchanged.
		 */
		public static final Normalizer STRIP = new StripNormalizer();
		
		private Normalizer() {}
		
		/**
		 * Performs XML whitespace normalization according to the chosen
		 * algorithm implemented by this type.
		 * Also see http://www.xml.com/pub/a/2000/09/27/schemas1.html?page=2
		 * 
		 * @param str
		 *            the string to normalize
		 * @return a normalized string
		 */
		String normalizeWhitespace(String str) { 
			return str; // PRESERVE by default; override for other algorithms
		}
				
		/**
		 * Recursively walks the given node subtree and merges runs of consecutive
		 * (adjacent) {@link Text} nodes (if present) into a single Text node
		 * containing their string concatenation; Empty Text nodes are removed. 
		 * If present, CDATA nodes are treated as Text nodes.
		 * <p>
		 * <i>After</i> merging consecutive Text nodes into a single Text node, the given 
		 * whitespace normalization algorithm is applied to each <i>resulting</i> 
		 * Text node.
		 * The semantics of the PRESERVE algorithm are the same as with the DOM method
		 * {@link org.w3c.dom.Node#normalize() org.w3c.dom.Node.normalize()}.
		 * <p>
		 * Note that documents built by a {@link nu.xom.Builder} with the default
		 * {@link nu.xom.NodeFactory} are guaranteed to never have adjacent or empty
		 * Text nodes. However, subsequent manual removal or insertion of nodes to
		 * the tree can cause Text nodes to become adjacent, and updates can cause
		 * Text nodes to become empty.
		 * <p>
		 * Text normalization with the whitespace PRESERVE algorithm is necessary to 
		 * achieve strictly standards-compliant
		 * XPath and XQuery semantics if a query compares or extracts the value of
		 * individual Text nodes that (unfortunately) happen to be adjacent to
		 * other Text nodes. Luckily, such use cases are rare in practical
		 * real-world scenarios and thus a user hardly ever needs to call this method
		 * before passing a XOM tree into XQuery or XPath.
		 * <p>
		 * Example Usage:
		 * 
		 * <pre>
		 * Element foo = new Element("foo");
		 * foo.appendChild("");
		 * foo.appendChild("bar");
		 * foo.appendChild("");
		 * 
		 * Element elem = new Element("elem");
		 * elem.appendChild("");
		 * elem.appendChild(foo);
		 * elem.appendChild("hello   ");
		 * elem.appendChild("world");
		 * elem.appendChild(" \n");
		 * elem.appendChild(foo.copy());
		 * elem.appendChild("");
		 * 
		 * XOMUtil.Normalizer.PRESERVE.normalize(elem);
		 * System.out.println(XOMUtil.toDebugString(elem));
		 * </pre>
		 * 
		 * PRESERVE yields the following normalized output:
		 * 
		 * <pre>
		 * [nu.xom.Element: elem]
		 *     [nu.xom.Element: foo]
		 *         [nu.xom.Text: bar]
		 *     [nu.xom.Text: hello   world \n]
		 *     [nu.xom.Element: foo]
		 *         [nu.xom.Text: bar]
		 * </pre>
		 * 
		 * In contrast, REPLACE yields the following hello world form:
		 * 
		 * <pre>
		 * [nu.xom.Element: elem]
		 *     [nu.xom.Element: foo]
		 *         [nu.xom.Text: bar]
		 *     [nu.xom.Text: hello   world  ]
		 *     [nu.xom.Element: foo]
		 *         [nu.xom.Text: bar]
		 * </pre>
		 * 
		 * Whereas, COLLAPSE yields:
		 * 
		 * <pre>
		 * [nu.xom.Element: elem]
		 *     [nu.xom.Element: foo]
		 *         [nu.xom.Text: bar]
		 *     [nu.xom.Text: hello world]
		 *     [nu.xom.Element: foo]
		 *         [nu.xom.Text: bar]
		 * </pre>
		 * 
		 * TRIM yields:
		 * 
		 * <pre>
		 * [nu.xom.Element: elem]
		 *     [nu.xom.Element: foo]
		 *         [nu.xom.Text: bar]
		 *     [nu.xom.Text: hello   world]
		 *     [nu.xom.Element: foo]
		 *         [nu.xom.Text: bar]
		 * </pre>
		 * 
		 * Finally, STRIP yields the same as PRESERVE because the example has no 
		 * whitepace-only results:
		 * 
		 * <pre>
		 * [nu.xom.Element: elem]
		 *     [nu.xom.Element: foo]
		 *         [nu.xom.Text: bar]
		 *     [nu.xom.Text: hello   world \n]
		 *     [nu.xom.Element: foo]
		 *         [nu.xom.Text: bar]
		 * </pre>
		 * 
		 * @param node
		 *            the subtree to normalize
		 */
		public final void normalize(ParentNode node) {
			// rather efficient implementation
			for (int i=node.getChildCount(); --i >= 0; ) {
				Node child = node.getChild(i);
				if (child instanceof Element) { // recursively walk the tree
					normalize((Element)child);
				}
				else if (child instanceof Text) {
					// scan to beginning of adjacent run, if any
					int j = i;
					while (--i >= 0 && node.getChild(i) instanceof Text) ;
					
					i++;
					if (j != i) { // > 1 adjacent Text nodes (rare case)
						merge(node, i, j); // merge into a single Text node
					} else { // found isolated Text node (common case)
						String value = child.getValue();
						String norm = normalizeWhitespace(value);
						if (norm.length() == 0) { 
							node.removeChild(i);
						} else if (!norm.equals(value)) {
							((Text) child).setValue(norm);
						}
					}
				}
			}
		}
		
		/**
		 * Found more than one adjacent Text node; merge them. Appends forwards
		 * and removes backwards to minimize memory copies of list elems.
		 */ 
		private void merge(ParentNode node, int i, int j) {
			int k = i;
			StringBuffer buf = new StringBuffer(node.getChild(k++).getValue());
			while (k <= j) buf.append(node.getChild(k++).getValue());
			k = j;
			while (k >= i) node.removeChild(k--);
			
			// replace run with compact merged Text node unless empty
			String norm = normalizeWhitespace(buf.toString());
			if (norm.length() > 0) {
				node.insertChild(new Text(norm), i);
			}	
		}
		
		/** see XML spec */
		private static boolean isWhitespace(char c) {
//			return c < ' ';
			switch (c) {
				case '\t': return true;
				case '\n': return true;
				case '\r': return true;
				case ' ' : return true;
				default  : return false;			
			}
		}
		
		private static boolean isWhitespaceOnly(String str) {
			for (int i=str.length(); --i >= 0; ) {
				if (!isWhitespace(str.charAt(i))) return false; 
			}
			return true;
		}
		
		private static String trim(String str) {
			// return str.trim();
			int j = str.length();
			int i = 0;
			while (i < j && isWhitespace(str.charAt(i))) i++;

			while (i < j && isWhitespace(str.charAt(j-1))) j--;

			return i > 0 || j < str.length() ? str.substring(i, j) : str;
		}
		
		///////////////////////////////////////////////////////////////////////////////
		// Doubly Nested classes:
		///////////////////////////////////////////////////////////////////////////////
		
		private static final class TrimNormalizer extends Normalizer {
			String normalizeWhitespace(String str) {
				return trim(str);
			}			
		}
		
		private static final class StripNormalizer extends Normalizer {
			String normalizeWhitespace(String str) {
				return isWhitespaceOnly(str) ? "" : str;
			}
		}
		
		private static final class ReplaceNormalizer extends Normalizer {
			String normalizeWhitespace(String str) {
				int len = str.length();
				StringBuffer buf = new StringBuffer(len);
				boolean modified = false; // keep identity and reduce memory if possible
				
				for (int i=0; i < len; i++) {
					char c = str.charAt(i);
					if (c != ' ' && isWhitespace(c)) { 
						c = ' ';
						modified = true;
					}
					buf.append(c);
				}
				return modified ? buf.toString() : str;
			}			
		}
		
		private static final class CollapseNormalizer extends Normalizer {
			String normalizeWhitespace(String str) {
				int len = str.length();
				StringBuffer buf = new StringBuffer(len);
				boolean modified = false; // keep identity and reduce memory if possible
				
				for (int i=0; i < len; i++) {
					char c = str.charAt(i);
					if (isWhitespace(c)) { 
						// skip to next non-whitespace
						int j = i;
						while (++i < len && isWhitespace(str.charAt(i)));
						
						i--;
						if (!modified && (c != ' ' || j != i)) modified = true;
						c = ' ';
					}
					buf.append(c);
				}
					
				/*
				 * Remove leading and trailing whitespace, if any.
				 * Consecutive leading and trailing runs have already been merged
				 * into a single space by above algorithm.
				 */
				len = buf.length();
				if (len > 0 && buf.charAt(len-1) == ' ') {
					buf.deleteCharAt(len-1);
					modified = true;
				}
				if (buf.length() > 0 && buf.charAt(0) == ' ') {
					buf.deleteCharAt(0);
					modified = true;
				}
				
				return modified ? buf.toString() : str;
			}
		}
	}
	
}