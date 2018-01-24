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
package nux.xom.io;

import java.io.IOException;

import nu.xom.Comment;
import nu.xom.DocType;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.ProcessingInstruction;
import nu.xom.Text;

/**
 * Using memory consumption close to zero, this interface enables writing
 * arbitrarily large XML documents onto a destination, such as an
 * <code>OutputStream</code>, SAX, StAX, DOM or bnux.
 * <p>
 * This interface is conceptually similar to the StAX
 * {@link javax.xml.stream.XMLStreamWriter} interface, except that it is more
 * XOM friendly, much easier to use (in particular with namespaces), and that
 * implementations are required to guarantee XML wellformedness due to relevant
 * sanity checks. Characters are automatically escaped wherever necessary.
 * <p>
 * Nodes must be written in document order, starting with
 * {@link #writeXMLDeclaration()}, followed by writes for the individual nodes,
 * finally finishing with {@link #writeEndDocument()}. Elements are opened and
 * closed via {@link #writeStartTag(Element)} and {@link #writeEndTag()},
 * respectively.
 * <p>
 * Implementations of this interface are retrievable via a
 * {@link StreamingSerializerFactory}.
 * <p>
 * If a document can be written successfully it can also be reparsed
 * successfully. Thus, wellformedness checks catch roundtrip bugs early, where
 * they are still cheap to fix: At the sender side rather than the (remote)
 * receiver side.
 * <p>
 * For example, any attempt to write a document containing namespace conflicts,
 * malformed attribute names or values, multiple or missing root elements, etc.
 * will throw a {@link nu.xom.WellformednessException}.
 * <p>
 * Example usage:
 * 
 * <pre>
 * StreamingSerializerFactory factory = new StreamingSerializerFactory();
 * 
 * StreamingSerializer ser = factory.createXMLSerializer(System.out, "UTF-8");
 * // StreamingSerializer ser = factory.createBinaryXMLSerializer(System.out, 0);
 * // StreamingSerializer ser = factory.createStaxSerializer(XMLStreamWriter writer);
 * 
 * ser.writeXMLDeclaration();
 * ser.writeStartTag(new Element("articles"));
 * for (int i = 0; i &lt; 1000000; i++) {
 * 	Element article = new Element("article");
 * 	article.addAttribute(new Attribute("id", String.valueOf(i)));
 * 	ser.writeStartTag(article);
 * 
 * 	ser.writeStartTag(new Element("prize"));
 * 	ser.write(new Text(String.valueOf(i * 1000)));
 * 	ser.writeEndTag(); // close prize
 * 
 * 	ser.writeStartTag(new Element("quantity"));
 * 	ser.write(new Text("hello world"));
 * 	ser.writeEndTag(); // close quantity
 * 
 * 	ser.writeEndTag(); // close article
 * }
 * ser.writeEndTag(); // close articles
 * ser.writeEndDocument();
 * </pre>
 * 
 * <p>
 * Example usage mixing streaming with convenient writing of entire
 * prefabricated subtrees. For large documents, this approach combines the
 * scalability advantages of streaming with the ease of use of (comparatively
 * small) main-memory subtree construction:
 * 
 * <pre>
 * StreamingSerializerFactory factory = new StreamingSerializerFactory();
 * 
 * StreamingSerializer ser = factory.createXMLSerializer(System.out, "UTF-8");
 * // StreamingSerializer ser = factory.createBinaryXMLSerializer(System.out, 0);
 * 
 * ser.writeXMLDeclaration();
 * ser.writeStartTag(new Element("articles"));
 * for (int i = 0; i &lt; 1000000; i++) {
 * 	Element article = new Element("article");
 * 	article.addAttribute(new Attribute("id", String.valueOf(i)));
 * 
 * 	Element prize = new Element("prize");
 * 	prize.appendChild(String.valueOf(i * 1000));
 * 	article.appendChild(prize);
 * 
 * 	Element quantity = new Element("quantity");
 * 	quantity.appendChild("hello world");
 * 	article.appendChild(quantity);
 * 
 * 	ser.write(article); // writes entire subtree
 * }
 * ser.writeEndTag(); // close articles
 * ser.writeEndDocument();
 * </pre>
 * 
 * <p>
 * Example writing the following namespaced SOAP message containing arbitrarily many
 * payload numbers in the message body:
 * 
 * <pre>
 *  &lt;?xml version="1.0" encoding="UTF-8"?>
 *  &lt;SOAP:Envelope xmlns:SOAP="http://www.w3.org/2002/06/soap-envelope" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
 *  &lt;SOAP:Header>&lt;app:foo xmlns:app="http://example.org">&lt;/app:foo>&lt;/SOAP:Header>
 *  &lt;SOAP:Body>
 *  &lt;app:payload xsi:type="decimal" xmlns:app="http://example.org">0&lt;/app:payload>
 *  &lt;app:payload xsi:type="decimal" xmlns:app="http://example.org">1&lt;/app:payload>
 *  &lt;app:payload xsi:type="decimal" xmlns:app="http://example.org">2&lt;/app:payload>
 *  &lt;/SOAP:Body>
 *  &lt;/SOAP:Envelope>
 * </pre>
 * 
 * <p>
 * The above output can be generated as follows:
 * 
 * <pre>
 * StreamingSerializerFactory factory = new StreamingSerializerFactory();
 * 
 * StreamingSerializer ser = factory.createXMLSerializer(System.out, "UTF-8");
 * // StreamingSerializer ser = factory.createBinaryXMLSerializer(System.out, 0);
 * // StreamingSerializer ser = factory.createStaxSerializer(XMLStreamWriter writer);
 * 
 * String NS_SOAP = "http://www.w3.org/2002/06/soap-envelope";
 * String NS_XSI = "http://www.w3.org/2001/XMLSchema-instance";
 * String NS_APP = "http://example.org";
 * Text lineSeparator = new Text("\n");
 * 
 * // SOAP:Envelope
 * ser.writeXMLDeclaration();
 * Element envelope = new Element("SOAP:Envelope", NS_SOAP);
 * envelope.addNamespaceDeclaration("xsi", NS_XSI);
 * ser.writeStartTag(envelope);
 * ser.write(lineSeparator);
 * 
 * // SOAP:Header
 * Element header = new Element("SOAP:Header", NS_SOAP);
 * header.appendChild(new Element("app:foo", NS_APP));
 * ser.write(header);
 * ser.write(lineSeparator);
 * 
 * // SOAP:Body
 * ser.writeStartTag(new Element("SOAP:Body", NS_SOAP));
 * 
 * // begin of user code for writing message payload:
 * for (int i = 0; i &lt; 1000000; i++) {
 *     ser.write(lineSeparator);
 *     Element payload = new Element("app:payload", NS_APP);
 *     payload.addAttribute(new Attribute("xsi:type", NS_XSI, "decimal"));
 *     payload.appendChild(new Text(String.valueOf(i)));
 *     ser.write(payload);
 * }
 * // end of user code
 * 
 * ser.write(lineSeparator);
 * ser.writeEndTag(); // close SOAP:Body
 * ser.write(lineSeparator);
 * ser.writeEndTag(); // close SOAP:Envelope
 * ser.writeEndDocument();
 * </pre>
 * 
 * @author whoschek.AT.lbl.DOT.gov
 * @author $Author: hoschek3 $
 * @version $Revision: 1.22 $, $Date: 2006/05/11 22:00:14 $
 */
public interface StreamingSerializer {
	
	/**
	 * Forces any bytes buffered by the implementation to be written onto the
	 * underlying destination.
	 * 
	 * @throws IOException
	 *             if the underlying destination encounters an I/O error
	 */
	public void flush() throws IOException;

	/**
	 * Writes the start tag for the given (potentially parentless) element; this
	 * excludes children and includes attributes and namespaces defined on this
	 * element, as if the element had as parent the element handed to the last
	 * <code>writeStartTag</code> call.
	 * <p>
	 * Corresponding closing end tags should be written via
	 * <code>writeEndTag</code>. A correct program must emit the same number
	 * of <code>writeStartTag</code> and <code>writeEndTag</code> calls.
	 * <p>
	 * The value of <code>elem.getParent()</code> is ignored. Instead, the
	 * (virtual) parent is considered to be the element passed to the last
	 * corresponding <code>writeStartTag(Element)</code> call. If
	 * there's no such last corresponding <code>writeStartTag</code> call, 
	 * then the (virtual) parent is considered to be a (virtual) document node.
	 * 
	 * @param elem
	 *            the element to write a start tag for
	 * @throws IOException
	 *             if the underlying destination encounters an I/O error
	 */
	public void writeStartTag(Element elem) throws IOException;
	
	/**
	 * Writes the corresponding closing end tag for the element handed to the
	 * last <code>writeStartTag</code> call.
	 * 
	 * @throws IOException
	 *             if the underlying destination encounters an I/O error
	 */
	public void writeEndTag() throws IOException;
	
	/**
	 * Recursively writes the entire given prefabricated document, including the
	 * XML declaration and all its descendants. This isn't particularly
	 * meaningful in a streaming scenario, and only available for completeness
	 * and ease of use.
	 * 
	 * @param doc
	 *            the document to write
	 * @throws IOException
	 *             if the underlying destination encounters an I/O error
	 */
	public void write(Document doc) throws IOException;
	
	/**
	 * Recursively writes the entire subtree rooted at the given (potentially
	 * parentless) element; this includes attributes and namespaces as if
	 * recursively calling writeStartTag/write/writeEndTag for this element and
	 * all its descendants, in document order.
	 * <p>
	 * For large documents, this method combines the scalability advantages of
	 * streaming with the ease of use of (comparatively small) main-memory
	 * subtree construction.
	 * 
	 * @param element
	 *            the root of the subtree to write
	 * @throws IOException
	 *             if the underlying destination encounters an I/O error
	 */
	public void write(Element element) throws IOException;
	
	/**
	 * Writes the given text node. 
	 * 
	 * @param text
	 *            the node to write
	 * @throws IOException
	 *             if the underlying destination encounters an I/O error
	 */
	public void write(Text text) throws IOException;
	
	/** 
	 * Writes the given comment node. 
	 * 
	 * @param comment
	 *            the node to write
	 * @throws IOException
	 *             if the underlying destination encounters an I/O error
	 */
	public void write(Comment comment) throws IOException;
	
	/** 
	 * Writes the given processing instruction node. 
	 *
	 * @param instruction
	 *            the node to write
	 * @throws IOException
	 *             if the underlying destination encounters an I/O error
	 */
	public void write(ProcessingInstruction instruction) throws IOException;
	
	/** 
	 * Writes the given document type node. 
	 *
	 * @param docType
	 *            the node to write
	 * @throws IOException
	 *             if the underlying destination encounters an I/O error
	 */
	public void write(DocType docType) throws IOException;
	
	/**
	 * Finishes writing the current document, auto-closing any remaining open
	 * element tags via <code>writeEndTag</code> calls; Implicitly calls <code>
	 * flush()</code> and releases resources.
	 * 
	 * @throws IOException
	 *             if the underlying destination encounters an I/O error
	 */
	public void writeEndDocument() throws IOException;

	/**
	 * Writes the standard XML declaration (including XML version and encoding);
	 * must be called before any other <code>write</code> flavour except
	 * <code>write(Document)</code>.
	 * 
	 * @throws IOException
	 *             if the underlying destination encounters an I/O error
	 */
	public void writeXMLDeclaration() throws IOException;
	
}