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

import java.util.Map;

import nu.xom.Attribute;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Node;
import nu.xom.NodeFactory;
import nu.xom.Nodes;
import nux.xom.pool.XOMUtil;

/**
 * Streaming path filter node factory for continuous queries and/or transformations
 * over very large or infinitely long XML input.
 * <p>
 * <b>Background</b><br>
 *
 * The W3C XQuery and XPath languages often require the <i>entire</i> input
 * document to be buffered in memory for a query to be executed in its full
 * generality [<a target="_blank"
 * href="http://www.research.ibm.com/xj/pubs/icde.pdf">Background Paper</a>,
 * <a target="_blank"
 * href="http://www-dbs.informatik.uni-heidelberg.de/publications/">More Papers</a>].
 * In other words, XQuery and XPath are hard to stream over very large or
 * infinitely long XML inputs without violating some aspects of the W3C
 * specifications. However, subsets of these languages (or simplified cousins)
 * can easily support streaming.
 * <p>
 * In fact, most use cases dealing with very large XML input documents do not
 * require the full <i>forward and backward</i> navigational capabilities of
 * XQuery and XPath <i>across independent element subtrees</i>. Rather those
 * use cases are record oriented, treating element subtrees (i.e. records)
 * independently, individually selecting/projecting/transforming record after
 * record, one record at a time. For example, consider an XML document with one
 * million records, each describing a published book, music album or web server
 * log entry. A query to find the titles of books that have more than three
 * authors looks at each record individually, hence can easily be streamed.
 * Another use case is splitting a document into several sub-documents based on
 * the content of each record.
 * <p>
 * More interestingly, consider a P2P XML content messaging router, network
 * transducer, transcoder, proxy or message queue that continuously
 * filters, transforms, routes and dispatches messages from infinitely long
 * streams, with the behaviour defined by deeply inspecting rules (i.e. queries)
 * based on content, network parameters or other metadata.
 * This class provides a convenient solution for such common use cases operating
 * on very large or infinitely long XML input. The solution uses a strongly
 * simplified location path language (which is modelled after XPath but <i>not</i>
 * XPath compliant), in combination with a {@link nu.xom.NodeFactory} and
 * an optional XQuery. The solution is not necessarily faster than
 * building the full document tree, but it consumes much less main memory.
 * <p>
 * <b>Here is how it works</b>
 *
 * You specify a simple "location path" such as <code>/books/book</code> or
 * <code>/weblogs/_2004/_05/entry</code>. The path may contain wildcards and
 * indicates which elements should be retained. All elements not matching the
 * path will be thrown away during parsing. Each retained element is fully
 * build (including its ancestors and descendants) and then made available to
 * the application via a callback to an application-provided
 * {@link StreamingTransform} object.
 * <p>
 * The <code>StreamingTransform</code> can operate on the fully build element (subtree)
 * in arbitrary ways. For example, it can simply print the element to screen or
 * disk and then forget about it. Or it can add the element (subtree) to the
 * document currently build by the {@link nu.xom.Builder}. In addition, a
 * transform can check conditions such as <i>has book more than three authors?
 * </i> A transform can also replace the element with a different element or a
 * list of arbitrary generated nodes. For example, if a book has more than three
 * authors, just the book title with a <code>authorCount</code> attribute
 * can be added to the document, instead of the entire book element subtree.
 * <p>
 * Typically, simple <code>StreamingTransforms</code> are formulated in custom
 * Java code, whereas complex ones are formulated as an XQuery.
 * <p>
 * <b>Streaming Location Path Syntax</b>
 *
 * <pre>
 * locationPath := {'/'step}...
 * step := [prefix':']localName
 * prefix := '*' | '' | XMLNamespacePrefix
 * localName := '*' | XMLLocalName
 * </pre>
 *
 * A location path consists of zero or more location steps separated by "/".
 * A step consists of an optional XML namespace prefix followed by a local name.
 * The wildcard symbol '*' means: <i>Match anything</i>.
 * An empty prefix ('') means: <i>Match if in no namespace (i.e. null namespace)</i>.
 *
 * <p>
 * Example legal location steps are:
 * <pre>
 * book       (Match elements named "book" in no namespace)
 * :book      (Match elements named "book" in no namespace)
 * bib:book   (Match elements named "book" in "bib" namespace)
 * bib:*      (Match elements with any name in "bib" namespace)
 * *:book     (Match elements named "book" in any namespace, including no namespace)
 * *:*        (Match elements with any name in any namespace, including no namespace)
 * :*         (Match elements with any name in no namespace)
 * </pre>
 *
 * Obviously, the location path language is quite simplistic, supporting the "child" axis only.
 * For example, axes such as descendant ("//"), ancestors, following, preceding, as well as
 * predicates and other XPath features are not supported. Typically, this does not matter
 * though, because a full XQuery can still be used on each element (subtree) matching the
 * location path, as follows:
 *
 * <b>Example Usage</b>
 *
 * The following is complete and efficient code for parsing and iterating through millions of
 * "person" records in a database-like XML document, printing all residents of "San Francisco",
 * while never allocating more memory than needed to hold one person element:
 * <pre>
 * StreamingTransform myTransform = new StreamingTransform() {
 *     public Nodes transform(Element person) {
 *         Nodes results = XQueryUtil.xquery(person, "name[../address/city = 'San Francisco']");
 *         if (results.size() &gt; 0) {
 *             System.out.println("name = " + results.get(0).getValue());
 *         }
 *         return new Nodes(); // mark current element as subject to garbage collection
 *     }
 * };
 *
 * // parse document with a filtering Builder
 * Builder builder = new Builder(new StreamingPathFilter("/persons/person", null)
 *     .createNodeFactory(null, myTransform));
 * builder.build(new File("/tmp/persons.xml"));
 * </pre>
 *
 * To find the title of all books that have more than three authors
 * and have 'Monterey' and 'Aquarium' somewhere in the title:
 * <pre>
 * String path = "/books/book";
 * Map prefixes = new HashMap();
 * prefixes.put("bib", "http://www.example.org/bookshelve/records");
 * prefixes.put("xsd", "http://www.w3.org/2001/XMLSchema");
 *
 * StreamingTransform myTransform = new StreamingTransform() {
 *     private Nodes NONE = new Nodes();
 *
 *     // execute XQuery against each element matching location path
 *     public Nodes transform(Element subtree) {
 *         Nodes results = XQueryUtil.xquery(subtree,
 *            "title[matches(., 'Monterey') and matches(., 'Aquarium') and count(../author) &gt; 3]");
 *
 *         for (int i=0; i &lt; results.size(); i++) {
 *             // do something useful with query results; here we just print them
 *             System.out.println(XOMUtil.toPrettyXML(results.get(i)));
 *         }
 *         return NONE; // current subtree becomes subject to garbage collection
 *         // returning empty node list removes current subtree from document being build.
 *         // returning new Nodes(subtree) retains the current subtree.
 *         // returning new Nodes(some other nodes) replaces the current subtree with
 *         // some other nodes.
 *         // if you want (SAX) parsing to terminate at this point, simply throw an exception
 *     }
 * };
 *
 * // parse document with a filtering Builder
 * StreamingPathFilter filter = new StreamingPathFilter(path, prefixes);
 * Builder builder = new Builder(filter.createNodeFactory(null, myTransform));
 * Document doc = builder.build(new File("/tmp/books.xml"));
 * System.out.println("doc.size()=" + doc.getRootElement().getChildElements().size());
 * System.out.println(XOMUtil.toPrettyXML(doc));
 * </pre>
 *
 * <p>
 * Here is a similar snippet version that takes a filtering <code>Builder</code> from a
 * thread-safe pool with optimized parser configuration:
 * <pre>
 * ...
 * ... same as above
 * ...
 * final StreamingPathFilter filter = new StreamingPathFilter(path, prefixes);
 * BuilderPool pool = new BuilderPool(100, new BuilderFactory() {
 *     protected Builder newBuilder(XMLReader parser, boolean validate) {
 *         return new Builder(parser, validate, filter.createNodeFactory(null, myTransform));
 *     }
 *   }
 * );
 *
 * Builder builder = pool.getBuilder(false);
 * Document doc = builder.build(new File("/tmp/books.xml"));
 * System.out.println("doc.size()=" + doc.getRootElement().getChildElements().size());
 * </pre>
 *
 * <b>Applicability</b>
 *
 * This class is well suited for a P2P XML content messaging router, network
 * transducer, transcoder, proxy or message queue that continuously
 * filters, transforms, routes and dispatches messages from infinitely long
 * streams.
 * <p>
 * However, this class is less suited for classic database oriented use cases.
 * Here, scalability is limited as the input stream is sequentially scanned, without
 * exploiting the indexing and random access properties typical for (relational) database
 * environments. For such database oriented use cases, consider using the <a
 * href="http://www.saxonica.com/documentation/documentation.html"> Saxon SQL
 * extensions functions </a> to XQuery, or consider building your own mixed
 * relational/XQuery integration layer, or consider using a database technology
 * with native XQuery support.
 *
 * @author whoschek.AT.lbl.DOT.gov
 * @author $Author: hoschek3 $
 * @version $Revision: 1.63 $, $Date: 2005/08/12 21:26:30 $
 */
public class StreamingPathFilter {

    private final String[] _namespaceURIs;
    private final String[] _localNames;

    /**
     * Constructs a compiled filter from the given location path and prefix
     * --&gt; namespaceURI map.
     *
     * @param locationPath
     *            the path expression to compile
     * @param prefixes
     *            a map of prefix --&gt; namespaceURI associations, each of type
     *            String --&gt; String.
     *
     * @throws StreamingPathFilterException
     *             if the location path has a syntax error
     */
    public StreamingPathFilter(String locationPath, Map prefixes) throws StreamingPathFilterException {
        if (locationPath == null)
            throw new StreamingPathFilterException("locationPath must not be null");
        if (locationPath.indexOf("//") >= 0)
            throw new StreamingPathFilterException("DESCENDANT axis is not supported");

        String path = locationPath.trim();
        if (path.startsWith("/")) path = path.substring(1);
        if (path.endsWith("/")) path = path.substring(0, path.length() - 1);
        path = path.trim();
        if (path.equals("")) path = "*:*"; // fixup "match anything"
        String[] localNames = path.split("/");
        String[] namespaceURIs = new String[localNames.length];

        // parse prefix:localName pairs and resolve prefixes to namespaceURIs
        for (int i = 0; i < localNames.length; i++) {
            int k = localNames[i].indexOf(':');
            if (k >= 0 && localNames[i].indexOf(':', k+1) >= 0)
                throw new StreamingPathFilterException(
                    "QName must not contain more than one colon: "
                    + "qname='" + localNames[i] + "', path='" + path + "'");
            if (k <= 0) {
                namespaceURIs[i] = ""; // no namespace
            } else {
                String prefix = localNames[i].substring(0, k).trim();
                if (k >= localNames[i].length() - 1)
                    throw new StreamingPathFilterException(
                        "Missing localName for prefix: " + "prefix='"
                        + prefix + "', path='" + path + "', prefixes=" + prefixes);
                if (prefix.equals("*")) {
                    // namespace is irrelevant (does not matter)
                    namespaceURIs[i] = null;
                } else {
                    // lookup namespace of uri
                    if (prefixes == null)
                        throw new StreamingPathFilterException("prefixes must not be null");
                    Object uri = prefixes.get(prefix);
                    if (uri == null)
                        throw new StreamingPathFilterException(
                            "Missing namespace for prefix: "
                            + "prefix='" + prefix + "', path='" + path
                            + "', prefixes=" + prefixes);
                    namespaceURIs[i] = uri.toString().trim();
                }
            } // end if

            localNames[i] = localNames[i].substring(k + 1).trim();
            if (localNames[i].equals("*")) {
                // localName is irrelevant (does not matter)
                localNames[i] = null;
            }
        }

        this._localNames = localNames;
        this._namespaceURIs = namespaceURIs;
//        System.err.println("localNames=" + java.util.Arrays.asList(localNames));
//        System.err.println("namespaceURIs=" + java.util.Arrays.asList(namespaceURIs));
    }

    /**
     * Creates and returns a new node factory for this path filter, to be be passed
     * to a {@link nu.xom.Builder}.
     * <p>
     * Like a <code>Builder</code>, the node factory can be reused serially,
     * but is not thread-safe because it is stateful. If you need thread-safety,
     * call this method each time you need a new node factory for a new thread.
     *
     * @param childFactory
     *            an optional factory to delegate calls to. All calls except
     *            <code>makeRootElement()</code>,
     *            <code>startMakingElement()</code> and
     *            <code>finishMakingElement()</code> are delegated to the child
     *            factory. If this parameter is <code>null</code> it defaults
     *            to the factory returned by
     *            {@link XOMUtil#getIgnoreWhitespaceOnlyTextNodeFactory()}.
     *
     * @param transform
     *            an application-specific callback called by the returned node
     *            factory whenever an element matches the filter's entire location
     *            path. May be <code>null</code> in which case the
     *            identity transformation is used, adding the matching element
     *            unchanged and "as is" to the document being build by a
     *            {@link nu.xom.Builder}.
     * @return a node factory for this path filter
     */
    public NodeFactory createNodeFactory(NodeFactory childFactory, StreamingTransform transform) {
        if (childFactory == null)
            childFactory = XOMUtil.getIgnoreWhitespaceOnlyTextNodeFactory();
        return new StreamingPathFilterNodeFactory(_localNames, _namespaceURIs, childFactory, transform);
    }

    ///////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    ///////////////////////////////////////////////////////////////////////////////
    private static final class StreamingPathFilterNodeFactory extends NodeFactory {

        private final String[] namespaceURIs;
        private final String[] localNames;
        private final StreamingTransform transform;
        private final NodeFactory child;

        private int level; // current nesting level = current location path step
        private Element mismatch; // last element that did not match path

        private final Nodes NONE = new Nodes();
        private static final boolean DEBUG = false;

        public StreamingPathFilterNodeFactory(String[] localNames, String[] namespaceURIs,
                NodeFactory child, StreamingTransform transform) {

            this.localNames = localNames;
            this.namespaceURIs = namespaceURIs;
            this.child = child;
            this.transform = transform;
        }

        public Document startMakingDocument() {
            // reset state
            level = -1;
            mismatch = null;
            return child.startMakingDocument();
        }

        public Element startMakingElement(String qname, String namespaceURI) {
            level++;
//            if (DEBUG) System.err.println("startlevel=" + level + ", name="+ qname);
            // check against path, if needed
            if (mismatch == null && level < localNames.length) {
                if (!isMatch(qname, namespaceURI)) {
                    // build this element despite mismatch;
                    // so we can reset state in finishMakingElement()
                    mismatch = super.startMakingElement(qname, namespaceURI);
                    return mismatch;
                }
            }
            if (mismatch == null) {
                // no mismatch so far, build this element
                return super.startMakingElement(qname, namespaceURI);
            } else {
                // mismatch; no need to build this element
                level--;
                return null;
            }
        }

        // does element match current level/step within location path?
        private boolean isMatch(String qname, String namespaceURI) {
            int i = qname.indexOf(':') + 1;
            String name = localNames[level];
            String uri = namespaceURIs[level];
            return
                (name == null ||
                 name.regionMatches(0, qname, i, Math.max(qname.length() - i, name.length()))
                 // faster than name.equals(qname.substring(i))
                )
                &&
                (uri == null || uri.equals(namespaceURI));
        }

        public Nodes finishMakingElement(Element elem) {
//            if (DEBUG) System.err.println("finishlevel=" + level + ", name="+ elem.getLocalName());
            if (level == 0) {
                // root element must always be present;
                // a document without root element is illegal in XOM
                mismatch = null; // help gc
                level--;
                return super.finishMakingElement(elem);
            }
            if (elem == mismatch) {
                // reset state
                mismatch = null;
                level--;
                return NONE;
            }
            if (level == localNames.length - 1) {
                // we've found an element matching the full path expression
                return transformMatch(elem);
            }

            level--;
            if (level < localNames.length - 1 && !hasChildElements(elem)) {
                // prune tree if mismatch or empty
                return NONE;
            }
            return super.finishMakingElement(elem);
        }

        private Nodes transformMatch(Element elem) {
//            if (DEBUG) System.err.println("found match at level=" + level + ":"
//                            + XOMUtil.toPrettyXML(elem));
            level--;
            if (transform == null) return super.finishMakingElement(elem);
            Nodes results = transform.transform(elem);

            // prevent potential nu.xom.MultipleParentException by detaching
            for (int i = results.size(); --i >= 0; ) {
                Node node = results.get(i);
                if (node != elem) node.detach();
            }
            return results;
        }

        // is at least one child element present?
        private boolean hasChildElements(Element elem) {
            for (int i = elem.getChildCount(); --i >= 0;) {
                if (elem.getChild(i) instanceof Element) return true;
            }
            return false;
        }

        //
        // delegating methods:
        //
        public Nodes makeComment(String data) {
            return mismatch == null ? child.makeComment(data) : NONE;
        }

        public Nodes makeText(String data) {
//            return mismatch == null ? child.makeText(data) : NONE;
            if (mismatch == null) {
                if (level == 0 && isWhitespaceOnly(data))
                    return NONE; // avoid accumulating whitespace garbage in root element (i.e. avoid hidden memory leak)
                else
                    return child.makeText(data);
            }
            return NONE;
        }

        public Nodes makeAttribute(String qname, String URI, String value, Attribute.Type type) {
            return mismatch == null ? child.makeAttribute(qname, URI, value, type) : NONE;
        }

        public Nodes makeProcessingInstruction(String target, String data) {
            return mismatch == null ? child.makeProcessingInstruction(target, data) : NONE;
        }

        public Nodes makeDocType(String rootElementName, String publicID, String systemID) {
            return child.makeDocType(rootElementName, publicID, systemID);
        }

        public void finishMakingDocument(Document document) {
            child.finishMakingDocument(document);
        }

        /** see XML spec */
        private static boolean isWhitespace(char c) {
            switch (c) {
                case '\t': return true;
                case '\n': return true;
                case '\r': return true;
                case ' ' : return true;
                default  : return false;
            }
//            // this wouldn't be quite right:
//            if (c > ' ') // see String.trim() implementation
//                return false;
//            if (! Character.isWhitespace(c))
//                return false;
        }

        private static boolean isWhitespaceOnly(String str) {
            for (int i=str.length(); --i >= 0; ) {
                if (!isWhitespace(str.charAt(i))) return false;
            }
            return true;
        }

    }

}