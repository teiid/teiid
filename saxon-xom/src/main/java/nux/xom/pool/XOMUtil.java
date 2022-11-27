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

import nu.xom.Element;
import nu.xom.Node;
import nu.xom.NodeFactory;
import nu.xom.Nodes;
import nu.xom.ParentNode;
import nu.xom.Text;

/**
 * Various utilities avoiding redundant code in several classes.
 *
 * @author whoschek.AT.lbl.DOT.gov
 * @author $Author: hoschek3 $
 * @version $Revision: 1.159 $, $Date: 2006/03/24 01:17:26 $
 */
public class XOMUtil {

    private XOMUtil() {} // not instantiable

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
     * <code>&lt;p&gt;&lt;strong&gt;Hello&lt;/strong&gt; &lt;em&gt;World!&lt;/em&gt;&lt;/p&gt;</code>
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



//    /**
//     * Streams (pushes) the given document through the given node factory,
//     * returning a new result document, filtered according to the policy
//     * implemented by the node factory. This method exactly mimics the
//     * NodeFactory based behaviour of the XOM {@link nu.xom.Builder}. Intended
//     * to filter a document that is already held in a main memory XOM tree,
//     * rather than held in a file.
//     *
//     * @param doc
//     *            the document to push into the node factory
//     * @param factory
//     *            the node factory to stream into (may be <code>null</code>).
//     * @return a new result document
//     */
//    public static Document build(Document doc, NodeFactory factory) {
//        return new NodeFactoryPusher().build(doc, factory);
//    }

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
//            return c < ' ';
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