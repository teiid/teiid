////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Copyright (c) 2013 Saxonica Limited.
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at http://mozilla.org/MPL/2.0/.
// This Source Code Form is "Incompatible With Secondary Licenses", as defined by the Mozilla Public License, v. 2.0.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

package net.sf.saxon.option.xom;

import net.sf.saxon.Configuration;
import net.sf.saxon.event.Receiver;
import net.sf.saxon.om.AtomicSequence;
import net.sf.saxon.om.DocumentInfo;
import net.sf.saxon.om.NamePool;
import net.sf.saxon.om.NamespaceBinding;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.om.Sequence;
import net.sf.saxon.om.SequenceIterator;
import net.sf.saxon.pattern.AnyNodeTest;
import net.sf.saxon.pattern.NameTest;
import net.sf.saxon.pattern.NodeKindTest;
import net.sf.saxon.pattern.NodeTest;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.tree.iter.AxisIterator;
import net.sf.saxon.tree.iter.EmptyAxisIterator;
import net.sf.saxon.tree.iter.SingletonIterator;
import net.sf.saxon.tree.util.FastStringBuffer;
import net.sf.saxon.tree.util.Navigator;
import net.sf.saxon.tree.util.SteppingNavigator;
import net.sf.saxon.tree.util.SteppingNode;
import net.sf.saxon.tree.wrapper.AbstractNodeWrapper;
import net.sf.saxon.tree.wrapper.SiblingCountingNode;
import net.sf.saxon.tree.wrapper.VirtualNode;
import net.sf.saxon.type.BuiltInAtomicType;
import net.sf.saxon.type.SchemaType;
import net.sf.saxon.type.Type;
import net.sf.saxon.type.Untyped;
import net.sf.saxon.value.StringValue;
import net.sf.saxon.value.UntypedAtomicValue;
import nu.xom.Attribute;
import nu.xom.Comment;
import nu.xom.DocType;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Node;
import nu.xom.ParentNode;
import nu.xom.ProcessingInstruction;
import nu.xom.Text;

/**
 * A node in the XML parse tree representing an XML element, character content,
 * or attribute.
 * <p/>
 * This is the implementation of the NodeInfo interface used as a wrapper for
 * XOM nodes.
 *
 * @author Michael H. Kay
 * @author Wolfgang Hoschek (ported net.sf.saxon.jdom to XOM)
 */

public class XOMNodeWrapper extends AbstractNodeWrapper implements VirtualNode, SiblingCountingNode, SteppingNode {

    protected Node node;

    protected short nodeKind;

    private XOMNodeWrapper parent; // null means unknown

    protected XOMDocumentWrapper docWrapper;

    //represents the index position in it's parent child nodes
    protected int index; // -1 means unknown


    /**
     * This constructor is protected: nodes should be created using the wrap
     * factory method on the XOMDocumentWrapper class
     *
     * @param node   The XOM node to be wrapped
     * @param parent The XOMNodeWrapper that wraps the parent of this node
     * @param index  Position of this node among its siblings
     */
    protected XOMNodeWrapper(Node node, XOMNodeWrapper parent, int index) {
        short kind;
        if (node instanceof Element) {
            kind = Type.ELEMENT;
        } else if (node instanceof Text) {
            kind = Type.TEXT;
        } else if (node instanceof Attribute) {
            kind = Type.ATTRIBUTE;
        } else if (node instanceof Comment) {
            kind = Type.COMMENT;
        } else if (node instanceof ProcessingInstruction) {
            kind = Type.PROCESSING_INSTRUCTION;
        } else if (node instanceof Document) {
            kind = Type.DOCUMENT;
        } else {
            throwIllegalNode(node); // moved out of fast path to enable better inlining
            return; // keep compiler happy
        }
        nodeKind = kind;
        this.node = node;
        this.parent = parent;
        this.index = index;
    }

    /**
     * Factory method to wrap a XOM node with a wrapper that implements the
     * Saxon NodeInfo interface.
     *
     * @param node       The XOM node
     * @param docWrapper The wrapper for the Document containing this node
     * @return The new wrapper for the supplied node
     */
    protected final XOMNodeWrapper makeWrapper(Node node, XOMDocumentWrapper docWrapper) {
        return makeWrapper(node, docWrapper, null, -1);
    }

    /**
     * Factory method to wrap a XOM node with a wrapper that implements the
     * Saxon NodeInfo interface.
     *
     * @param node       The XOM node
     * @param docWrapper The wrapper for the Document containing this node
     * @param parent     The wrapper for the parent of the XOM node
     * @param index      The position of this node relative to its siblings
     * @return The new wrapper for the supplied node
     */

    protected final XOMNodeWrapper makeWrapper(Node node, XOMDocumentWrapper docWrapper,
                                               XOMNodeWrapper parent, int index) {

        if (node == docWrapper.node) return docWrapper;
        XOMNodeWrapper wrapper = new XOMNodeWrapper(node, parent, index);
        wrapper.docWrapper = docWrapper;
        return wrapper;
    }

    private static void throwIllegalNode(/*@Nullable*/ Node node) {
        String str = node == null ?
                "NULL" :
                node.getClass() + " instance " + node.toString();
        throw new IllegalArgumentException("Bad node type in XOM! " + str);
    }


    /**
     * To implement {@link Sequence}, this method returns a singleton iterator
     * that delivers this item in the form of a sequence
     *
     * @return a singleton iterator that returns this item
     */

    public SequenceIterator iterate() {
        return SingletonIterator.makeIterator(this);
    }

    /**
     * Get the configuration
     */

    public Configuration getConfiguration() {
        return docWrapper.getConfiguration();
    }

    /**
     * Get the underlying XOM node, to implement the VirtualNode interface
     */

    public Object getUnderlyingNode() {
        return node;
    }


    /**
     * Get the name pool for this node
     *
     * @return the NamePool
     */

    public NamePool getNamePool() {
        return docWrapper.getNamePool();
    }

    /**
     * Return the type of node.
     *
     * @return one of the values Node.ELEMENT, Node.TEXT, Node.ATTRIBUTE, etc.
     */

    public int getNodeKind() {
        return nodeKind;
    }

    /**
     * Get the typed value.
     *
     * @return the typed value. If requireSingleton is set to true, the result
     *         will always be an AtomicValue. In other cases it may be a Value
     *         representing a sequence whose items are atomic values.
     * @since 8.5
     */

    public AtomicSequence atomize() {
        switch (getNodeKind()) {
            case Type.COMMENT:
            case Type.PROCESSING_INSTRUCTION:
                return new StringValue(getStringValueCS());
            default:
                return new UntypedAtomicValue(getStringValueCS());
        }
    }

    /**
     * Get the type annotation
     */

    public int getTypeAnnotation() {
        SchemaType st = getSchemaType();
        return (st == null ? -1 : st.getFingerprint());
    }

    /**
     * Get the type annotation of this node, if any. The type annotation is represented as
     * SchemaType object.
     * <p/>
     * <p>Types derived from a DTD are not reflected in the result of this method.</p>
     *
     * @return For element and attribute nodes: the type annotation derived from schema
     *         validation (defaulting to xs:untyped and xs:untypedAtomic in the absence of schema
     *         validation). For comments, text nodes, processing instructions, and namespaces: null.
     *         For document nodes, either xs:untyped if the document has not been validated, or
     *         xs:anyType if it has.
     * @since 9.4
     */

    public SchemaType getSchemaType() {
        if (getNodeKind() == Type.ATTRIBUTE) {
            return BuiltInAtomicType.UNTYPED_ATOMIC;
        } else {
            return Untyped.getInstance();
        }
    }


    /**
     * Determine whether this is the same node as another node. <br />
     * Note: a.isSameNode(b) if and only if generateId(a)==generateId(b)
     *
     * @return true if this Node object and the supplied Node object represent
     *         the same node in the tree.
     */

    public boolean isSameNodeInfo(NodeInfo other) {
        // In XOM equality means identity
        return other instanceof XOMNodeWrapper && node == ((XOMNodeWrapper) other).node;
    }

    /**
     * The equals() method compares nodes for identity. It is defined to give the same result
     * as isSameNodeInfo().
     *
     * @param other the node to be compared with this node
     * @return true if this NodeInfo object and the supplied NodeInfo object represent
     *         the same node in the tree.
     * @since 8.7 Previously, the effect of the equals() method was not defined. Callers
     *        should therefore be aware that third party implementations of the NodeInfo interface may
     *        not implement the correct semantics. It is safer to use isSameNodeInfo() for this reason.
     *        The equals() method has been defined because it is useful in contexts such as a Java Set or HashMap.
     */

    public boolean equals(Object other) {
        return other instanceof NodeInfo && isSameNodeInfo((NodeInfo) other);
    }

    /**
     * The hashCode() method obeys the contract for hashCode(): that is, if two objects are equal
     * (represent the same node) then they must have the same hashCode()
     *
     * @since 8.7 Previously, the effect of the equals() and hashCode() methods was not defined. Callers
     *        should therefore be aware that third party implementations of the NodeInfo interface may
     *        not implement the correct semantics.
     */

    public int hashCode() {
        return node.hashCode();
    }

    /**
     * Get the System ID for the node.
     *
     * @return the System Identifier of the entity in the source document
     *         containing the node, or null if not known. Note this is not the
     *         same as the base URI: the base URI can be modified by xml:base,
     *         but the system ID cannot.
     */

    public String getSystemId() {
        return docWrapper.baseURI;
    }

    public void setSystemId(String uri) {
        docWrapper.baseURI = uri;
    }

    /**
     * Get the Base URI for the node, that is, the URI used for resolving a
     * relative URI contained in the node.
     */

    public String getBaseURI() {
        return node.getBaseURI();
    }

    /**
     * Get line number
     *
     * @return the line number of the node in its original source document; or
     *         -1 if not available
     */

    public int getLineNumber() {
        return -1;
    }

    /**
     * Get column number
     *
     * @return the column number of the node in its original source document; or -1 if not available
     */

    public int getColumnNumber() {
        return -1;
    }

    /**
     * Determine the relative position of this node and another node, in
     * document order. The other node will always be in the same document.
     *
     * @param other The other node, whose position is to be compared with this
     *              node
     * @return -1 if this node precedes the other node, +1 if it follows the
     *         other node, or 0 if they are the same node. (In this case,
     *         isSameNode() will always return true, and the two nodes will
     *         produce the same result for generateId())
     */

    public int compareOrder(NodeInfo other) {
        if (other instanceof XOMNodeWrapper) {
            return compareOrderFast(node, ((XOMNodeWrapper) other).node);
        } else {
            // it must be a namespace node
            return -other.compareOrder(this);
        }
    }

    private static int compareOrderFast(Node first, Node second) {
        /*
           * Unfortunately we do not have a sequence number for each node at hand;
           * this would allow to turn the comparison into a simple sequence number
           * subtraction. Walking the entire tree and batch-generating sequence
           * numbers on the fly is no good option either. However, this rewritten
           * implementation turns out to be more than fast enough.
           */

        // assert first != null && second != null
        // assert first and second MUST NOT be namespace nodes
        if (first == second) return 0;

        ParentNode firstParent = first.getParent();
        ParentNode secondParent = second.getParent();
        if (firstParent == null) {
            if (secondParent != null) return -1; // first node is the root
            // both nodes are parentless, use arbitrary but fixed order:
            return first.hashCode() - second.hashCode();
        }

        if (secondParent == null) return +1; // second node is the root

        // do they have the same parent (common case)?
        if (firstParent == secondParent) {
            int i1 = firstParent.indexOf(first);
            int i2 = firstParent.indexOf(second);

            // note that attributes and namespaces are not children
            // of their own parent (i = -1).
            // attribute (if any) comes before child
            if (i1 != -1) return (i2 != -1) ? i1 - i2 : +1;
            if (i2 != -1) return -1;

            // assert: i1 == -1 && i2 == -1
            // i.e. both nodes are attributes
            Element elem = (Element) firstParent;
            for (int i = elem.getAttributeCount(); --i >= 0; ) {
                Attribute attr = elem.getAttribute(i);
                if (attr == second) return -1;
                if (attr == first) return +1;
            }
            throw new IllegalStateException("should be unreachable");
        }

        // find the depths of both nodes in the tree
        int depth1 = 0;
        int depth2 = 0;
        Node p1 = first;
        Node p2 = second;
        while (p1 != null) {
            depth1++;
            p1 = p1.getParent();
            if (p1 == second) return +1;
        }
        while (p2 != null) {
            depth2++;
            p2 = p2.getParent();
            if (p2 == first) return -1;
        }

        // move up one branch of the tree so we have two nodes on the same level
        p1 = first;
        while (depth1 > depth2) {
            p1 = p1.getParent();
            depth1--;
        }
        p2 = second;
        while (depth2 > depth1) {
            p2 = p2.getParent();
            depth2--;
        }

        // now move up both branches in sync until we find a common parent
        while (true) {
            firstParent = p1.getParent();
            secondParent = p2.getParent();
            if (firstParent == null || secondParent == null) {
                // both nodes are documentless, use arbitrary but fixed order
                // based on their root elements
                return p1.hashCode() - p2.hashCode();
                // throw new NullPointerException("XOM tree compare - internal error");
            }
            if (firstParent == secondParent) {
                return firstParent.indexOf(p1) - firstParent.indexOf(p2);
            }
            p1 = firstParent;
            p2 = secondParent;
        }
    }

    /**
     * Determine the relative position of this node and another node, in document order,
     * distinguishing whether the first node is a preceding, following, descendant, ancestor,
     * or the same node as the second.
     * <p/>
     * The other node must always be in the same tree; the effect of calling this method
     * when the two nodes are in different trees is undefined. If either node is a namespace
     * or attribute node, the method should throw UnsupportedOperationException.
     *
     * @param other The other node, whose position is to be compared with this
     *              node
     * @return {@link net.sf.saxon.om.AxisInfo#PRECEDING} if this node is on the preceding axis of the other node;
     *         {@link net.sf.saxon.om.AxisInfo#FOLLOWING} if it is on the following axis; {@link net.sf.saxon.om.AxisInfo#ANCESTOR} if the first node is an
     *         ancestor of the second; {@link net.sf.saxon.om.AxisInfo#DESCENDANT} if the first is a descendant of the second;
     *         {@link net.sf.saxon.om.AxisInfo#SELF} if they are the same node.
     * @throws UnsupportedOperationException if either node is an attribute or namespace
     * @since 9.5
     */
    public int comparePosition(NodeInfo other) {
        return Navigator.comparePosition(this, other);
    }

    /**
     * Return the string value of the node. The interpretation of this depends
     * on the type of node. For an element it is the accumulated character
     * content of the element, including descendant elements.
     *
     * @return the string value of the node
     */

    public String getStringValue() {
        return node.getValue();
    }

    /**
     * Get the value of the item as a CharSequence. This is in some cases more efficient than
     * the version of the method that returns a String.
     */

    public CharSequence getStringValueCS() {
        return node.getValue();
    }

    /**
     * Get name code. The name code is a coded form of the node name: two nodes
     * with the same name code have the same namespace URI, the same local name,
     * and the same prefix. By masking the name code with &0xfffff, you get a
     * fingerprint: two nodes with the same fingerprint have the same local name
     * and namespace URI.
     *
     * @see net.sf.saxon.om.NamePool#allocate allocate
     */

    public int getNameCode() {
        switch (nodeKind) {
            case Type.ELEMENT:
            case Type.ATTRIBUTE:
            case Type.PROCESSING_INSTRUCTION:
                return docWrapper.getNamePool().allocate(getPrefix(), getURI(),
                        getLocalPart());
            default:
                return -1;
        }
    }

    /**
     * Get fingerprint. The fingerprint is a coded form of the expanded name of
     * the node: two nodes with the same name code have the same namespace URI
     * and the same local name. A fingerprint of -1 should be returned for a
     * node with no name.
     */

    public int getFingerprint() {
        int nc = getNameCode();
        if (nc == -1) return -1;
        return nc & 0xfffff;
    }

    /**
     * Get the local part of the name of this node. This is the name after the
     * ":" if any.
     *
     * @return the local part of the name. For an unnamed node, returns "".
     */

    public String getLocalPart() {
        switch (nodeKind) {
            case Type.ELEMENT:
                return ((Element) node).getLocalName();
            case Type.ATTRIBUTE:
                return ((Attribute) node).getLocalName();
            case Type.PROCESSING_INSTRUCTION:
                return ((ProcessingInstruction) node).getTarget();
            default:
                return "";
        }
    }

    /**
     * Get the prefix of the name of the node. This is defined only for elements and attributes.
     * If the node has no prefix, or for other kinds of node, return a zero-length string.
     *
     * @return The prefix of the name of the node.
     */

    public String getPrefix() {
        switch (nodeKind) {
            case Type.ELEMENT:
                return ((Element) node).getNamespacePrefix();
            case Type.ATTRIBUTE:
                return ((Attribute) node).getNamespacePrefix();
            default:
                return "";
        }
    }

    /**
     * Get the URI part of the name of this node. This is the URI corresponding
     * to the prefix, or the URI of the default namespace if appropriate.
     *
     * @return The URI of the namespace of this node. For an unnamed node, or
     *         for a node with an empty prefix, return an empty string.
     */

    public String getURI() {
        switch (nodeKind) {
            case Type.ELEMENT:
                return ((Element) node).getNamespaceURI();
            case Type.ATTRIBUTE:
                return ((Attribute) node).getNamespaceURI();
            default:
                return "";
        }
    }

    /**
     * Get the display name of this node. For elements and attributes this is
     * [prefix:]localname. For unnamed nodes, it is an empty string.
     *
     * @return The display name of this node. For a node with no name, return an
     *         empty string.
     */

    public String getDisplayName() {
        switch (nodeKind) {
            case Type.ELEMENT:
                return ((Element) node).getQualifiedName();
            case Type.ATTRIBUTE:
                return ((Attribute) node).getQualifiedName();
            case Type.PROCESSING_INSTRUCTION:
                return ((ProcessingInstruction) node).getTarget();
            default:
                return "";
        }
    }

    /**
     * Get the NodeInfo object representing the parent of this node
     */

    public SteppingNode getParent() {
        if (parent == null) {
            ParentNode p = node.getParent();
            if (p != null) parent = makeWrapper(p, docWrapper);
        }
        return parent;
    }

    public SteppingNode getNextSibling() {
        ParentNode parenti = node.getParent();
        if (parenti == null) {
            return null;
        }
        int count = parenti.getChildCount();
        if (index != -1) {
            if ((index + 1) < count) {
                return makeWrapper(parenti.getChild(index + 1), docWrapper, parent, index + 1);
            } else {
                return null;
            }
        }
        index = parenti.indexOf(node);
        if (index + 1 < count) {
            return makeWrapper(parenti.getChild(index + 1), docWrapper, parent, index + 1);
        }
        return null;
    }

    public SteppingNode getPreviousSibling() {
        ParentNode parenti = node.getParent();
        if (parenti == null) {
            return null;
        }
        if (index != -1) {
            if ((index - 1) > 0) {
                return makeWrapper(parenti.getChild(index - 1), docWrapper, parent, index - 1);
            } else {
                return null;
            }
        }
        index = parenti.indexOf(node);
        if (index - 1 > 0) {
            return makeWrapper(parenti.getChild(index - 1), docWrapper, parent, index - 1);
        }
        return null;
    }

    public SteppingNode getFirstChild() {
        if (node.getChildCount() > 0) {
            for (int i=0; i<node.getChildCount(); i++) {
                Node n = node.getChild(i);
                if (!(n instanceof DocType)) {
                    return makeWrapper(n, docWrapper, this, 0);
                }
            }
        }
        return null;
    }

    public SteppingNode getSuccessorElement(SteppingNode anchor, String uri, String local) {
        Node stop = (anchor == null ? null : ((XOMNodeWrapper) anchor).node);
        Node next = node;
        do {
            next = getSuccessorNode(next, stop);
        } while (next != null &&
                !(next instanceof Element &&
                        (uri == null || uri.equals(((Element) next).getNamespaceURI())) &&
                        (local == null || local.equals(((Element) next).getLocalName()))));
        if (next == null) {
            return null;
        } else {
            return makeWrapper(next, docWrapper);
        }
    }

    /**
     * Get the following node in an iteration of descendants
     *
     * @param start  the start node
     * @param anchor the node marking the root of the subtree within which navigation takes place (may be null)
     * @return the next node in document order after the start node, excluding attributes and namespaces
     */

    private static Node getSuccessorNode(Node start, Node anchor) {
        if (start.getChildCount() > 0) {
            return start.getChild(0);
        }
        if (start == anchor) {
            return null;
        }
        Node p = start;
        while (true) {
            ParentNode q = p.getParent();
            if (q == null) {
                return null;
            }
            int i = q.indexOf(p) + 1;   // TODO: inefficient if a node has a large number of children
            if (i < q.getChildCount()) {
                return q.getChild(i);
            }
            if (q == anchor) {
                return null;
            }
            p = q;
        }
    }

    /**
     * Get the index position of this node among its siblings (starting from 0)
     */

    public int getSiblingPosition() {
        // This method is used only to support generate-id()
        if (index != -1) return index;
        switch (nodeKind) {
            case Type.ATTRIBUTE: {
                Attribute att = (Attribute) node;
                Element p = (Element) att.getParent();
                if (p == null) return 0;
                for (int i = p.getAttributeCount(); --i >= 0; ) {
                    if (p.getAttribute(i) == att) {
                        index = i;
                        return i;
                    }
                }
                throw new IllegalStateException("XOM node not linked to parent node");
            }

            default: {
                ParentNode p = node.getParent();
                int i = (p == null ? 0 : p.indexOf(node));
                if (i == -1) throw new IllegalStateException("XOM node not linked to parent node");
                index = i;
                return index;
            }
        }
    }

    /**
     * Return an iteration over the nodes reached by the given axis from this
     * node
     *
     * @param axisNumber
     *            the axis to be used
     * @return a SequenceIterator that scans the nodes reached by the axis in
     *         turn.
     */

    /*public AxisIterator iterateAxis(byte axisNumber) {
         return iterateAxis(axisNumber, AnyNodeTest.getInstance());
     } */

    /**
     * Return an iteration over the nodes reached by the given axis from this
     * node
     * <p/>
     * // * @param axisNumber
     * the axis to be used
     *
     * @param nodeTest A pattern to be matched by the returned nodes
     * @return a SequenceIterator that scans the nodes reached by the axis in
     *         turn.
     */

/*	public AxisIterator iterateAxis(byte axisNumber, NodeTest nodeTest) {
		// for clarifications, see the W3C specs or:
		// http://msdn.microsoft.com/library/default.asp?url=/library/en-us/xmlsdk/html/xmrefaxes.asp
		switch (axisNumber) {
		case AxisInfo.ANCESTOR:
			return new AncestorAxisIterator(this, false, nodeTest);

		case AxisInfo.ANCESTOR_OR_SELF:
			return new AncestorAxisIterator(this, true, nodeTest);

		case AxisInfo.ATTRIBUTE:
			if (nodeKind != Type.ELEMENT || ((Element) node).getAttributeCount() == 0) {
				return EmptyAxisIterator.emptyAxisIterator();
			} else {
				return new AttributeAxisIterator(this, nodeTest);
			}

		case AxisInfo.CHILD:
			if (hasChildNodes()) {
				return new ChildAxisIterator(this, true, true, nodeTest);
			} else {
				return EmptyAxisIterator.emptyAxisIterator();
			}

		case AxisInfo.DESCENDANT:
			if (hasChildNodes()) {
				return new DescendantAxisIterator(this, false, false, nodeTest);
			} else {
				return EmptyAxisIterator.emptyAxisIterator();
			}

		case AxisInfo.DESCENDANT_OR_SELF:
			if (hasChildNodes()) {
				return new DescendantAxisIterator(this, true, false, nodeTest);
			} else {
				return Navigator.filteredSingleton(this, nodeTest);
			}

		case AxisInfo.FOLLOWING:
			if (getParent() == null) {
				return EmptyAxisIterator.emptyAxisIterator();
			} else {
				return new DescendantAxisIterator(this, false, true, nodeTest);
			}

		case AxisInfo.FOLLOWING_SIBLING:
			if (nodeKind == Type.ATTRIBUTE || getParent() == null) {
				return EmptyAxisIterator.emptyAxisIterator();
			} else {
				return new ChildAxisIterator(this, false, true, nodeTest);
			}

		case AxisInfo.NAMESPACE:
			if (nodeKind == Type.ELEMENT) {
				return NamespaceNode.makeIterator(this, nodeTest);
			} else {
				return EmptyAxisIterator.emptyAxisIterator();
			}

		case AxisInfo.PARENT:
			if (getParent() == null) {
				return EmptyAxisIterator.emptyAxisIterator();
			} else {
				return Navigator.filteredSingleton(getParent(), nodeTest);
			}

		case AxisInfo.PRECEDING:
			return new PrecedingAxisIterator(this, false, nodeTest);
//			return new Navigator.AxisFilter(
//					new Navigator.PrecedingEnumeration(this, false), nodeTest);

		case AxisInfo.PRECEDING_SIBLING:
			if (nodeKind == Type.ATTRIBUTE || getParent() == null) {
				return EmptyAxisIterator.emptyAxisIterator();
			} else {
				return new ChildAxisIterator(this, false, false, nodeTest);
			}

		case AxisInfo.SELF:
			return Navigator.filteredSingleton(this, nodeTest);

		case AxisInfo.PRECEDING_OR_ANCESTOR:
			// This axis is used internally by saxon for the xsl:number implementation,
			// it returns the union of the preceding axis and the ancestor axis.
			return new PrecedingAxisIterator(this, true, nodeTest);
//			return new Navigator.AxisFilter(new Navigator.PrecedingEnumeration(
//					this, true), nodeTest);

		default:
			throw new IllegalArgumentException("Unknown axis number " + axisNumber);
		}
	}  */
    @Override
    protected AxisIterator<NodeInfo> iterateAttributes(NodeTest nodeTest) {
        return new Navigator.AxisFilter(
                new AttributeAxisIterator(this, nodeTest),
                nodeTest);
    }

    @Override
    protected AxisIterator<NodeInfo> iterateChildren(NodeTest nodeTest) {
        if (hasChildNodes()) {
            return new Navigator.AxisFilter(
                    new ChildAxisIterator(this, true, true, nodeTest),
                    nodeTest);
        } else {
            return EmptyAxisIterator.emptyAxisIterator();
        }
    }

    @Override
    protected AxisIterator<NodeInfo> iterateSiblings(NodeTest nodeTest, boolean forwards) {
        return new Navigator.AxisFilter(
                new ChildAxisIterator(this, false, forwards, nodeTest),
                nodeTest);
    }

    @Override
    protected AxisIterator<NodeInfo> iterateDescendants(NodeTest nodeTest, boolean includeSelf) {
        if (includeSelf) {
            return new SteppingNavigator.DescendantAxisIterator(this, true, nodeTest);

        } else {
            if (hasChildNodes()) {
                return new SteppingNavigator.DescendantAxisIterator(this, false, nodeTest);
            } else {
                return EmptyAxisIterator.emptyAxisIterator();
            }

        }
    }

//	private static AxisIterator makeSingleIterator(XOMNodeWrapper wrapper, NodeTest nodeTest) {
//		if (nodeTest == AnyNodeTest.getInstance() || nodeTest.matches(wrapper))
//			return SingletonIterator.makeIterator(wrapper);
//		else
//			return EmptyIterator.getInstance();
//	}

    /**
     * Get the string value of a given attribute of this node
     *
     * @param uri   the namespace URI of the attribute name. Supply the empty string for an attribute
     *              that is in no namespace
     * @param local the local part of the attribute name.
     * @return the attribute value if it exists, or null if it does not exist. Always returns null
     *         if this node is not an element.
     * @since 9.4
     */

    public String getAttributeValue(/*@NotNull*/ String uri, /*@NotNull*/ String local) {
        if (nodeKind == Type.ELEMENT) {
            Attribute att = ((Element) node).getAttribute(local, uri);
            if (att != null) {
                return att.getValue();
            }
        }
        return null;
    }

    /**
     * Get the root node of the tree containing this node
     *
     * @return the NodeInfo representing the top-level ancestor of this node.
     *         This will not necessarily be a document node
     */

    public NodeInfo getRoot() {
        return docWrapper;
    }

    /**
     * Get the root node, if it is a document node.
     *
     * @return the DocumentInfo representing the containing document.
     */

    public DocumentInfo getDocumentRoot() {
        if (docWrapper.node instanceof Document) {
            return docWrapper;
        } else {
            return null;
        }
    }

    /**
     * Determine whether the node has any children. <br />
     * Note: the result is equivalent to <br />
     * getEnumeration(Axis.CHILD, AnyNodeTest.getInstance()).hasNext()
     */

    public boolean hasChildNodes() {
        return node.getChildCount() > 0;
    }

    /**
     * Get a character string that uniquely identifies this node. Note:
     * a.isSameNode(b) if and only if generateId(a)==generateId(b)
     *
     * @param buffer a buffer to contain a string that uniquely identifies this node, across all documents
     */

    public void generateId(FastStringBuffer buffer) {
        Navigator.appendSequentialKey(this, buffer, true);
        //buffer.append(Navigator.getSequentialKey(this));
    }

    /**
     * Get the document number of the document containing this node. For a
     * free-standing orphan node, just return the hashcode.
     */

    public long getDocumentNumber() {
        return docWrapper.getDocumentNumber();
    }

    /**
     * Copy this node to a given outputter (deep copy)
     */

    public void copy(Receiver out, int copyOptions,
                     int locationId) throws XPathException {
        Navigator.copy(this, out, copyOptions, locationId);
    }

    /**
     * Get all namespace undeclarations and undeclarations defined on this element.
     *
     * @param buffer If this is non-null, and the result array fits in this buffer, then the result
     *               may overwrite the contents of this array, to avoid the cost of allocating a new array on the heap.
     * @return An array of integers representing the namespace declarations and undeclarations present on
     *         this element. For a node other than an element, return null. Otherwise, the returned array is a
     *         sequence of namespace codes, whose meaning may be interpreted by reference to the name pool. The
     *         top half word of each namespace code represents the prefix, the bottom half represents the URI.
     *         If the bottom half is zero, then this is a namespace undeclaration rather than a declaration.
     *         The XML namespace is never included in the list. If the supplied array is larger than required,
     *         then the first unused entry will be set to -1.
     *         <p/>
     *         <p>For a node other than an element, the method returns null.</p>
     */

    public NamespaceBinding[] getDeclaredNamespaces(NamespaceBinding[] buffer) {
        if (node instanceof Element) {
            Element elem = (Element) node;
            int size = elem.getNamespaceDeclarationCount();
            if (size == 0) {
                return NamespaceBinding.EMPTY_ARRAY;
            }
            NamespaceBinding[] result = (buffer == null || size > buffer.length ? new NamespaceBinding[size] : buffer);
            for (int i = 0; i < size; i++) {
                String prefix = elem.getNamespacePrefix(i);
                String uri = elem.getNamespaceURI(prefix);
                result[i] = new NamespaceBinding(prefix, uri);
            }
            if (size < result.length) {
                result[size] = null;
            }
            return result;
        } else {
            return null;
        }
    }

    /**
     * Determine whether this node has the is-id property
     *
     * @return true if the node is an ID
     */

    public boolean isId() {
        return getNodeKind() == Type.ATTRIBUTE && ((Attribute) node).getType() == Attribute.Type.ID;
    }

    /**
     * Determine whether this node has the is-idref property
     *
     * @return true if the node is an IDREF or IDREFS element or attribute
     */

    public boolean isIdref() {
        return getNodeKind() == Type.ATTRIBUTE && (
                ((Attribute) node).getType() == Attribute.Type.IDREF ||
                        ((Attribute) node).getType() == Attribute.Type.IDREFS);
    }

    /**
     * Determine whether the node has the is-nilled property
     *
     * @return true if the node has the is-nilled property
     */

    public boolean isNilled() {
        return false;
    }

    ///////////////////////////////////////////////////////////////////////////////
    // Methods to support update access
    ///////////////////////////////////////////////////////////////////////////////

    /**
     * Delete this node (that is, detach it from its parent)
     */

    public void delete() throws XPathException {
        if (parent != null) {
            if (nodeKind == Type.ATTRIBUTE) {
                ((Element) parent.node).removeAttribute((Attribute) node);
            } else {
                ((ParentNode) parent.node).removeChild(node);
            }
        }
    }

    /**
     * Insert a sequence of nodes as the first children of the target node
     * @param content  the nodes to be inserted.
     */

//    public void insertAsFirst(SequenceIterator content) throws XPathException {
//        if (!(node instanceof ParentNode)) {
//            throw new XPathException("Cannot insert children unless parent is an element or document node");
//        }
//        int i = 0;
//        while (true) {
//            NodeInfo next = (NodeInfo)content.next();
//            if (next == null) {
//                break;
//            }
//            if (next instanceof XOMNodeWrapper) {
//                Node nextNode = ((XOMNodeWrapper)next).node;
//                ParentNode existingParent = nextNode.getParent();
//                if (existingParent != null) {
//                    existingParent.removeChild(nextNode);
//                }
//                ((ParentNode)node).insertChild(nextNode, i++);
//            } else {
//                throw new XPathException("Cannot insert non-XOM node");
//            }
//        }
//    }
//
//    /**
//     * Insert a sequence of nodes as the last children of the target node
//     * @param content  the nodes to be inserted.
//     */
//
//    public void insertAsLast(SequenceIterator content) throws XPathException {
//        if (!(node instanceof ParentNode)) {
//            throw new XPathException("Cannot insert children unless parent is an element or document node");
//        }
//        while (true) {
//            NodeInfo next = (NodeInfo)content.next();
//            if (next == null) {
//                break;
//            }
//            if (next instanceof XOMNodeWrapper) {
//                Node nextNode = ((XOMNodeWrapper)next).node;
//                ParentNode existingParent = nextNode.getParent();
//                if (existingParent != null) {
//                    existingParent.removeChild(nextNode);
//                }
//                ((ParentNode)node).appendChild(nextNode);
//            } else {
//                throw new XPathException("Cannot insert non-XOM node");
//            }
//        }
//    }
//
//
//    /**
//     * Add attributes to this node
//     *
//     * @param content the attributes to be added
//     */
//
//    public void insertAttributes(SequenceIterator content) throws XPathException {
//        if (nodeKind == Type.ELEMENT) {
//            while (true) {
//            NodeInfo next = (NodeInfo)content.next();
//            if (next == null) {
//                break;
//            }
//            if (next.getNodeKind() != Type.ATTRIBUTE) {
//                throw new XPathException("Node to be inserted is not an attribute");
//            }
//            if (next instanceof XOMNodeWrapper) {
//                Node node = ((XOMNodeWrapper)next).node;
//                if (node.getParent() != null) {
//                    node = node.copy();
//                }
//                ((Element)node).addAttribute((Attribute)node);
//            } else {
//                throw new XPathException("Cannot insert non-XOM node");
//            }
//        }
//        } else {
//            throw new XPathException("Cannot insert attributes unless parent is an element node");
//        }
//    }
//
//    /**
//     * Rename this node
//     *
//     * @param newName the new name
//     */
//
//    public void rename(StructuredQName newName) throws XPathException {
//        if (node instanceof Element) {
//            ((Element)node).setNamespaceURI(newName.getNamespaceURI());
//            ((Element)node).setLocalName(newName.getLocalName());
//            ((Element)node).setNamespacePrefix(newName.getPrefix());
//        } else if (node instanceof Attribute) {
//            ((Attribute)node).setNamespace(newName.getPrefix(), newName.getNamespaceURI());
//            ((Attribute)node).setLocalName(newName.getLocalName());
//        }
//    }

    /**
     * Replace this node with a given sequence of nodes
     *
     * @param replacement the replacement nodes
     */

//    public void replace(SequenceIterator replacement) throws XPathException {
//        XOMNodeWrapper parentNode = ((XOMNodeWrapper))
//        if (getPar) {
//            throw new XPathException("Cannot replace node unless parent is an element or document node");
//        }
//        while (true) {
//            NodeInfo next = (NodeInfo)content.next();
//            if (next == null) {
//                break;
//            }
//            if (next instanceof XOMNodeWrapper) {
//                Node nextNode = ((XOMNodeWrapper)next).node;
//                ParentNode existingParent = nextNode.getParent();
//                if (existingParent != null) {
//                    existingParent.removeChild(nextNode);
//                }
//                ((ParentNode)node).appendChild(nextNode);
//            } else {
//                throw new XPathException("Cannot insert non-XOM node");
//            }
//        }
//    }

    /**
     * Replace the string-value of this node
     *
     * @param stringValue the new string value
     */

//    public void replaceStringValue(CharSequence stringValue) throws XPathException {
//        switch (nodeKind) {
//            case Type.ATTRIBUTE:
//                ((Attribute)node).setValue(stringValue.toString());
//            case Type.
//        }
//    }

    ///////////////////////////////////////////////////////////////////////////////
    // Axis enumeration classes
    ///////////////////////////////////////////////////////////////////////////////

    /**
     * Handles the ancestor axis in a rather direct manner.
     */
    private final class AncestorAxisIterator implements AxisIterator {

        private XOMNodeWrapper start;
        private boolean includeSelf;

        private NodeInfo current;

        private NodeTest nodeTest;
        private int position;

        public AncestorAxisIterator(XOMNodeWrapper start, boolean includeSelf, NodeTest test) {
            // use lazy instead of eager materialization (performance)
            this.start = start;
            if (test == AnyNodeTest.getInstance()) test = null;
            nodeTest = test;
            if (!includeSelf) {
                current = start;
            }
            this.includeSelf = includeSelf;
            position = 0;
        }

        /**
         * Move to the next node, without returning it. Returns true if there is
         * a next node, false if the end of the sequence has been reached. After
         * calling this method, the current node may be retrieved using the
         * current() function.
         */

        public boolean moveNext() {
            return (next() != null);
        }

        public NodeInfo next() {
            NodeInfo curr;
            do { // until we find a match
                curr = advance();
            }
            while (curr != null && nodeTest != null && (!nodeTest.matches(curr)));

            if (curr != null) position++;
            current = curr;
            return curr;
        }

        private NodeInfo advance() {
            if (current == null)
                current = start;
            else
                current = current.getParent();

            return current;
        }

        public NodeInfo current() {
            return current;
        }

        public int position() {
            return position;
        }

        public void close() {
        }

        /**
         * Return an iterator over an axis, starting at the current node.
         *
         * @param axis the axis to iterate over, using a constant such as
         *             {@link net.sf.saxon.om.AxisInfo#CHILD}
         * @param test a predicate to apply to the nodes before returning them.
         * @throws NullPointerException if there is no current node
         */

        public AxisIterator iterateAxis(byte axis, NodeTest test) {
            return current.iterateAxis(axis, test);
        }

        /**
         * Return the atomized value of the current node.
         *
         * @return the atomized value.
         * @throws NullPointerException if there is no current node
         */

        public Sequence atomize() throws XPathException {
            return current.atomize();
        }

        /**
         * Return the string value of the current node.
         *
         * @return the string value, as an instance of CharSequence.
         * @throws NullPointerException if there is no current node
         */

        public CharSequence getStringValue() {
            return current.getStringValue();
        }

        /*@NotNull*/
        public AxisIterator getAnother() {
            return new AncestorAxisIterator(start, includeSelf, nodeTest);
        }

        public int getProperties() {
            return 0;
        }

    } // end of class AncestorAxisIterator

    /**
     * Handles the attribute axis in a rather direct manner.
     */
    private final class AttributeAxisIterator implements AxisIterator {

        private XOMNodeWrapper start;

        private NodeInfo current;
        private int cursor;

        private NodeTest nodeTest;
        private int position;

        public AttributeAxisIterator(XOMNodeWrapper start, NodeTest test) {
            // use lazy instead of eager materialization (performance)
            this.start = start;
            if (test == AnyNodeTest.getInstance()) test = null;
            nodeTest = test;
            position = 0;
            cursor = 0;
        }

        /**
         * Move to the next node, without returning it. Returns true if there is
         * a next node, false if the end of the sequence has been reached. After
         * calling this method, the current node may be retrieved using the
         * current() function.
         */

        public boolean moveNext() {
            return (next() != null);
        }


        public NodeInfo next() {
            NodeInfo curr;
            do { // until we find a match
                curr = advance();
            }
            while (curr != null && nodeTest != null && (!nodeTest.matches(curr)));

            if (curr != null) position++;
            current = curr;
            return curr;
        }

        private NodeInfo advance() {
            Element elem = (Element) start.node;
            if (cursor == elem.getAttributeCount()) return null;
            NodeInfo curr = makeWrapper(elem.getAttribute(cursor), docWrapper, start, cursor);
            cursor++;
            return curr;
        }

        public NodeInfo current() {
            return current;
        }

        public int position() {
            return position;
        }

        public void close() {
        }

        /**
         * Return an iterator over an axis, starting at the current node.
         *
         * @param axis the axis to iterate over, using a constant such as
         *             {@link net.sf.saxon.om.AxisInfo#CHILD}
         * @param test a predicate to apply to the nodes before returning them.
         * @throws NullPointerException if there is no current node
         */

        public AxisIterator iterateAxis(byte axis, NodeTest test) {
            return current.iterateAxis(axis, test);
        }

        /**
         * Return the atomized value of the current node.
         *
         * @return the atomized value.
         * @throws NullPointerException if there is no current node
         */

        public Sequence atomize() throws XPathException {
            return current.atomize();
        }

        /**
         * Return the string value of the current node.
         *
         * @return the string value, as an instance of CharSequence.
         * @throws NullPointerException if there is no current node
         */

        public CharSequence getStringValue() {
            return current.getStringValue();
        }

        /*@NotNull*/
        public AxisIterator getAnother() {
            return new AttributeAxisIterator(start, nodeTest);
        }

        public int getProperties() {
            return 0;
        }

    } // end of class AttributeAxisIterator

    /**
     * The class ChildAxisIterator handles not only the child axis, but also the
     * following-sibling and preceding-sibling axes. It can also iterate the
     * children of the start node in reverse order, something that is needed to
     * support the preceding and preceding-or-ancestor axes (the latter being
     * used by xsl:number)
     */
    private final class ChildAxisIterator implements AxisIterator {

        private XOMNodeWrapper start;
        private XOMNodeWrapper commonParent;
        private int ix;
        private boolean downwards; // iterate children of start node (not siblings)
        private boolean forwards; // iterate in document order (not reverse order)

        private NodeInfo current;
        private ParentNode par;
        private int cursor;

        private NodeTest nodeTest;
        private int position;

        private ChildAxisIterator(XOMNodeWrapper start, boolean downwards, boolean forwards, NodeTest test) {
            this.start = start;
            this.downwards = downwards;
            this.forwards = forwards;

            if (test == AnyNodeTest.getInstance()) test = null;
            nodeTest = test;
            position = 0;

            commonParent = downwards ? start : (XOMNodeWrapper) start.getParent();

            par = (ParentNode) commonParent.node;
            if (downwards) {
                ix = (forwards ? 0 : par.getChildCount());
            } else {
                // find the start node among the list of siblings
//				ix = start.getSiblingPosition();
                ix = par.indexOf(start.node);
                if (forwards) ix++;
            }
            cursor = ix;
            if (!downwards && !forwards) ix--;
        }

        /**
         * Move to the next node, without returning it. Returns true if there is
         * a next node, false if the end of the sequence has been reached. After
         * calling this method, the current node may be retrieved using the
         * current() function.
         */

        public boolean moveNext() {
            return (next() != null);
        }


        public NodeInfo next() {
            NodeInfo curr;
            do { // until we find a match
                curr = advance();
            }
            while (curr != null && nodeTest != null && (!nodeTest.matches(curr)));

            if (curr != null) position++;
            current = curr;
            return curr;
        }

        private NodeInfo advance() {
            Node nextChild;
            do {
                if (forwards) {
                    if (cursor == par.getChildCount()) return null;
                    nextChild = par.getChild(cursor++);
                } else { // backwards
                    if (cursor == 0) return null;
                    nextChild = par.getChild(--cursor);
                }
            } while (nextChild instanceof DocType);
            // DocType is not an XPath node; can occur for /child::node()

            NodeInfo curr = makeWrapper(nextChild, docWrapper, commonParent, ix);
            ix += (forwards ? 1 : -1);
            return curr;
        }

        public NodeInfo current() {
            return current;
        }

        public int position() {
            return position;
        }

        public void close() {
        }

        /**
         * Return an iterator over an axis, starting at the current node.
         *
         * @param axis the axis to iterate over, using a constant such as
         *             {@link net.sf.saxon.om.AxisInfo#CHILD}
         * @param test a predicate to apply to the nodes before returning them.
         * @throws NullPointerException if there is no current node
         */

        public AxisIterator iterateAxis(byte axis, NodeTest test) {
            return current.iterateAxis(axis, test);
        }

        /**
         * Return the atomized value of the current node.
         *
         * @return the atomized value.
         * @throws NullPointerException if there is no current node
         */

        public Sequence atomize() throws XPathException {
            return current.atomize();
        }

        /**
         * Return the string value of the current node.
         *
         * @return the string value, as an instance of CharSequence.
         * @throws NullPointerException if there is no current node
         */

        public CharSequence getStringValue() {
            return current.getStringValue();
        }

        /*@NotNull*/
        public AxisIterator getAnother() {
            return new ChildAxisIterator(start, downwards, forwards, nodeTest);
        }

        public int getProperties() {
            return 0;
        }
    }

/*	*//**
     * A bit of a misnomer; efficiently takes care of descendants,
     * descentants-or-self as well as "following" axis.
     * "includeSelf" must be false for the following axis.
     * Uses simple and effective O(1) backtracking via indexOf().
     *//*
    private final class DescendantAxisIterator implements AxisIterator {

		private XOMNodeWrapper start;
		private boolean includeSelf;
		private boolean following;

		private Node anchor; // so we know where to stop the scan
		private Node currNode;
		private boolean moveToNextSibling;

		private NodeInfo current;
		private NodeTest nodeTest;
		private int position;

		private String testLocalName;
		private String testURI;

		public DescendantAxisIterator(XOMNodeWrapper start, boolean includeSelf, boolean following, NodeTest test) {
			this.start = start;
			this.includeSelf = includeSelf;
			this.following = following;
			moveToNextSibling = following;

			if (!following) anchor = start.node;
			if (!includeSelf) currNode = start.node;

			if (test == AnyNodeTest.getInstance()) { // performance hack
				test = null; // mark as AnyNodeTest
			}
			else if (test instanceof NameTest) {
				NameTest nt = (NameTest) test;
				if (nt.getPrimitiveType() == Type.ELEMENT) { // performance hack
					// mark as element name test
                    testLocalName = nt.getLocalPart();
                    testURI = nt.getNamespaceURI();
				}
			}
			else if (test instanceof NodeKindTest) {
				if (test.getPrimitiveType() == Type.ELEMENT) { // performance hack
					// mark as element type test
					testLocalName = "";
					testURI = null;
				}
			}
			nodeTest = test;
			position = 0;
		}

        *//**
     * Move to the next node, without returning it. Returns true if there is
     * a next node, false if the end of the sequence has been reached. After
     * calling this method, the current node may be retrieved using the
     * current() function.
     *//*

        public boolean moveNext() {
            return (next() != null);
        }


		public NodeInfo next() {
			NodeInfo curr;
			do { // until we find a match
				curr = advance();
			}
			while (curr != null && nodeTest != null && (! nodeTest.matches(curr)));

			if (curr != null) position++;
			current = curr;
			return curr;
		}

		// might look expensive at first glance - but it's not
		private NodeInfo advance() {
			if (currNode == null) { // if includeSelf
				currNode = start.node;
				return start;
			}

			int i;
			do {
				i = 0;
				Node p = currNode;

				if (p.getChildCount() == 0 || moveToNextSibling) { // move to next sibling

					moveToNextSibling = false; // do it just once
					while (true) {
						// if we've reached the root we're done scanning
						p = currNode.getParent();
						if (p == null) return null;

						// Note: correct even if currNode is an attribute.
						// Performance is particularly good with the O(1) patch
						// for XOM's ParentNode.indexOf()
						i = currNode.getParent().indexOf(currNode) + 1;

						if (i < p.getChildCount()) {
							break; // break out of while(true) loop; move to next sibling
						}
						else { // reached last sibling; move up
							currNode = p;
							// if we've come all the way back to the start anchor we're done
							if (p == anchor) return null;
						}
					}
				}
				currNode = p.getChild(i);
			} while (!conforms(currNode));

			// note the null here: makeNodeWrapper(parent, ...) is fast, so it
			// doesn't really matter that we don't keep a link to it.
			// In fact, it makes objects more short lived, easing pressure on
			// the VM allocator and collector for tenured heaps.
			return makeWrapper(currNode, docWrapper, null, i);
		}

		// avoids XOMNodeWrapper allocation when there's clearly a mismatch (common case)
		private boolean conforms(Node node) {
			if (testLocalName != null) { // element test?
				if (!(node instanceof Element)) return false;
				if (testURI == null) return true; // pure element type test

				// element name test
				Element elem = (Element) node;
				return testLocalName.equals(elem.getLocalName()) &&
					testURI.equals(elem.getNamespaceURI());
			}
			else { // DocType is not an XPath node; can occur for /descendants::node()
				return !(node instanceof DocType);
			}
		}

		public NodeInfo current() {
			return current;
		}

		public int position() {
			return position;
		}

        public void close() {
        }

        *//**
     * Return an iterator over an axis, starting at the current node.
     *
     * @param axis the axis to iterate over, using a constant such as
     *             {@link net.sf.saxon.om.AxisInfo#CHILD}
     * @param test a predicate to apply to the nodes before returning them.
     * @throws NullPointerException if there is no current node
     *//*

         public AxisIterator iterateAxis(byte axis, NodeTest test) {
             return current.iterateAxis(axis, test);
         }

         *//**
     * Return the atomized value of the current node.
     *
     * @return the atomized value.
     * @throws NullPointerException if there is no current node
     *//*

         public Sequence atomize() throws XPathException {
             return current.atomize();
         }

         *//**
     * Return the string value of the current node.
     *
     * @return the string value, as an instance of CharSequence.
     * @throws NullPointerException if there is no current node
     *//*

         public CharSequence getStringValue() {
             return current.getStringValue();
         }

		*//*@NotNull*//*
        public AxisIterator getAnother() {
			return new DescendantAxisIterator(start, includeSelf, following, nodeTest);
		}

 		public int getProperties() {
			return 0;
		}
	}*/

    /**
     * Efficiently takes care of preceding axis and Saxon internal preceding-or-ancestor axis.
     * Uses simple and effective O(1) backtracking via indexOf().
     * Implemented along similar lines as DescendantAxisIterator.
     */
    private final class PrecedingAxisIterator implements AxisIterator {

        private XOMNodeWrapper start;
        private boolean includeAncestors;

        private Node currNode;
        private ParentNode nextAncestor; // next ancestors to skip if !includeAncestors

        private NodeInfo current;
        private NodeTest nodeTest;
        private int position;

        private String testLocalName;
        private String testURI;

        public PrecedingAxisIterator(XOMNodeWrapper start, boolean includeAncestors, NodeTest test) {
            this.start = start;
            this.includeAncestors = includeAncestors;
            currNode = start.node;
            nextAncestor = includeAncestors ? null : start.node.getParent();

            if (test == AnyNodeTest.getInstance()) { // performance hack
                test = null; // mark as AnyNodeTest
            } else if (test instanceof NameTest) {
                NameTest nt = (NameTest) test;
                if (nt.getPrimitiveType() == Type.ELEMENT) { // performance hack
                    // mark as element name test
                    NamePool pool = getNamePool();
                    testLocalName = pool.getLocalName(nt.getFingerprint());
                    testURI = pool.getURI(nt.getFingerprint());
                }
            } else if (test instanceof NodeKindTest) {
                if (test.getPrimitiveType() == Type.ELEMENT) { // performance hack
                    // mark as element type test
                    testLocalName = "";
                    testURI = null;
                }
            }
            nodeTest = test;
            position = 0;
        }

        /**
         * Move to the next node, without returning it. Returns true if there is
         * a next node, false if the end of the sequence has been reached. After
         * calling this method, the current node may be retrieved using the
         * current() function.
         */

        public boolean moveNext() {
            return (next() != null);
        }

        public NodeInfo next() {
            NodeInfo curr;
            do { // until we find a match
                curr = advance();
            }
            while (curr != null && nodeTest != null && (!nodeTest.matches(curr)));

            if (curr != null) position++;
            current = curr;
            return curr;
        }

        // might look expensive at first glance - but it's not
        private NodeInfo advance() {
            int i;
            do {
                Node p;

                while (true) {
                    // if we've reached the root we're done scanning
//					System.out.println("p="+p);
                    p = currNode.getParent();
                    if (p == null) return null;

                    // Note: correct even if currNode is an attribute.
                    // Performance is particularly good with the O(1) patch
                    // for XOM's ParentNode.indexOf()
                    i = currNode.getParent().indexOf(currNode) - 1;

                    if (i >= 0) { // move to next sibling's last descendant node
                        p = p.getChild(i); // move to next sibling
                        int j;
                        while ((j = p.getChildCount() - 1) >= 0) { // move to last descendant node
                            p = p.getChild(j);
                            i = j;
                        }
                        break; // break out of while(true) loop
                    } else { // there are no more siblings; move up
                        // if !includeAncestors skip the ancestors of the start node
                        // assert p != null
                        if (p != nextAncestor) break; // break out of while(true) loop

                        nextAncestor = nextAncestor.getParent();
                        currNode = p;
                    }
                }
                currNode = p;

            } while (!conforms(currNode));

            // note the null here: makeNodeWrapper(parent, ...) is fast, so it
            // doesn't really matter that we don't keep a link to it.
            // In fact, it makes objects more short lived, easing pressure on
            // the VM allocator and collector for tenured heaps.
            return makeWrapper(currNode, docWrapper, null, i);
        }

        // avoids XOMNodeWrapper allocation when there's clearly a mismatch (common case)
        // same as for DescendantAxisIterator
        private boolean conforms(Node node) {
            if (testLocalName != null) { // element test?
                if (!(node instanceof Element)) {
                    return false;
                }
                if (testURI == null) {
                    return true; // pure element type test
                }

                // element name test
                Element elem = (Element) node;
                return testLocalName.equals(elem.getLocalName()) &&
                        testURI.equals(elem.getNamespaceURI());
            } else { // DocType is not an XPath node
                return !(node instanceof DocType);
            }
        }

        public NodeInfo current() {
            return current;
        }

        public int position() {
            return position;
        }

        public void close() {
        }

        /**
         * Return an iterator over an axis, starting at the current node.
         *
         * @param axis the axis to iterate over, using a constant such as
         *             {@link net.sf.saxon.om.AxisInfo#CHILD}
         * @param test a predicate to apply to the nodes before returning them.
         * @throws NullPointerException if there is no current node
         */

        public AxisIterator iterateAxis(byte axis, NodeTest test) {
            return current.iterateAxis(axis, test);
        }

        /**
         * Return the atomized value of the current node.
         *
         * @return the atomized value.
         * @throws NullPointerException if there is no current node
         */

        public Sequence atomize() throws XPathException {
            return current.atomize();
        }

        /**
         * Return the string value of the current node.
         *
         * @return the string value, as an instance of CharSequence.
         * @throws NullPointerException if there is no current node
         */

        public CharSequence getStringValue() {
            return current.getStringValue();
        }

        /*@NotNull*/
        public AxisIterator getAnother() {
            return new PrecedingAxisIterator(start, includeAncestors, nodeTest);
        }

        public int getProperties() {
            return 0;
        }
    }

}

