////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Copyright (c) 2017 Saxonica Limited.
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at http://mozilla.org/MPL/2.0/.
// This Source Code Form is "Incompatible With Secondary Licenses", as defined by the Mozilla Public License, v. 2.0.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

package net.sf.saxon.option.xom;

import net.sf.saxon.Configuration;
import net.sf.saxon.om.NamePool;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.om.TreeInfo;
import net.sf.saxon.type.SchemaType;
import net.sf.saxon.type.Type;
import net.sf.saxon.type.Untyped;
import nu.xom.Attribute;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Node;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

/**
 * The root node of an XPath tree. (Or equivalently, the tree itself).
 * <p>
 * This class is used not only for a document, but also for the root
 * of a document-less tree fragment.
 *
 * @author Michael H. Kay
 * @author Wolfgang Hoschek (ported net.sf.saxon.jdom to XOM)
 */

public class XOMDocumentWrapper extends XOMNodeWrapper implements TreeInfo {

    protected Configuration config;
    protected long documentNumber;
    private HashMap<String, NodeInfo> idIndex;
    private HashMap<String, Object> userData;

    /**
     * Create a Saxon wrapper for a XOM root node
     *
     * @param root    The XOM root node
     * @param config  The configuration which defines the name pool used for all
     *                names in this tree
     */
    public XOMDocumentWrapper(Node root, Configuration config) {
        super(root, null, 0);
        if (root.getParent() != null)
            throw new IllegalArgumentException("root node must not have a parent node");
        docWrapper = this;
        treeInfo = this;
        setConfiguration(config);
    }

    /**
     * Get the NodeInfo object representing the document node at the root of the tree
     *
     * @return the document node
     */

    public NodeInfo getRootNode() {
        return this;
    }

    /**
     * Wrap a node in the XOM document.
     *
     * @param node The node to be wrapped. This must be a node in the same
     *             document (the system does not check for this).
     * @return the wrapping NodeInfo object
     */

    public NodeInfo wrap(Node node) {
        if (node == this.node) {
            return this;
        }
        return makeWrapper(node, this);
    }

    /**
     * Set the configuration, which defines the name pool used for all names in
     * this document. This is always called after a new document has been
     * created. The implementation must register the name pool with the
     * document, so that it can be retrieved using getNamePool(). It must also
     * call NamePool.allocateDocumentNumber(), and return the relevant document
     * number when getDocumentNumber() is subsequently called.
     *
     * @param config The configuration to be used
     */

    public void setConfiguration(Configuration config) {
        this.config = config;
        documentNumber = config.getDocumentNumberAllocator().allocateDocumentNumber();
    }

    /**
     * Get the configuration previously set using setConfiguration
     */

    public Configuration getConfiguration() {
        return config;
    }

    /**
     * Get the name pool used for the names in this document
     *
     * @return the name pool in which all the names used in this document are
     *         registered
     */

    public NamePool getNamePool() {
        return config.getNamePool();
    }


    public void setSystemId(String uri) {
        ((Document)node).setBaseURI(uri);
    }

    /**
     * Ask whether the document contains any nodes whose type annotation is anything other than
     * UNTYPED
     *
     * @return true if the document contains elements whose type is other than UNTYPED
     */
    public boolean isTyped() {
        return false;
    }

    /**
     * Get the unique document number for this document (the number is unique
     * for all documents within a NamePool)
     *
     * @return the unique number identifying this document within the name pool
     */

    public long getDocumentNumber() {
        return documentNumber;
    }

    /**
     * Get the element with a given ID, if any
     *
     * @param id        the required ID value
     * @param getParent
     * @return the element with the given ID, or null if there is no such ID
     *         present (or if the parser has not notified attributes as being of
     *         type ID).
     */

    /*@Nullable*/
    public NodeInfo selectID(String id, boolean getParent) {
        if (idIndex == null) {
            Element elem;
            switch (nodeKind) {
                case Type.DOCUMENT:
                    elem = ((Document) node).getRootElement();
                    break;
                case Type.ELEMENT:
                    elem = (Element) node;
                    break;
                default:
                    return null;
            }
            idIndex = new HashMap<String, NodeInfo>(50);
            buildIDIndex(elem);
        }
        return idIndex.get(id);
    }


    private void buildIDIndex(Element elem) {
        // walk the tree in reverse document order, to satisfy the XPath 1.0 rule
        // that says if an ID appears twice, the first one wins
        for (int i = elem.getChildCount(); --i >= 0; ) {
            Node child = elem.getChild(i);
            if (child instanceof Element) {
                buildIDIndex((Element) child);
            }
        }
        for (int i = elem.getAttributeCount(); --i >= 0; ) {
            Attribute att = elem.getAttribute(i);
            if (att.getType() == Attribute.Type.ID) {
                idIndex.put(att.getValue(), wrap(elem));
            }
        }
    }

    /**
     * Get the list of unparsed entities defined in this document
     *
     * @return an Iterator, whose items are of type String, containing the names of all
     *         unparsed entities defined in this document. If there are no unparsed entities or if the
     *         information is not available then an empty iterator is returned
     */

    public Iterator<String> getUnparsedEntityNames() {
        return Collections.EMPTY_LIST.iterator();
    }

    /**
     * Get the unparsed entity with a given name
     *
     * @param name the name of the entity
     * @return null: XOM does not provide access to unparsed entities
     */

    public String[] getUnparsedEntity(String name) {
        return null;
    }

    /**
     * Get the type annotation of this node, if any. The type annotation is represented as
     * SchemaType object.
     * <p>
     * <p>Types derived from a DTD are not reflected in the result of this method.
     *
     * @return For element and attribute nodes: the type annotation derived from schema
     *         validation (defaulting to xs:untyped and xs:untypedAtomic in the absence of schema
     *         validation). For comments, text nodes, processing instructions, and namespaces: null.
     *         For document nodes, either xs:untyped if the document has not been validated, or
     *         xs:anyType if it has.
     * @since 9.4
     */
    public SchemaType getSchemaType() {
        return Untyped.getInstance();
    }

    /**
     * Set user data on the document node. The user data can be retrieved subsequently
     * using {@link #getUserData}
     *
     * @param key   A string giving the name of the property to be set. Clients are responsible
     *              for choosing a key that is likely to be unique. Must not be null. Keys used internally
     *              by Saxon are prefixed "saxon:".
     * @param value The value to be set for the property. May be null, which effectively
     *              removes the existing value for the property.
     */

    public void setUserData(String key, Object value) {
        if (userData == null) {
            userData = new HashMap(4);
        }
        if (value == null) {
            userData.remove(key);
        } else {
            userData.put(key, value);
        }
    }

    /**
     * Get user data held in the document node. This retrieves properties previously set using
     * {@link #setUserData}
     *
     * @param key A string giving the name of the property to be retrieved.
     * @return the value of the property, or null if the property has not been defined.
     */

    public Object getUserData(String key) {
        if (userData == null) {
            return null;
        } else {
            return userData.get(key);
        }
    }

}
