/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.query.mapping.xml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.teiid.query.QueryPlugin;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


/**
 * <p>Reads an mapping definition file in XML format.  When finished reading
 * the mapping info from the XML file, this class holds an object representation
 * of it, rooted in a <code>MappingNode</code> instance, and also holds an XML
 * <code>Document</code> representation. </p>
 *
 * <h3>Example usage (loading from stream; exceptions not shown)</h3>
 * <p><pre>
 *     MappingLoader loader = new MappingLoader();
 *     MappingNode mappingRootNode = loader.loadDocument(istream);
 * </pre></p>
 *
 * @see MappingNode
 */
public class MappingLoader {
    
    HashMap unresolvedNamespaces = new HashMap();
    
    /**
     * <p>Load mapping definition from XML Document object. </p>
     */
    public MappingDocument loadDocument(InputStream stream) throws MappingException {
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setValidating(false);
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(stream);
            return loadContents(doc);
        } catch (IOException e) {
            throw new MappingException(e);
        } catch (ParserConfigurationException e) {
        	throw new MappingException(e);
		} catch (SAXException e) {
			throw new MappingException(e);
		}
    }
    
    public MappingDocument loadDocument(String fileName) throws MappingException, FileNotFoundException {
    	FileInputStream fis = new FileInputStream(fileName);
    	return loadDocument(fis);
    }    

    /**
     * Load contents into temporary structures.
     */
    MappingDocument loadContents(Document document) throws MappingException {
        MappingDocument doc = new MappingDocument(false);
        
        loadDocumentProperties(doc, document.getDocumentElement());
        
        // now load all the children
        Collection mappingChildren = getChildren(document.getDocumentElement(), MappingNodeConstants.Tags.MAPPING_NODE_NAME); 
        doc = (MappingDocument)recursiveLoadContents(mappingChildren, doc);
        return doc;
    }
        
    private MappingNode recursiveLoadContents(Collection mappingChildren, MappingBaseNode parent) 
        throws MappingException {
        
        MappingBaseNode node = null;
        for (Iterator i = mappingChildren.iterator(); i.hasNext();){
            Element elem = (Element)i.next();

            node = processMappingNode(elem, parent);

            Collection childrenMappingNodes = getChildren(elem, MappingNodeConstants.Tags.MAPPING_NODE_NAME);
            recursiveLoadContents(childrenMappingNodes, node);
        }
        return parent;
    }
    
    public Collection<Element> getChildren(Element elem, String name) {
    	NodeList children = elem.getChildNodes();
    	LinkedList<Element> results = new LinkedList<Element>();
    	for (int i = 0; i < children.getLength(); i++) {
    		Node node = children.item(i);
    		if (node instanceof Element && node.getNodeName().equals(name)) {
    			results.add((Element)node);
    		}
    	}
    	return results;
    }
    
    public Element getChild(Element elem, String name) {
    	NodeList children = elem.getChildNodes();
    	for (int i = 0; i < children.getLength(); i++) {
    		Node node = children.item(i);
    		if (node instanceof Element && node.getNodeName().equals(name)) {
    			return (Element)node;
    		}
    	}
    	return null;
    }

    /**
     * Load a "sequence" node from the mapping document
     * @param element - parent element
     * @return a sequence node
     */
    MappingSequenceNode loadSequenceNode(Element element, MappingBaseNode parentNode) {
        MappingSequenceNode node = new MappingSequenceNode();
        node.setMinOccurrs(getMinOccurrences(element));
        node.setMaxOccurrs(getMaxOccurrences(element));        
        node.setSource(getSource(element));
        node.setExclude(isExcluded(element));
        node.setStagingTables(getStagingTableNames(element));
        return node;
    }
    
    /**
     * Load a "Element" node from mapping document. This can be a
     * "Recursive" or "Criteria" or normal node.
     * @param element - parent element
     * @return retuns a MappingElement
     */
    MappingElement loadElementNode(Element element, MappingBaseNode parentNode, boolean rootElement) 
        throws MappingException{
        
        MappingElement node = null;
        
        String name = getName(element);
        if (name == null || name.length()==0) {
            throw new MappingException(QueryPlugin.Util.getString("MappingLoader.invalidName")); //$NON-NLS-1$
        }
        
        Namespace[] namespaces = getNamespaceDeclarations(element);
        Namespace namespace = getNamespace(element, namespaces, parentNode);
        
        // There are effectively three types of elements, recursive, criteria and regular..
        if (isRecursive(element)) {
            // first check if this is a "recursive" element
            MappingRecursiveElement elem = new MappingRecursiveElement(name, namespace, getRecursionMappingClass(element));
            elem.setCriteria(getRecursionCriteria(element));
            elem.setRecursionLimit(getRecursionLimit(element), throwExceptionOnRecursionLimit(element));
            node = elem;
        }
        else {
            // this regular element
            node = new MappingElement(name, namespace);
        }
        
        // now load all other common properties.
        if (rootElement) {
            node.setMinOccurrs(1);
            node.setMaxOccurrs(1);            
        }
        else {
            node.setMinOccurrs(getMinOccurrences(element));
            node.setMaxOccurrs(getMaxOccurrences(element));
        }
        
        node.setNameInSource(getNameInSource(element));
        node.setSource(getSource(element));
        node.setOptional(isOptional(element));
        node.setDefaultValue(getDefaultValue(element));
        node.setValue(getFixedValue(element));
        node.setNillable(isNillable(element));
        node.setExclude(isExcluded(element));
        node.setType(getBuitInType(element));
        node.setNormalizeText(getNormalizeText(element));
        node.setAlwaysInclude(includeAlways(element));
        node.setStagingTables(getStagingTableNames(element));                
        node.setNamespaces(namespaces);

        return node;
    }

    /**
     * Load an attribute node from the element
     */
    void loadAttributeNode(Element element, MappingElement parent) throws MappingException {
        String name = getName(element);
        String nsPrefix = getElementValue(element, MappingNodeConstants.Tags.NAMESPACE_PREFIX);
        
        if (name == null || name.length()==0) {
            throw new MappingException(QueryPlugin.Util.getString("MappingLoader.invalidName")); //$NON-NLS-1$
        }
        
        Namespace namespace = null;
        boolean normalAttribute = true;
        if (name.equalsIgnoreCase(MappingNodeConstants.NAMESPACE_DECLARATION_ATTRIBUTE_NAMESPACE)) {
            // this is default name space where only "xmlns" is defined. We do not need to global map
            // as there may be more(I guess..)
            namespace = new Namespace("", getFixedValue(element)); //$NON-NLS-1$
            parent.addNamespace(namespace);            
            normalAttribute = false;
        }
        else if (nsPrefix != null && nsPrefix.equalsIgnoreCase(MappingNodeConstants.NAMESPACE_DECLARATION_ATTRIBUTE_NAMESPACE)) {
            // prior to 5.5, a document can use the namespaces before it declares them; this little
            // trick(hack) to fix it.
            namespace = (Namespace)this.unresolvedNamespaces.remove(name);
            if (namespace == null) {
                namespace = new Namespace(name);    
            }
            // this is specific name space declaration like xsi, foo etc..                            
            namespace.setUri(getFixedValue(element));
            parent.addNamespace(namespace);      
            normalAttribute = false;
        }
        else {
            // this is a invalid form of 
            namespace = getNamespace(element, null, parent);
        }
        
        // if this not any namespace specific attribute then it is any normal attribute on the
        // tree; treat it as such.
        if (normalAttribute) {
            MappingAttribute attribute = new MappingAttribute(getName(element), namespace);
            // now get all other properties for the attribute.
            attribute.setNameInSource(getNameInSource(element));
            attribute.setDefaultValue(getDefaultValue(element));
            attribute.setValue(getFixedValue(element));
            attribute.setExclude(isExcluded(element));
            attribute.setNormalizeText(getNormalizeText(element));
            attribute.setOptional(isOptional(element));
            attribute.setAlwaysInclude(includeAlways(element));
            parent.addAttribute(attribute);
        }
    }
    
    /**
     * Load a "Choice" Node
     * @param element
     * @return
     */
    MappingChoiceNode loadChoiceNode(Element element, MappingBaseNode parentNode) {
        MappingChoiceNode node = new MappingChoiceNode(exceptionOnDefault(element));
        node.setMinOccurrs(getMinOccurrences(element));
        node.setMaxOccurrs(getMaxOccurrences(element));        
        node.setSource(getSource(element));
        node.setExclude(isExcluded(element));
        node.setStagingTables(getStagingTableNames(element));
        return node;
    }
    
    /**
     * Load the "all" node
     */
    MappingAllNode loadAllNode(Element element, MappingBaseNode parentNode) {
        MappingAllNode node = new MappingAllNode();
        node.setMinOccurrs(getMinOccurrences(element));
        node.setMaxOccurrs(getMaxOccurrences(element));        
        node.setSource(getSource(element));
        node.setExclude(isExcluded(element));
        node.setStagingTables(getStagingTableNames(element));
        return node;        
    }
    
    /**
     * Load the comment node
     */
    void loadCommentNode(Element element, MappingElement parent) {
        MappingCommentNode comment = new MappingCommentNode(getCommentText(element));
        parent.addCommentNode(comment);
    }
    
    /**
     * Load the mapping document
     */
    MappingDocument loadDocumentProperties (MappingDocument doc, Element element) {
        boolean formatted = isFormattedDocument(element);
        if (formatted != MappingNodeConstants.Defaults.DEFAULT_FORMATTED_DOCUMENT.booleanValue()) {
            doc.setFormatted(formatted);
        }
        String encoding = getDocumentEncoding(element);
        if (!MappingNodeConstants.Defaults.DEFAULT_DOCUMENT_ENCODING.equalsIgnoreCase(encoding)) {
            doc.setDocumentEncoding(encoding);
        }
       return doc;
    }

    /**
     * Load a criteria node; Criteria node can only be child of a choice node.
     */
    MappingCriteriaNode loadCriteriaNode(Element element, MappingBaseNode parentNode) throws MappingException{
        if (getCriteria(element) != null || isDefaultOnChoiceNode(element)) {
            // add this criteria node to the parent and make it the parent itself for rest of the information.
            return new MappingCriteriaNode(getCriteria(element), isDefaultOnChoiceNode(element));
        }
        throw new MappingException(QueryPlugin.Util.getString("MappingLoader.invalid_criteria_node")); //$NON-NLS-1$
    }
    
    /**
     * Process a mapping node that has been encountered.
     * @param element XML Document Element which is the source of the
     * MappingNode
     * @param parentNode MappingNode parent - will be null only the first
     * time, for the root Element
     * @return MappingNode just processed
     */
    MappingBaseNode processMappingNode(Element element, MappingBaseNode parentNode) throws MappingException {
        
        boolean isRootNode = (parentNode instanceof MappingDocument);
        
        // Parse the node based on the node type.
        String nodeType = getElementValue( element, MappingNodeConstants.Tags.NODE_TYPE );
        if (nodeType == null || nodeType.length() == 0) {
            nodeType = MappingNodeConstants.ELEMENT;
        }
         
        // Load the document properties
        if (isRootNode && nodeType.equalsIgnoreCase(MappingNodeConstants.ELEMENT)) {
            MappingDocument doc = (MappingDocument)parentNode;
            loadDocumentProperties(doc, element);
        }
        
        // prior to 5.5, a criteria node also behaved like element, however it has been changed to
        // have only criteria information. however we still need to support the old vdbs with 
        // old type of mapping document (this can be removed after couple versions).
        if (nodeType.equalsIgnoreCase(MappingNodeConstants.ELEMENT)
                        && (getCriteria(element) != null || isDefaultOnChoiceNode(element))) {
            // add this criteria node to the parent and make it the parent itself for rest of the information.
            MappingCriteriaNode node = new MappingCriteriaNode(getCriteria(element), isDefaultOnChoiceNode(element));
            parentNode.addCriteriaNode(node);
            parentNode = node;
        }
        
        if (nodeType.equalsIgnoreCase(MappingNodeConstants.ELEMENT)) {
            MappingElement child = loadElementNode(element, parentNode, isRootNode);
            parentNode.addChildElement(child);
            return child;
        }      
        else if (nodeType.equalsIgnoreCase(MappingNodeConstants.ATTRIBUTE)) {
            loadAttributeNode(element, (MappingElement)parentNode);
        }
        else if (nodeType.equalsIgnoreCase(MappingNodeConstants.CHOICE)) {
            MappingChoiceNode child = loadChoiceNode(element, parentNode);
            parentNode.addChoiceNode(child);
            return child;
        }
        else if (nodeType.equalsIgnoreCase(MappingNodeConstants.CRITERIA)) {
            MappingCriteriaNode child = loadCriteriaNode(element, parentNode);
            parentNode.addCriteriaNode(child);
            return child;
        }                
        else if (nodeType.equalsIgnoreCase(MappingNodeConstants.ALL)) {
            MappingAllNode child = loadAllNode(element, parentNode);
            parentNode.addAllNode(child);
            return child;
        }        
        else if (nodeType.equalsIgnoreCase(MappingNodeConstants.SEQUENCE)) {
            MappingSequenceNode child = loadSequenceNode(element, parentNode);
            parentNode.addSequenceNode(child);
            return child;
        }
        else if (nodeType.equalsIgnoreCase(MappingNodeConstants.COMMENT)) {
            loadCommentNode(element, (MappingElement)parentNode);
        }
        else {
            // should we ignore I am not sure??
            throw new MappingException(QueryPlugin.Util.getString("MappingLoader.unknown_node_type", nodeType)); //$NON-NLS-1$            
        }
        return null;
    }
    
    /**
     * Return Properties holding namespaces declarations, or null if none
     */
    private Namespace[] getNamespaceDeclarations(Element element) {
        ArrayList namespaces = new ArrayList();
        Iterator elements = getChildren(element, MappingNodeConstants.Tags.NAMESPACE_DECLARATION).iterator();
        while (elements.hasNext()){
            Element namespace = (Element)elements.next();
            Element prefixEl = getElement(namespace, MappingNodeConstants.Tags.NAMESPACE_DECLARATION_PREFIX);
            Element uriEl = getElement(namespace, MappingNodeConstants.Tags.NAMESPACE_DECLARATION_URI);
            String prefix = (prefixEl != null ? getTextTrim(prefixEl) : MappingNodeConstants.DEFAULT_NAMESPACE_PREFIX);
            String uri = getTextTrim(uriEl);
            namespaces.add(new Namespace(prefix, uri));
        }
        return (Namespace[])namespaces.toArray(new Namespace[namespaces.size()]);
    }
    
    static String getTextTrim(Element element) {
    	if (element == null) {
    		return null;
    	}
    	String result = element.getTextContent();
    	if (result != null) {
    		return result.trim();
    	}
    	return result;
    }

    /**
     * Get a specific child element of an element.
     */
    Element getElement( Element element, String childName ) {
        return getChild(element, childName);
    }

    String getElementValue( Element element, String childName ) {
        Element child = getChild(element, childName);
        return getTextTrim(child);
    }
    
    String getElementValue( Element element, String childName , String defalt) {
        Element child = getChild(element, childName);
        String result = getTextTrim(child);
        if (result == null) {
        	return defalt;
        }
        return result;
    }    
    
    int getIntElementValue( Element element, String childName , int defalt) {
        Element child = getChild(element, childName);
        if (child != null) {
            return Integer.valueOf(getTextTrim(child)).intValue();
        }
        return defalt;
    } 
    
    boolean getBooleanElementValue( Element element, String childName , boolean defalt) {
        Element child = getChild(element, childName);
        if (child != null) {
            return Boolean.valueOf(getTextTrim(child)).booleanValue();
        }
        return defalt;
    } 
    
    String getName(Element element) {
        return getElementValue(element, MappingNodeConstants.Tags.NAME);
    }

    int getMinOccurrences(Element element) {
        return getIntElementValue(element, MappingNodeConstants.Tags.CARDINALITY_MIN_BOUND, MappingNodeConstants.Defaults.DEFAULT_CARDINALITY_MINIMUM_BOUND.intValue());        
    }
    
    int getMaxOccurrences(Element element) {
        String maxBound = getElementValue(element, MappingNodeConstants.Tags.CARDINALITY_MAX_BOUND );        
        if (maxBound != null && maxBound.equals(MappingNodeConstants.CARDINALITY_UNBOUNDED_STRING)){
            return MappingNodeConstants.CARDINALITY_UNBOUNDED.intValue();
        } else if (maxBound != null){
            return Integer.valueOf(maxBound).intValue();
        }
        else {
            return MappingNodeConstants.Defaults.DEFAULT_CARDINALITY_MAXIMUM_BOUND.intValue();
        }
    }

    String getNameInSource(Element element) {
        return getElementValue(element, MappingNodeConstants.Tags.ELEMENT_NAME);        
    }
    
    String getCommentText(Element element) {
        return getElementValue(element, MappingNodeConstants.Tags.COMMENT_TEXT);        
    }
    
    boolean isOptional(Element element) {
        return getBooleanElementValue( element, MappingNodeConstants.Tags.IS_OPTIONAL, MappingNodeConstants.Defaults.DEFAULT_IS_OPTIONAL.booleanValue());        
    }
    
    String getSource(Element element) {
        return getElementValue(element, MappingNodeConstants.Tags.RESULT_SET_NAME );        
    }
    
    String getCriteria(Element element) {
        return getElementValue(element, MappingNodeConstants.Tags.CRITERIA );        
    }

    String getDefaultValue(Element element) { 
        return getElementValue(element, MappingNodeConstants.Tags.DEFAULT_VALUE);
    }
    
    String getFixedValue(Element element) { 
        return getElementValue(element, MappingNodeConstants.Tags.FIXED_VALUE);
    }
    
    boolean isNillable(Element element) { 
        return getBooleanElementValue(element, MappingNodeConstants.Tags.IS_NILLABLE, MappingNodeConstants.Defaults.DEFAULT_IS_NILLABLE.booleanValue());
    }

    boolean isExcluded(Element element) { 
        return getBooleanElementValue(element, MappingNodeConstants.Tags.IS_EXCLUDED, MappingNodeConstants.Defaults.DEFAULT_IS_EXCLUDED.booleanValue());
    }
    
    boolean isDefaultOnChoiceNode(Element element) { 
        return getBooleanElementValue(element, MappingNodeConstants.Tags.IS_DEFAULT_CHOICE, MappingNodeConstants.Defaults.DEFAULT_IS_DEFAULT_CHOICE.booleanValue());
    }
    
    boolean exceptionOnDefault(Element element) { 
        return getBooleanElementValue(element, MappingNodeConstants.Tags.EXCEPTION_ON_DEFAULT, MappingNodeConstants.Defaults.DEFAULT_EXCEPTION_ON_DEFAULT.booleanValue());
    }

    String getDocumentEncoding(Element element) { 
        return getElementValue(element, MappingNodeConstants.Tags.DOCUMENT_ENCODING, MappingNodeConstants.Defaults.DEFAULT_DOCUMENT_ENCODING);
    }
    
    boolean isFormattedDocument(Element element) { 
        return getBooleanElementValue(element, MappingNodeConstants.Tags.FORMATTED_DOCUMENT, MappingNodeConstants.Defaults.DEFAULT_FORMATTED_DOCUMENT.booleanValue());
    }
    
    boolean isRecursive(Element element) { 
        return getBooleanElementValue(element, MappingNodeConstants.Tags.IS_RECURSIVE, MappingNodeConstants.Defaults.DEFAULT_IS_RECURSIVE.booleanValue());
    }
    
    String getRecursionCriteria(Element element) { 
        return getElementValue(element, MappingNodeConstants.Tags.RECURSION_CRITERIA);
    }
    
    String getRecursionMappingClass(Element element) { 
        return getElementValue(element, MappingNodeConstants.Tags.RECURSION_ROOT_MAPPING_CLASS);
    }
        
    int getRecursionLimit(Element element) {
        return getIntElementValue(element, MappingNodeConstants.Tags.RECURSION_LIMIT, MappingNodeConstants.Defaults.DEFAULT_RECURSION_LIMIT.intValue());        
    }
    
    boolean throwExceptionOnRecursionLimit(Element element) { 
        return getBooleanElementValue(element, MappingNodeConstants.Tags.RECURSION_LIMIT_EXCEPTION, MappingNodeConstants.Defaults.DEFAULT_EXCEPTION_ON_RECURSION_LIMIT.booleanValue());
    }

    List getStagingTableNames(Element element) { 
        ArrayList cacheGroups = new ArrayList();
        Collection tempGroupElements = getChildren(element, MappingNodeConstants.Tags.TEMP_GROUP_NAME);
        for (Iterator i = tempGroupElements.iterator(); i.hasNext();) {
            Element tempGroup = (Element)i.next();
            cacheGroups.add(getTextTrim(tempGroup));            
        }
        return cacheGroups;
    }
    
    String getNormalizeText(Element element) { 
        return getElementValue(element, MappingNodeConstants.Tags.NORMALIZE_TEXT, MappingNodeConstants.Defaults.DEFAULT_NORMALIZE_TEXT);
    }
    
    String getBuitInType(Element element) { 
        return getElementValue(element, MappingNodeConstants.Tags.BUILT_IN_TYPE);
    }
    
    boolean includeAlways(Element element) { 
        return getBooleanElementValue(element, MappingNodeConstants.Tags.ALWAYS_INCLUDE, false);
    }    

    Namespace getNamespace(Element element, Namespace[] localNamespaces, MappingBaseNode parentNode) { 
        String prefix = getElementValue(element, MappingNodeConstants.Tags.NAMESPACE_PREFIX);
        
        if (prefix != null) {
            if (localNamespaces != null) {
                // first try to find the name space in its own namespace attributes
                for (int i = 0; i < localNamespaces.length; i++) {
                    if (localNamespaces[i].getPrefix().equals(prefix)) {
                        return localNamespaces[i];
                    }
                }
            }
            
            // then look in the parent nodes.
            while (parentNode != null) {
                if (parentNode instanceof MappingElement) {
                    MappingElement parentElement = (MappingElement)parentNode;
                    return getNamespace(element, parentElement.getNamespaces(), parentElement.getParentNode());
                }
                parentNode = parentNode.getParentNode();
            }
         
            // default; we should never get here.. except otherwise a namespace is used before its
            // declaration; which is case sometimes.
            Namespace unresolved = new Namespace(prefix);
            this.unresolvedNamespaces.put(prefix, unresolved);
            return unresolved;
        }
        return MappingNodeConstants.NO_NAMESPACE;
    }
    
} // END CLASS
