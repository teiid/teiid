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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.teiid.query.sql.symbol.ElementSymbol;



/** 
 * A Mapping Node which defines a Element in XML Schema Mapping document
 */
public class MappingElement extends MappingBaseNode {

    // Element symbol in the resultset source
    ElementSymbol symbol;
    
    Namespace namespace;
    
    public MappingElement(String name) {
        this(name, MappingNodeConstants.NO_NAMESPACE);
    }
    
    public MappingElement(String name, String nameInSource) {
        this(name, MappingNodeConstants.NO_NAMESPACE);
        setNameInSource(nameInSource);
    }    
    
    public MappingElement(String name, Namespace namespace) {
        setProperty(MappingNodeConstants.Properties.NAME, name);
        setProperty(MappingNodeConstants.Properties.NODE_TYPE, MappingNodeConstants.ELEMENT);

        this.namespace = namespace;
        if (namespace != MappingNodeConstants.NO_NAMESPACE) {
            setProperty(MappingNodeConstants.Properties.NAMESPACE_PREFIX, namespace.getPrefix());
        }        
    }
    
    public void acceptVisitor(MappingVisitor visitor) {
        visitor.visit(this);
    }
    
    public Namespace getNamespace() {
        return this.namespace;
    }
        
    public MappingElement setNameInSource(String srcName) {
        if (srcName != null) {
            setProperty(MappingNodeConstants.Properties.ELEMENT_NAME, srcName);
        }
        return this;
    }
    
    public MappingElement setNillable(boolean nil) {
        setProperty(MappingNodeConstants.Properties.IS_NILLABLE, Boolean.valueOf(nil));
        return this;
    }
    
    public MappingElement setDefaultValue(String value) {
        if (value != null) {
            setProperty(MappingNodeConstants.Properties.DEFAULT_VALUE, value);
        }
        return this;
    }

    public MappingElement setValue(String value) {
        if (value != null) {
            setProperty(MappingNodeConstants.Properties.FIXED_VALUE, value);
        }
        return this;
    }
       
    public MappingElement setOptional(boolean optional) {
        setProperty(MappingNodeConstants.Properties.IS_OPTIONAL, Boolean.valueOf(optional));
        return this;
    }
    
    public MappingElement setNormalizeText(String normalize) {
        if (normalize != null) {
            setProperty(MappingNodeConstants.Properties.NORMALIZE_TEXT, normalize);    
        }
        return this;
    }
    
    public MappingElement setType(String type) {
        if (type != null) {
            setProperty(MappingNodeConstants.Properties.BUILT_IN_TYPE, type);
        }
        return this;
    }
    
    public MappingElement setAlwaysInclude(boolean include) {
        setProperty(MappingNodeConstants.Properties.ALWAYS_INCLUDE, Boolean.valueOf(include));
        return this;
    }
                  
    /**
     * Adds a comment node to the current element and returns the added
     * child node 
     */
    public MappingCommentNode addCommentNode(MappingCommentNode elem) {
        addChild(elem);
        return elem;
    } 
    
    /**
     * Adds the attribute node to the current node and returns the current node
     */
    public MappingElement addAttribute(MappingAttribute attr) {
        addChild(attr);
        return this;
    }

    /**
     * Adds a sibiling node to the current node and returns the added sibiling node;
     * @param elem
     */
    public MappingElement addSibilingElement(MappingElement elem) {
        getParent().addChild(elem);
        return elem;
    }
    
    /**
     * Remove attribute node from element
     * @param toRemove
     */
    public void removeAttribute(MappingAttribute toRemove) {
        List children = getChildren();
        for (final Iterator i = children.iterator(); i.hasNext();) {
            if (i.next() == toRemove) {
                i.remove();
                break;
            }
        } // for
    }
    
    /**
     * Declare the namespaces on the element.
     */
    public void setNamespaces(Namespace[] spaces) {
        if (spaces != null && spaces.length > 0) {
            Properties props = new Properties(); 
            for (int i = 0; i < spaces.length; i++) {
                props.put(spaces[i].getPrefix(), spaces[i].getUri());
            }
            setProperty(MappingNodeConstants.Properties.NAMESPACE_DECLARATIONS, props);
        }
    }
    
    public void addNamespace(Namespace space) {
        if (space != null) {
            Properties props = (Properties)getProperty(MappingNodeConstants.Properties.NAMESPACE_DECLARATIONS);
            if (props == null) {
                props = new Properties();
            }
            props.put(space.getPrefix(), space.getUri());
            setProperty(MappingNodeConstants.Properties.NAMESPACE_DECLARATIONS, props);
        }
    }
     
    /**
     * Get all the attributes on this Node
     * @return empty list if not found any attributes
     */
    public List getAttributes() {
        return getChildren(MappingNodeConstants.ATTRIBUTE);
    }
    
    public boolean hasAttributes() {
        List attr = getAttributes();
        return (attr != null && !attr.isEmpty());
    }
        
    /**
     * true if this element is recursive element; false otherwise
     */
    public boolean isRecursive() {
        return false;
    }  

    /**
     * Name of the node
     * @return
     */
    public String getName() {
        return (String)getProperty(MappingNodeConstants.Properties.NAME);        
    }
        
    /**
     * Namespace prefix
     * @return
     */
    public String getNamespacePrefix() {
        return (String)getProperty(MappingNodeConstants.Properties.NAMESPACE_PREFIX);        
    }
    
    /**
     * Namespaces to be decalred
     * @return
     */
    public Namespace[] getNamespaces() {
        ArrayList list = new ArrayList();
        Properties props = (Properties)getProperty(MappingNodeConstants.Properties.NAMESPACE_DECLARATIONS);
        if (props != null && !props.isEmpty()) {
            for(Iterator i = props.keySet().iterator(); i.hasNext();) {
                String key = (String)i.next();
                String value = props.getProperty(key);
                list.add(new Namespace(key, value));
            }
        }
        return (Namespace[]) list.toArray(new Namespace[list.size()]);
    }
    
    public Properties getNamespacesAsProperties() {
        Properties props = (Properties)getProperty(MappingNodeConstants.Properties.NAMESPACE_DECLARATIONS);
        if (props == null) {
            props = new Properties();
        }
        return props;
    }
    
    public String getNameInSource() {
        return (String)getProperty(MappingNodeConstants.Properties.ELEMENT_NAME);        
    }
    
    public boolean isNillable() {
        Boolean nillable = (Boolean)getProperty(MappingNodeConstants.Properties.IS_NILLABLE);
        if (nillable != null) {
            return nillable.booleanValue();
        }
        return false;
    }
    
    public String getDefaultValue() {
        return (String)getProperty(MappingNodeConstants.Properties.DEFAULT_VALUE);        
    }
    
    public String getValue() {
        return (String)getProperty(MappingNodeConstants.Properties.FIXED_VALUE);        
    }
    
    public boolean isOptional() {
        Boolean optional = (Boolean)getProperty(MappingNodeConstants.Properties.IS_OPTIONAL);
        if (optional != null) {
            return optional.booleanValue();
        }
        return false;
    }
    
    public String getNormalizeText() {
        String text = (String)getProperty(MappingNodeConstants.Properties.NORMALIZE_TEXT);
        if (text == null) {
            text = MappingNodeConstants.Defaults.DEFAULT_NORMALIZE_TEXT;
        }
        return text;
    }
    
    public String getType() {
        return (String)getProperty(MappingNodeConstants.Properties.BUILT_IN_TYPE);
    }
    
    public boolean isAlwaysInclude() {
        Boolean include = (Boolean)getProperty(MappingNodeConstants.Properties.ALWAYS_INCLUDE);
        if (include != null) {
            return include.booleanValue();
        }
        return true;
    }
    
    /**
     * @see org.teiid.query.mapping.xml.MappingBaseNode#isTagRoot()
     */
    public boolean isTagRoot() {
        MappingBaseNode parent = getParentNode();
        while (parent != null) {
            if (parent instanceof MappingElement) {
                return false;
            }
            parent = parent.getParentNode();
        }
        return true;
    }    
    
    public void setElementSymbol(ElementSymbol symbol) {
        this.symbol = symbol;
    }

    public ElementSymbol getElementSymbol() {
        return this.symbol;
    }
    
    /** 
     * @see org.teiid.query.mapping.xml.MappingNode#getSourceNode()
     */
    public MappingSourceNode getSourceNode() {
        String nameInSource = getNameInSource();
        if (nameInSource != null) {
            String source = nameInSource.substring(0, nameInSource.lastIndexOf('.'));
            MappingBaseNode parent = getParentNode();
            while(parent != null) {
                if (parent instanceof MappingSourceNode) {
                    MappingSourceNode sourceNode = (MappingSourceNode)parent;
                    if (sourceNode.getResultName().equalsIgnoreCase(source)) {
                        return sourceNode;
                    }
                }
                parent = parent.getParentNode();
            }
        }
        return super.getSourceNode();
    }
}



