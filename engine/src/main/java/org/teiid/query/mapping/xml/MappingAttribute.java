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

import org.teiid.query.sql.symbol.ElementSymbol;


/** 
 * A Mapping node which denotes a attribute node.
 */
public class MappingAttribute extends MappingNode {
    // Element symbol in the resultset source
    ElementSymbol symbol;
    
    // namespace of the attribute
    Namespace namespace;
    
    public MappingAttribute(String name) {
        this(name, MappingNodeConstants.NO_NAMESPACE);
    }
    
    public MappingAttribute(String name, String nameInSource) {
        this(name, MappingNodeConstants.NO_NAMESPACE);
        setNameInSource(nameInSource);
    }    
        
    public MappingAttribute(String name, Namespace namespace) {
        setProperty(MappingNodeConstants.Properties.NAME, name);
        setProperty(MappingNodeConstants.Properties.NODE_TYPE, MappingNodeConstants.ATTRIBUTE);
        
        this.namespace = namespace;
        if (namespace != MappingNodeConstants.NO_NAMESPACE) {
            setProperty(MappingNodeConstants.Properties.NAMESPACE_PREFIX, namespace.getPrefix());
        }
    }    
         
    public void acceptVisitor(MappingVisitor visitor) {
        visitor.visit(this);
    }
    
    public MappingElement getParentNode() {
        return (MappingElement)getParent();
    }
    
    public Namespace getNamespace() {
        return this.namespace;
    }
        
    public void setNameInSource(String srcName) {
        if (srcName != null) {
            setProperty(MappingNodeConstants.Properties.ELEMENT_NAME, srcName);
        }
    }
    
    public void setDefaultValue(String value) {
        if (value != null) {
            setProperty(MappingNodeConstants.Properties.DEFAULT_VALUE, value);
        }
    }

    public void setValue(String value) {
        if (value != null) {
            setProperty(MappingNodeConstants.Properties.FIXED_VALUE, value);
        }
    }
    
    public void setOptional(boolean optional) {
        setProperty(MappingNodeConstants.Properties.IS_OPTIONAL, Boolean.valueOf(optional));        
    }
 
    public void setNormalizeText(String normalize) {
        if (normalize != null) {
            setProperty(MappingNodeConstants.Properties.NORMALIZE_TEXT, normalize);
        }
    }    
    
    public void setAlwaysInclude(boolean include) {
        setProperty(MappingNodeConstants.Properties.ALWAYS_INCLUDE, Boolean.valueOf(include));
    }
    
    /** 
     * @see org.teiid.query.mapping.xml.MappingNode#getPathName()
     */
    public String getPathName() {
        return "@" + super.getPathName(); //$NON-NLS-1$
    }
    
    public String getCanonicalName() {
        return getFullyQualifiedName().toUpperCase();
    }

    /**
     * Namespace prefix
     * @return
     */
    public String getNamespacePrefix() {
        return (String)getProperty(MappingNodeConstants.Properties.NAMESPACE_PREFIX);        
    }
    
    public String getNameInSource() {
        return (String)getProperty(MappingNodeConstants.Properties.ELEMENT_NAME);        
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
    
    public boolean isAlwaysInclude() {
        Boolean include = (Boolean)getProperty(MappingNodeConstants.Properties.ALWAYS_INCLUDE);
        if (include != null) {
            return include.booleanValue();
        }
        return false;
    }    
    
    public String getSource() {
        return (String) getProperty(MappingNodeConstants.Properties.RESULT_SET_NAME);
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
