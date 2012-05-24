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

package org.teiid.query.processor.xml;

import java.util.Properties;

import org.teiid.core.TeiidComponentException;
import org.teiid.query.QueryPlugin;
import org.teiid.query.mapping.xml.MappingAttribute;
import org.teiid.query.mapping.xml.MappingElement;
import org.teiid.query.mapping.xml.MappingNode;
import org.teiid.query.mapping.xml.MappingNodeConstants;
import org.teiid.query.sql.symbol.ElementSymbol;



/** 
 * THis class contains the information of an element or an attribute.
 * @since 4.3
 */
public class NodeDescriptor {
    public static final String DEFAULT_NAMESPACE_URI = ""; //$NON-NLS-1$
    
    //namespace prefix
    private String namespacePrefix;
    //namespace URI
    private String namespaceURI;
    //name without prefix
    private String name;
    //name with prefix
    private String qName;
    //declared namespace URIs
    //The property names should be String namespace prefixes, and the
    //corresponding values should be String namespace uri's.  A default 
    //namespace may be contained within this Properties object by mapping the
    //DEFAULT_NAMESPACE_PREFIX constant to a String uri.  An empty default namespace
    //may be contained by mapping that constant to an empty String - this
    //has the effect of locally having NO namespace by default.
    //These namespace declarations hold for the scope of this element (the element itself and its contents).
    private Properties namespaceURIs;
    //For element, it indicates that this element is not nillable or 
    //has a min occurs of zero, and may not have to be output in the final document (unless
    //it has string content or child elements that must be in the document).
    //For attribute, it indicates that this attribute should be considered
    //optional and does not have to be output if the element containing this
    //attribute is also optional and ends up with no child content. This
    //is to enable fragments with attributes but no child element content to be
    //trimmed from the result document.
    private boolean isOptional;
    //true if it is element
    private boolean isElement;
    //default value
    private String defaultValue;
    // text normalization mode 
    // and can be one of the following
    // preserve, replace and collapse.
    private String textNormalizationMode;
    
    private Class runtimeType;
    private String docBuiltInType;
    
    public NodeDescriptor(String name, String qName, String namespacePrefix, String namespaceURI, Properties namespaceURIs, String defaultValue, boolean isOptional, boolean isElement, String textNormalizationMode, Class runtimeType, String docBuiltInType) {
        this.name = name;
        this.qName = qName;
        this.namespaceURI = namespaceURI;
        this.namespaceURIs = namespaceURIs;
        this.isOptional = isOptional;
        this.namespacePrefix = namespacePrefix;
        this.defaultValue = defaultValue;
        this.isElement = isElement;
        this.textNormalizationMode = textNormalizationMode;
        this.runtimeType = runtimeType;
        this.docBuiltInType = docBuiltInType;       
    }
    
    /** 
     * @return Returns the isOptional.
     * @since 4.3
     */
    public boolean isOptional() {
        return this.isOptional;
    }
    
    /** 
     * @return Returns the name.
     * @since 4.3
     */
    public String getName() {
        return this.name;
    }
    
    /** 
     * @return Returns the namespaceURI.
     * @since 4.3
     */
    public String getNamespaceURI() {
        return this.namespaceURI;
    }
    
    /** 
     * @return Returns the namespaceURIs.
     * @since 4.3
     */
    public Properties getNamespaceURIs() {
        return this.namespaceURIs;
    }
    
    /** 
     * @return Returns the qName.
     * @since 4.3
     */
    public String getQName() {
        return this.qName;
    }
   
    /** 
     * @return Returns the isElement.
     * @since 4.3
     */
    public boolean isElement() {
        return this.isElement;
    }

    
    /** 
     * @return Returns the defaultValue.
     * @since 4.3
     */
    public String getDefaultValue() {
        return this.defaultValue;
    }

    
    /** 
     * @return Returns the namespacePrefix.
     * @since 4.3
     */
    public String getNamespacePrefix() {
        return this.namespacePrefix;
    }

    
    /** 
     * @return Returns the textNormalizationMode.
     * @since 4.3
     */
    public String getTextNormalizationMode() {
        return this.textNormalizationMode;
    }

    
    /** 
     * @param textNormalizationMode The textNormalizationMode to set.
     * @since 4.3
     */
    public void setTextNormalizationMode(String textNormalizationMode) {
        this.textNormalizationMode = textNormalizationMode;
    }
    
    
    public Class getRuntimeType() {
        return this.runtimeType;
    }

    
    public String getDocBuiltInType() {
        return this.docBuiltInType;
    }
    
    public static NodeDescriptor createNodeDescriptor(MappingElement element) {
        // should't this be if nillable it is optional ?
        boolean isOptional = false;
        if (element.getMinOccurence() == 0 && !element.isNillable()){
            isOptional = true;
        }
        
        String docBuiltInType = element.getType();
        if(docBuiltInType == MappingNodeConstants.NO_TYPE) {
            docBuiltInType = null;
        }      
        
        ElementSymbol symbol = element.getElementSymbol();
        
        NodeDescriptor descriptor = new NodeDescriptor(element.getName(), 
                                                       getQName(element.getName(),element.getNamespacePrefix()), 
                                                       element.getNamespace().getPrefix(), 
                                                       element.getNamespace().getUri(), 
                                                       element.getNamespacesAsProperties(), 
                                                       ((symbol != null)?element.getDefaultValue():element.getValue()), 
                                                       isOptional, 
                                                       true, 
                                                       element.getNormalizeText(), 
                                                       ((symbol != null)?symbol.getType():null), 
                                                       docBuiltInType);
        return descriptor;
        
    }

    public static NodeDescriptor createNillableNode() {
        return new NodeDescriptor("nil", //$NON-NLS-1$
                                  getQName("nil", MappingNodeConstants.INSTANCES_NAMESPACE_PREFIX), //$NON-NLS-1$
                                  MappingNodeConstants.INSTANCES_NAMESPACE_PREFIX, 
                                  MappingNodeConstants.INSTANCES_NAMESPACE,
                                  null,
                                  "true", //$NON-NLS-1$
                                  false,    // is optional
                                  false,    // is element
                                  MappingNodeConstants.NORMALIZE_TEXT_PRESERVE,
                                  null,
                                  null);
    }
    
    
    public static NodeDescriptor createNodeDescriptor(MappingAttribute attribute) {
      ElementSymbol symbol = attribute.getElementSymbol();
      
      // create processing instruction based on the info
      NodeDescriptor descriptor = new NodeDescriptor(attribute.getName(),
                                                     getQName(attribute.getName(), attribute.getNamespacePrefix()),
                                                     attribute.getNamespace().getPrefix(),
                                                     attribute.getNamespace().getUri(),
                                                     null,
                                                     ((symbol != null)?attribute.getDefaultValue():attribute.getValue()),
                                                     attribute.isOptional(),
                                                     false,
                                                     attribute.getNormalizeText(),
                                                     ((symbol != null)?symbol.getType():null),
                                                     null);
      
      return descriptor;
    }
    
    public static NodeDescriptor createNodeDescriptor(String name, String namespacePrefix, boolean isElement, String defaultValue, Properties namespaceDeclarations, Properties parentNamespaceDeclarations,boolean isOptional, MappingNode node, String textNormalizationMode) throws TeiidComponentException{
        return createNodeDescriptor(name, namespacePrefix, isElement, defaultValue, namespaceDeclarations, parentNamespaceDeclarations, isOptional, node, textNormalizationMode, null, null);
    }
    
    public static NodeDescriptor createNodeDescriptor(String name, String namespacePrefix, boolean isElement, String defaultValue, Properties namespaceDeclarations, Properties parentNamespaceDeclarations,boolean isOptional, MappingNode node, String textNormalizationMode, Class runtimeType, String docBuiltInType) throws TeiidComponentException{
        //get namespace uri
        String uri = null;
        if(namespacePrefix == null) {
            namespacePrefix = MappingNodeConstants.DEFAULT_NAMESPACE_PREFIX;
        }else {
            if(namespaceDeclarations != null) {       
                uri = namespaceDeclarations.getProperty(namespacePrefix);
            }
            if(parentNamespaceDeclarations != null) {       
                uri = parentNamespaceDeclarations.getProperty(namespacePrefix);
            }
            if(uri == null){
                //look for namespace in ancester nodes
                MappingNode parent;
                while((parent = node.getParent()) != null){
                  parentNamespaceDeclarations= (Properties)parent.getProperty(MappingNodeConstants.Properties.NAMESPACE_DECLARATIONS);
                  if(parentNamespaceDeclarations != null) {
                      uri = parentNamespaceDeclarations.getProperty(namespacePrefix);
                      if(uri != null) {
                          break;
                      }
                  }
                  node = parent;
                }
            }
        }  
        if(uri == null) {
            if(namespacePrefix.equals(MappingNodeConstants.DEFAULT_NAMESPACE_PREFIX)) {
                uri = NodeDescriptor.DEFAULT_NAMESPACE_URI;
            } else if(namespacePrefix.equals(MappingNodeConstants.INSTANCES_NAMESPACE_PREFIX)) {
                uri = MappingNodeConstants.INSTANCES_NAMESPACE;
            }else {
                String msg = QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30213, new Object[] {namespacePrefix, name}); 
                 throw new TeiidComponentException(QueryPlugin.Event.TEIID30213, msg);
            }
        }
        
        //create NodeDescriptor
        NodeDescriptor descriptor = new NodeDescriptor(name, getQName(name, namespacePrefix), namespacePrefix, uri, namespaceDeclarations, defaultValue, isOptional, isElement, textNormalizationMode, runtimeType, docBuiltInType);
        return descriptor;
    }    

    //get qualified name (with namespace)
    private static String getQName(String name, String prefix){
        if(prefix != null && prefix.length() > 0){
            name = prefix + ":" + name; //$NON-NLS-1$
        }
        return name;
    }    
}
