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

package com.metamatrix.query.mapping.xml;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.Namespace;
import org.jdom.output.XMLOutputter;

import com.metamatrix.internal.core.xml.JdomHelper;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
 * <p>Outputs a mapping definition in XML format.  The mapping definition is
 * held in object form in an instance of the <code>MappingDefinition</code>
 * class.  This is transferred into an XML <code>Document</code> representation
 * and then written to a file. </p>
 *
 * @see MappingDefinition
 */
public class MappingOutputter {

    private static final String NS_NAME                 = null;
    private static final String NS_URL                  = null;
    private static final String ELEM_ROOT               = MappingNodeConstants.Tags.MAPPING_ROOT_NAME;
    private static final String ELEM_NODE               = MappingNodeConstants.Tags.MAPPING_NODE_NAME;

    /** The namespace for the XML mapping document. */
    private Namespace namespace = Namespace.getNamespace( NS_NAME, NS_URL );

    /**
     * <p>Ouput the current JDOM <code>Document</code> to the output stream.</p>
     *
     * @param stream The output stream
     *
     * @throws IOException if there are problems writing to the file.
     */
    public void write(MappingDocument doc, PrintWriter stream) throws IOException {
        write(doc, stream,false,false);
    }

    /**
     * <p>Ouput the current JDOM <code>Document</code> to the output stream.</p>
     *
     * @param stream The output stream
     * @param newlines true if the output should contain newline characters, or false otherwise
     * @param indent true if the output should be indented, or false otherwise.
     *
     * @throws IOException if there are problems writing to the file.
     */
    public void write(MappingDocument doc, PrintWriter stream, final boolean newlines, final boolean indent ) 
        throws IOException {
        
        String indentString = ""; //$NON-NLS-1$
        if (indent) {
            indentString = "    "; //$NON-NLS-1$
        }
        XMLOutputter outputter = new XMLOutputter(JdomHelper.getFormat(indentString, newlines));
        outputter.output( loadDocument(doc), stream );
    }

    // =========================================================================
    //          D O C U M E N T    B U I L D I N G     M E T H O D S
    // =========================================================================

    /**
     * <p>Load XML document from domain object. </p>
     */
     Document loadDocument(MappingDocument mappingDoc) {
        Document xmlDoc = new Document(new Element(ELEM_ROOT, namespace));
        setDocumentProperties(mappingDoc, xmlDoc.getRootElement());
        
        loadNode(mappingDoc.getRootNode(), xmlDoc.getRootElement());
        return xmlDoc;
     }

     /**
      * Set the document spefic properties
      */
     void setDocumentProperties(MappingDocument mappingDoc, Element rootElement) {
         rootElement.addContent(new Element(MappingNodeConstants.Tags.DOCUMENT_ENCODING).setText(mappingDoc.getDocumentEncoding()));
         rootElement.addContent(new Element(MappingNodeConstants.Tags.FORMATTED_DOCUMENT).setText(Boolean.toString(mappingDoc.isFormatted())));
     }
     
    /**
     * <p>Load XML document from domain object. </p>
     */
    void loadNode( MappingNode node, Element parentElement ) {

        Element element = processNode( node, parentElement );

        Iterator children = node.getChildren().iterator();
        while ( children.hasNext() ) {
            MappingNode child = (MappingNode)children.next();
            loadNode( child, element );
        }
    }

    /**
     * Process a mapping node, creating an XML element for it.
     */
    Element processNode( MappingNode node, Element parentElement ) {

        Element element = new Element( ELEM_NODE, namespace );
        parentElement.addContent( element );

        //namespace declarations have to be handled specially
        Properties namespaces = (Properties)node.getProperty(MappingNodeConstants.Properties.NAMESPACE_DECLARATIONS);
        if (namespaces != null){
            addNamespaceDeclarations(element, namespaces);
        }

        // Only get the property values actually stored in the MappingNode, not
        // default values also
        Map properties = node.getNodeProperties();

        addElementProperties( element, properties );

        return element;
    }

    private void addNamespaceDeclarations(Element element, Properties namespaces){
        Enumeration e = namespaces.propertyNames();
        while (e.hasMoreElements()){
            String prefix = (String)e.nextElement();
            String uri = namespaces.getProperty(prefix);
            Element namespaceDeclaration = new Element(MappingNodeConstants.Tags.NAMESPACE_DECLARATION, namespace);
            if (!prefix.equals(MappingNodeConstants.DEFAULT_NAMESPACE_PREFIX)){
                namespaceDeclaration.addContent( new Element(MappingNodeConstants.Tags.NAMESPACE_DECLARATION_PREFIX).setText(prefix) );
            }
            namespaceDeclaration.addContent( new Element(MappingNodeConstants.Tags.NAMESPACE_DECLARATION_URI).setText(uri) );
            element.addContent( namespaceDeclaration);
        }
    }

    /**
     * Utility to put a property into a map only if the value is not null.
     */
    void addProperty( Map map, String name, Object value ) {
        if ( value != null ) {
            map.put( name, value );
        }
    }

    /**
     * Add a set of properties to an XML node.
     */
    void addElementProperties( Element element, Map properties ) {
        Iterator propNames = MappingNodeConstants.Tags.OUTPUTTER_PROPERTY_TAGS.iterator();
        while ( propNames.hasNext() ) {
            String propName = (String)propNames.next();
            Integer propKey = MappingNodeConstants.getPropertyInteger(propName);
            if ( properties.containsKey(propKey) ) {
                Object value = properties.get(propKey);
                addElementProperty( element, propName, value );
            }
        }
    }

    /**
     * Add a single property to an XML node.
     */
    void addElementProperty( Element element, String name, Object value ) {
        if ( value == null ) {
            throw new IllegalArgumentException( QueryPlugin.Util.getString(ErrorMessageKeys.MAPPING_0010, name) );
        }
        if (value instanceof Collection){
            Iterator i = ((Collection)value).iterator();
            while (i.hasNext()) {
                element.addContent( new Element(name,namespace).setText(getXMLText(i.next())));
			}
        } else {
            element.addContent( new Element(name,namespace).setText(getXMLText(value)));
        }
    }

    // =========================================================================
    //                     U T I L I T Y    M E T H O D S
    // =========================================================================

    /** Utility to return a string, accounting for null. */
    private String getXMLText( Object obj ) {
        if ( obj instanceof String ) {
            return getXMLText( (String)obj );
        } else if ( obj instanceof Integer ) {
            return getXMLText( (Integer)obj );
        } else if ( obj instanceof Boolean ) {
            return getXMLText( (Boolean)obj );
        } else {
            throw new IllegalArgumentException( QueryPlugin.Util.getString(ErrorMessageKeys.MAPPING_0011, obj.getClass().getName() ));
        }
    }

    /** Utility to return a string, accounting for null. */
    private String getXMLText( String str ) {
        return ( str != null ) ? str : ""; //$NON-NLS-1$
    }

    /** Utility to convert an integer to a string */
    private String getXMLText( Integer value ) {
        return value.toString();
    }

    /** Utility to convert a Boolean to a string */
    private String getXMLText( Boolean value ) {
        return value.toString();
    }

} // END CLASS

