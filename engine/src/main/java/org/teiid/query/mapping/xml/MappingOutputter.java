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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.teiid.query.QueryPlugin;
import org.teiid.query.function.source.XMLSystemFunctions;


/**
 * <p>Outputs a mapping definition in XML format.  The mapping definition is
 * held in object form in an instance of the <code>MappingDefinition</code>
 * class.  This is transferred into an XML <code>Document</code> representation
 * and then written to a file. </p>
 *
 * @see MappingDefinition
 */
public class MappingOutputter {

    private static final String ELEM_ROOT               = MappingNodeConstants.Tags.MAPPING_ROOT_NAME;
    private static final String ELEM_NODE               = MappingNodeConstants.Tags.MAPPING_NODE_NAME;
    
    XMLStreamWriter writer;

    /**
     * @param stream The output stream
     *
     * @throws IOException if there are problems writing to the file.
     */
    public void write(MappingDocument doc, PrintWriter stream) throws IOException {
        try {
        	XMLOutputFactory xof = XMLSystemFunctions.getOutputFactory();
        	writer = xof.createXMLStreamWriter(stream);
        	writer.writeStartDocument("UTF-8", "1.0"); //$NON-NLS-1$ //$NON-NLS-2$
        	writer.writeStartElement(ELEM_ROOT);
        	writeElement(MappingNodeConstants.Tags.DOCUMENT_ENCODING, doc.getDocumentEncoding());
        	writeElement(MappingNodeConstants.Tags.FORMATTED_DOCUMENT, Boolean.toString(doc.isFormatted()));
        	loadNode(doc.getRootNode());
        	writer.writeEndElement();
        	writer.writeEndDocument();
		} catch (XMLStreamException e) {
			throw new IOException(e);
		}
    }
    
    void writeElement(String name, String content) throws XMLStreamException {
    	writer.writeStartElement(name);
    	writer.writeCharacters(content);
    	writer.writeEndElement();
    }

    // =========================================================================
    //          D O C U M E N T    B U I L D I N G     M E T H O D S
    // =========================================================================

    /**
     * <p>Load XML document from domain object. </p>
     * @throws XMLStreamException 
     */
    void loadNode( MappingNode node ) throws XMLStreamException {
    	writer.writeStartElement(ELEM_NODE);

        //namespace declarations have to be handled specially
        Properties namespaces = (Properties)node.getProperty(MappingNodeConstants.Properties.NAMESPACE_DECLARATIONS);
        if (namespaces != null){
            addNamespaceDeclarations(namespaces);
        }

        // Only get the property values actually stored in the MappingNode, not
        // default values also
        Map properties = node.getNodeProperties();

        addElementProperties(properties );

        Iterator children = node.getChildren().iterator();
        while ( children.hasNext() ) {
            MappingNode child = (MappingNode)children.next();
            loadNode( child );
        }
        writer.writeEndElement();
    }

    private void addNamespaceDeclarations(Properties namespaces) throws XMLStreamException{
        Enumeration e = namespaces.propertyNames();
        while (e.hasMoreElements()){
            String prefix = (String)e.nextElement();
            String uri = namespaces.getProperty(prefix);
            writer.writeStartElement(MappingNodeConstants.Tags.NAMESPACE_DECLARATION);
            if (!prefix.equals(MappingNodeConstants.DEFAULT_NAMESPACE_PREFIX)){
            	writeElement(MappingNodeConstants.Tags.NAMESPACE_DECLARATION_PREFIX, prefix);
            }
            writeElement(MappingNodeConstants.Tags.NAMESPACE_DECLARATION_URI, uri);
            writer.writeEndElement();
        }
    }

    /**
     * Add a set of properties to an XML node.
     * @throws XMLStreamException 
     */
    void addElementProperties(Map properties ) throws XMLStreamException {
        Iterator<String> propNames = MappingNodeConstants.Tags.OUTPUTTER_PROPERTY_TAGS.iterator();
        while ( propNames.hasNext() ) {
            String propName = propNames.next();
            MappingNodeConstants.Properties propKey = MappingNodeConstants.getProperty(propName);
            if ( properties.containsKey(propKey) ) {
                Object value = properties.get(propKey);
                addElementProperty( propName, value );
            }
        }
    }

    /**
     * Add a single property to an XML node.
     * @throws XMLStreamException 
     */
    void addElementProperty(String name, Object value ) throws XMLStreamException {
        if ( value == null ) {
            throw new IllegalArgumentException( QueryPlugin.Util.getString("ERR.015.002.0010", name) ); //$NON-NLS-1$
        }
        if (value instanceof Collection){
            Iterator i = ((Collection)value).iterator();
            while (i.hasNext()) {
            	writeElement(name, getXMLText(i.next()));
			}
        } else {
        	writeElement(name, getXMLText(value));
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
            throw new IllegalArgumentException( QueryPlugin.Util.getString("ERR.015.002.0011", obj.getClass().getName() )); //$NON-NLS-1$
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

