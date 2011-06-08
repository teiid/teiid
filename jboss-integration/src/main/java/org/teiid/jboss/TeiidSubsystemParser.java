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
package org.teiid.jboss;

import static org.jboss.as.controller.parsing.ParseUtils.requireNoAttributes;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.*;

import java.util.List;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.jboss.as.controller.parsing.ParseUtils;
import org.jboss.as.controller.persistence.SubsystemMarshallingContext;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.staxmapper.XMLElementReader;
import org.jboss.staxmapper.XMLElementWriter;
import org.jboss.staxmapper.XMLExtendedStreamReader;
import org.jboss.staxmapper.XMLExtendedStreamWriter;

class TeiidSubsystemParser implements XMLStreamConstants, XMLElementReader<List<ModelNode>>, XMLElementWriter<SubsystemMarshallingContext> {

    @Override
    public void writeContent(final XMLExtendedStreamWriter writer, final SubsystemMarshallingContext context) throws XMLStreamException {
        context.startSubsystemElement(Namespace.CURRENT.getUri(), false);
        writer.writeStartElement(Configuration.QUERY_ENGINE);

        ModelNode node = context.getModelNode();
        ModelNode teiidRuntime = node.require(Configuration.QUERY_ENGINE);
        //writeElement(writer, Element.batchSize, teiidRuntime.require("batch-size"));

        //writer.writeEndElement(); // End teiid-runtime element.
        writer.writeEndElement(); // End of subsystem element
    }
    
    private boolean has(ModelNode node, String name) {
        return node.has(name) && node.get(name).isDefined();
    }

    private void writeElement(final XMLExtendedStreamWriter writer, final Element element, final ModelNode value)
            throws XMLStreamException {
        writer.writeStartElement(element.getLocalName());
        writer.writeCharacters(value.asString());
        writer.writeEndElement();
    }        

    @Override
    public void readElement(final XMLExtendedStreamReader reader, final List<ModelNode> list) throws XMLStreamException {
        final ModelNode address = new ModelNode();
        address.add(SUBSYSTEM, TeiidExtension.SUBSYSTEM_NAME);
        address.protect();
            	 
        final ModelNode subsystem = new ModelNode();
        subsystem.get(OP).set(ADD);
        subsystem.get(OP_ADDR).set(address);
        list.add(subsystem);
        
    	// no attributes 
    	requireNoAttributes(reader);

        // elements
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            switch (Namespace.forUri(reader.getNamespaceURI())) {
                case TEIID_1_0: {
                    Element element = Element.forName(reader.getLocalName());
                    switch (element) {
                        case QUERY_ENGINE_ELEMENT:
                        	ModelNode node = parseQueryEngine(reader);
//                        	node.get(OP).set(ADD);
//                        	ModelNode nodeAddress = address.clone();
//                        	nodeAddress.add(Configuration.QUERY_ENGINE, "teiid-query-engine"); // should this be for each instance name? // //$NON-NLS-1$
//                        	nodeAddress.protect();
//                          node.get(OP_ADDR).set(nodeAddress);       
//                          list.add(node);
                        	subsystem.get(Configuration.QUERY_ENGINE).set(node);
                            break;                            
                        default: 
                            throw ParseUtils.unexpectedElement(reader);
                    }
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedElement(reader);
                }
            }
        }  
        
        
    }
    
    private ModelNode parseQueryEngine(XMLExtendedStreamReader reader) throws XMLStreamException {
    	ModelNode node = new ModelNode();
    	
    	if (reader.getAttributeCount() > 0) {
    		for(int i=0; i<reader.getAttributeCount(); i++) {
    			String attrName = reader.getAttributeLocalName(i);
    			String attrValue = reader.getAttributeValue(i);
    			node.get(attrName).set(attrValue);
    		}
    	}    	
    	
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
            	// integers
				case MAX_THREADS_ELEMENT:
				case MAX_ACTIVE_PLANS_ELEMENT:
				case USER_REQUEST_SOURCE_CONCURRENCY_ELEMENT:
				case TIME_SLICE_IN_MILLI_ELEMENT:
				case MAX_ROWS_FETCH_SIZE_ELEMENT:
				case LOB_CHUNK_SIZE_IN_KB_ELEMENT:
				case QUERY_THRESHOLD_IN_SECS_ELEMENT:
				case MAX_SOURCE_ROWS_ELEMENT:
				case MAX_ODBC_LOB_SIZE_ALLOWED_ELEMENT:
				case MAX_SESSIONS_ALLOWED_ELEMENT:
				case SESSION_EXPIRATION_TIME_LIMIT_ELEMENT:
					node.get(reader.getLocalName()).set(Integer.parseInt(reader.getElementText()));
					break;
	
				// booleans
				case USE_DATA_ROLES_ELEMENT:
				case ALLOW_CREATE_TEMPORY_TABLES_BY_DEFAULT_ELEMENT:
				case ALLOW_FUNCTION_CALLS_BY_DEFAULT_ELEMENT:
				case EXCEPTION_ON_MAX_SOURCE_ROWS_ELEMENT:
				case DETECTING_CHANGE_EVENTS_ELEMENT:
				case ALLOW_ENV_FUNCTION_ELEMENT:
					node.get(reader.getLocalName()).set(Boolean.parseBoolean(reader.getElementText()));
					break;

				//Strings
				case EVENT_DISTRIBUTOR_NAME_ELEMENT:
				case JDBC_SECURITY_DOMAIN_ELEMENT:
	
				// complex types
				case BUFFER_SERVICE_ELEMENT:
					node.get(reader.getLocalName()).set(parseBufferConfiguration(reader));
					break;
				case RESULTSET_CACHE_ELEMENT:
					node.get(reader.getLocalName()).set(parseCacheConfiguration(reader));
					break;
				case PREPAREDPLAN_CACHE_ELEMENT:
					node.get(reader.getLocalName()).set(parseCacheConfiguration(reader));
					break;
				case CACHE_FACORY_ELEMENT:
					node.get(reader.getLocalName()).set(parseCacheFacoryConfiguration(reader));
					break;
				case JDBC_ELEMENT:
					node.get(reader.getLocalName()).set(parseSocketConfiguration(reader));
					break;
				case ODBC_ELEMENT:
					node.get(reader.getLocalName()).set(parseSocketConfiguration(reader));
					break;                   
                    
                default: 
                    throw ParseUtils.unexpectedElement(reader);
            }
        }
    	return node;
    }
    
    private ModelNode parseBufferConfiguration(XMLExtendedStreamReader reader) throws XMLStreamException {
    	ModelNode node = new ModelNode();
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
			switch (element) {
			case USE_DISK_ELEMENT:
				node.get(reader.getLocalName()).set(Boolean.parseBoolean(reader.getElementText()));
				break;
			case DISK_DIRECTORY_ELEMENT:
				node.get(reader.getLocalName()).set(reader.getElementText());
				break;
			case PROCESSOR_BATCH_SIZE_ELEMENT:
			case CONNECTOR_BATCH_SIZE_ELEMENT:
			case MAX_RESERVE_BATCH_COLUMNS_ELEMENT:
			case MAX_PROCESSING_BATCH_COLUMNS_ELEMENT:
			case MAX_FILE_SIZE_ELEMENT:
			case MAX_BUFFER_SPACE_ELEMENT:
			case MAX_OPEN_FILES_ELEMENT:
				node.get(reader.getLocalName()).set(Integer.parseInt(reader.getElementText()));
				break;
			default:
				throw ParseUtils.unexpectedElement(reader);
			}
		}
    	return node;
    }
    
    private ModelNode parseCacheConfiguration(XMLExtendedStreamReader reader) throws XMLStreamException {
    	ModelNode node = new ModelNode();

    	if (reader.getAttributeCount() > 0) {
    		for(int i=0; i<reader.getAttributeCount(); i++) {
    			String attrName = reader.getAttributeLocalName(i);
    			String attrValue = reader.getAttributeValue(i);
    			node.get(attrName).set(Boolean.parseBoolean(attrValue));
    		}
    	}    	
 
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
            case MAX_ENTRIES_ELEMENT:
            case MAX_AGE_IN_SECS_ELEMENT:
            case MAX_STALENESS_ELEMENT:
            	node.get(reader.getLocalName()).set(Integer.parseInt(reader.getElementText()));
            	break;
            case CACHE_TYPE_ELEMENT:
            case CACHE_LOCATION_ELEMENT:
            	node.get(reader.getLocalName()).set(reader.getElementText());    
            	break;
            default: 
                throw ParseUtils.unexpectedElement(reader);
            }
        }
    	return node;
    }    
    
    private ModelNode parseCacheFacoryConfiguration(XMLExtendedStreamReader reader) throws XMLStreamException {
    	ModelNode node = new ModelNode();
 
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
            case CACHE_SERVICE_JNDI_NAME_ELEMENT:
            case RESULTSET_CACHE_NAME_ELEMENT:
            	node.get(reader.getLocalName()).set(reader.getElementText());
            	break;
            default: 
                throw ParseUtils.unexpectedElement(reader);
            }
        }
    	return node;
    }       
    
    private ModelNode parseSocketConfiguration(XMLExtendedStreamReader reader) throws XMLStreamException {
    	ModelNode node = new ModelNode();

    	if (reader.getAttributeCount() > 0) {
    		for(int i=0; i<reader.getAttributeCount(); i++) {
    			String attrName = reader.getAttributeLocalName(i);
    			String attrValue = reader.getAttributeValue(i);
    			node.get(attrName).set(Boolean.parseBoolean(attrValue));
    		}
    	}    	
 
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
            case MAX_SOCKET_SIZE_ELEMENT:
            case IN_BUFFER_SIZE_ELEMENT:
            case OUT_BUFFER_SIZE_ELEMENT:
            	node.get(reader.getLocalName()).set(Integer.parseInt(reader.getElementText()));
            	break;
            case SOCKET_BINDING_ELEMENT:
            	node.get(reader.getLocalName()).set(reader.getElementText());
            	break;
            case SSL_ELEMENT:            	
            	node.get(reader.getLocalName()).set(parseSSLConfiguration(reader));
            	break;
            default: 
                throw ParseUtils.unexpectedElement(reader);
            }
        }
    	return node;
    }
    
    private ModelNode parseSSLConfiguration(XMLExtendedStreamReader reader) throws XMLStreamException {
    	ModelNode node = new ModelNode();

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
            case SSL_MODE_ELEMENT:
            case KEY_STORE_FILE_ELEMENT:
            case KEY_STORE_PASSWD_ELEMENT:
            case KEY_STORE_TYPE_ELEMENT:
            case SSL_PROTOCOL_ELEMENT:
            case TRUST_FILE_ELEMENT:
            case TRUST_PASSWD_ELEMENT:
            case AUTH_MODE_ELEMENT:
            case KEY_MANAGEMENT_ALG_ELEMENT:
            	node.get(reader.getLocalName()).set(reader.getElementText());
            	break;
            default: 
                throw ParseUtils.unexpectedElement(reader);
            }
        }
    	return node;
    }    
}
