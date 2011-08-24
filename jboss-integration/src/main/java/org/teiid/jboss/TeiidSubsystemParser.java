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

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.ADD;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.SUBSYSTEM;
import static org.jboss.as.controller.parsing.ParseUtils.requireNoAttributes;

import java.util.List;
import java.util.Set;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.jboss.as.controller.parsing.ParseUtils;
import org.jboss.as.controller.persistence.SubsystemMarshallingContext;
import org.jboss.dmr.ModelNode;
import org.jboss.staxmapper.XMLElementReader;
import org.jboss.staxmapper.XMLElementWriter;
import org.jboss.staxmapper.XMLExtendedStreamReader;
import org.jboss.staxmapper.XMLExtendedStreamWriter;

class TeiidSubsystemParser implements XMLStreamConstants, XMLElementReader<List<ModelNode>>, XMLElementWriter<SubsystemMarshallingContext> {

    @Override
    public void writeContent(final XMLExtendedStreamWriter writer, final SubsystemMarshallingContext context) throws XMLStreamException {
        context.startSubsystemElement(Namespace.CURRENT.getUri(), false);
        ModelNode node = context.getModelNode();
        if (!node.isDefined()) {
        	return;
        }
        
        writeElement(writer, Element.ASYNC_THREAD_GROUP_ELEMENT, node);
        writeElement(writer, Element.ALLOW_ENV_FUNCTION_ELEMENT, node);
        writeElement(writer, Element.AUTHORIZATION_VALIDATOR_MODULE_ELEMENT, node);
        writeElement(writer, Element.POLICY_DECIDER_MODULE_ELEMENT, node);
        
    	if (has(node, Element.BUFFER_SERVICE_ELEMENT.getLocalName())){
    		writer.writeStartElement(Element.BUFFER_SERVICE_ELEMENT.getLocalName());
    		writeBufferService(writer, node.get(Element.BUFFER_SERVICE_ELEMENT.getLocalName()));
    		writer.writeEndElement();
    	}
    	
    	if (has(node, Element.CACHE_FACORY_ELEMENT.getLocalName())){
    		writer.writeStartElement(Element.CACHE_FACORY_ELEMENT.getLocalName());
    		writeCacheFactoryConfiguration(writer, node.get(Element.CACHE_FACORY_ELEMENT.getLocalName()));
    		writer.writeEndElement();
    	}    	
    	
    	if (has(node, Element.RESULTSET_CACHE_ELEMENT.getLocalName())){
    		writer.writeStartElement(Element.RESULTSET_CACHE_ELEMENT.getLocalName());
    		writeCacheConfiguration(writer, node.get(Element.RESULTSET_CACHE_ELEMENT.getLocalName()));
    		writer.writeEndElement();
    	}
    	
    	if (has(node, Element.PREPAREDPLAN_CACHE_ELEMENT.getLocalName())){
    		writer.writeStartElement(Element.RESULTSET_CACHE_ELEMENT.getLocalName());
    		writeCacheConfiguration(writer, node.get(Element.PREPAREDPLAN_CACHE_ELEMENT.getLocalName()));
    		writer.writeEndElement();
    	}
    	
    	Set<String> engines = node.get(Element.QUERY_ENGINE_ELEMENT.getLocalName()).keys();
    	if (engines != null && !engines.isEmpty()) {
    		for (String engine:engines) {
    	        writer.writeStartElement(Element.QUERY_ENGINE_ELEMENT.getLocalName());
    	        writeQueryEngine(writer, node.get(Element.QUERY_ENGINE_ELEMENT.getLocalName(), engine));
    	        writer.writeEndElement();    			
    		}
    	}
    	
    	Set<String> translators = node.get(Element.TRANSLATOR_ELEMENT.getLocalName()).keys();
    	if (translators != null && !translators.isEmpty()) {
    		for (String translator:translators) {
    	        writer.writeStartElement(Element.TRANSLATOR_ELEMENT.getLocalName());
    	        writeTranslator(writer, node.get(Element.TRANSLATOR_ELEMENT.getLocalName(), translator));
    	        writer.writeEndElement();    			
    		}
    	}        
        writer.writeEndElement(); // End of subsystem element
    }
    
    private void writeTranslator(XMLExtendedStreamWriter writer, ModelNode node) throws XMLStreamException {
    	writeAttribute(writer, Element.TRANSLATOR_NAME_ATTRIBUTE, node);
    	writeAttribute(writer, Element.TRANSLATOR_MODULE_ATTRIBUTE, node);
    }
    
    // write the elements according to the schema defined.
    private void writeQueryEngine( XMLExtendedStreamWriter writer, ModelNode node) throws XMLStreamException {
    	writeAttribute(writer, Element.ENGINE_NAME_ATTRIBUTE, node);
    	
    	writeElement(writer, Element.MAX_THREADS_ELEMENT, node);
    	writeElement(writer, Element.MAX_ACTIVE_PLANS_ELEMENT, node);
    	writeElement(writer, Element.USER_REQUEST_SOURCE_CONCURRENCY_ELEMENT, node);
    	writeElement(writer, Element.TIME_SLICE_IN_MILLI_ELEMENT, node);
    	writeElement(writer, Element.MAX_ROWS_FETCH_SIZE_ELEMENT, node);
    	writeElement(writer, Element.LOB_CHUNK_SIZE_IN_KB_ELEMENT, node);
    	writeElement(writer, Element.AUTHORIZATION_VALIDATOR_MODULE_ELEMENT, node);
    	writeElement(writer, Element.POLICY_DECIDER_MODULE_ELEMENT, node);
    	writeElement(writer, Element.QUERY_THRESHOLD_IN_SECS_ELEMENT, node);
    	writeElement(writer, Element.MAX_SOURCE_ROWS_ELEMENT, node);
    	writeElement(writer, Element.EXCEPTION_ON_MAX_SOURCE_ROWS_ELEMENT, node);
    	writeElement(writer, Element.MAX_ODBC_LOB_SIZE_ALLOWED_ELEMENT, node);
    	writeElement(writer, Element.EVENT_DISTRIBUTOR_NAME_ELEMENT, node);
    	writeElement(writer, Element.DETECTING_CHANGE_EVENTS_ELEMENT, node);
    	writeElement(writer, Element.JDBC_SECURITY_DOMAIN_ELEMENT, node);
    	writeElement(writer, Element.MAX_SESSIONS_ALLOWED_ELEMENT, node);
    	writeElement(writer, Element.SESSION_EXPIRATION_TIME_LIMIT_ELEMENT, node);
    	    	
    	//jdbc
    	if (has(node, Element.JDBC_ELEMENT.getLocalName())){
    		writer.writeStartElement(Element.JDBC_ELEMENT.getLocalName());
    		writeSocketConfiguration(writer, node.get(Element.JDBC_ELEMENT.getLocalName()));
    		writer.writeEndElement();
    	}
    	
    	//odbc
    	if (has(node, Element.ODBC_ELEMENT.getLocalName())) {
    		writer.writeStartElement(Element.ODBC_ELEMENT.getLocalName());
    		writeSocketConfiguration(writer, node.get(Element.ODBC_ELEMENT.getLocalName()));
    		writer.writeEndElement();
    	}
    }
    
    private void writeSocketConfiguration(XMLExtendedStreamWriter writer, ModelNode node) throws XMLStreamException {
    	writeElement(writer, Element.MAX_SOCKET_SIZE_ELEMENT, node);
    	writeElement(writer, Element.IN_BUFFER_SIZE_ELEMENT, node);
    	writeElement(writer, Element.OUT_BUFFER_SIZE_ELEMENT, node);
    	writeElement(writer, Element.SOCKET_BINDING_ELEMENT, node);
    	
    	if (has(node, Element.SSL_ELEMENT.getLocalName())) {
    		writer.writeStartElement(Element.SSL_ELEMENT.getLocalName());
    		writeSSLConfiguration(writer, node.get(Element.SSL_ELEMENT.getLocalName()));
    		writer.writeEndElement();
    	}
	}

	private void writeSSLConfiguration(XMLExtendedStreamWriter writer, ModelNode node) throws XMLStreamException {
		writeElement(writer, Element.SSL_MODE_ELEMENT, node);
		writeElement(writer, Element.KEY_STORE_FILE_ELEMENT, node);
		writeElement(writer, Element.KEY_STORE_PASSWD_ELEMENT, node);
		writeElement(writer, Element.KEY_STORE_TYPE_ELEMENT, node);
		writeElement(writer, Element.SSL_PROTOCOL_ELEMENT, node);
		writeElement(writer, Element.TRUST_FILE_ELEMENT, node);
		writeElement(writer, Element.TRUST_PASSWD_ELEMENT, node);
		writeElement(writer, Element.AUTH_MODE_ELEMENT, node);    
		writeElement(writer, Element.KEY_MANAGEMENT_ALG_ELEMENT, node);  
	}

	private void writeBufferService(XMLExtendedStreamWriter writer, ModelNode node) throws XMLStreamException {
		writeElement(writer, Element.USE_DISK_ELEMENT, node);
		writeElement(writer, Element.PROCESSOR_BATCH_SIZE_ELEMENT, node);
		writeElement(writer, Element.CONNECTOR_BATCH_SIZE_ELEMENT, node);
		writeElement(writer, Element.MAX_PROCESSING_KB_ELEMENT, node);
		writeElement(writer, Element.MAX_RESERVED_KB_ELEMENT, node);
		writeElement(writer, Element.MAX_FILE_SIZE_ELEMENT, node);
		writeElement(writer, Element.MAX_BUFFER_SPACE_ELEMENT, node);
		writeElement(writer, Element.MAX_OPEN_FILES_ELEMENT, node);
	}

	private void writeCacheFactoryConfiguration(XMLExtendedStreamWriter writer, ModelNode node) throws XMLStreamException {
		writeElement(writer, Element.CACHE_SERVICE_JNDI_NAME_ELEMENT, node);
		writeElement(writer, Element.RESULTSET_CACHE_NAME_ELEMENT, node);
	}

	private void writeCacheConfiguration(XMLExtendedStreamWriter writer, ModelNode node) throws XMLStreamException {
		writeElement(writer, Element.MAX_ENTRIES_ELEMENT, node);
		writeElement(writer, Element.MAX_AGE_IN_SECS_ELEMENT, node);
		writeElement(writer, Element.MAX_STALENESS_ELEMENT, node);
		writeElement(writer, Element.CACHE_TYPE_ELEMENT, node);
		writeElement(writer, Element.CACHE_LOCATION_ELEMENT, node);
	}

	private boolean has(ModelNode node, String name) {
        return node.has(name) && node.get(name).isDefined();
    }

    private void writeElement(final XMLExtendedStreamWriter writer, final Element element, final ModelNode node) throws XMLStreamException {
    	if (has(node, element.getLocalName())) {
	        writer.writeStartElement(element.getLocalName());
	        writer.writeCharacters(node.get(element.getLocalName()).asString());
	        writer.writeEndElement();
    	}
    }        
    
    private void writeAttribute(final XMLExtendedStreamWriter writer, final Element element, final ModelNode node) throws XMLStreamException {
    	if (has(node, element.getLocalName())) {
	        writer.writeAttribute(element.getLocalName(), node.get(element.getLocalName()).asString());
    	}
    }     

    @Override
    public void readElement(final XMLExtendedStreamReader reader, final List<ModelNode> list) throws XMLStreamException {
        final ModelNode address = new ModelNode();
        address.add(SUBSYSTEM, TeiidExtension.TEIID_SUBSYSTEM);
        address.protect();
        
        final ModelNode bootServices = new ModelNode();
        bootServices.get(OP).set(ADD);
        bootServices.get(OP_ADDR).set(address);
        list.add(bootServices);  
        
    	// no attributes 
    	requireNoAttributes(reader);

        // elements
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            switch (Namespace.forUri(reader.getNamespaceURI())) {
                case TEIID_1_0: {
                    Element element = Element.forName(reader.getLocalName());
                    switch (element) {
    				case ALLOW_ENV_FUNCTION_ELEMENT:
    					bootServices.get(reader.getLocalName()).set(Boolean.parseBoolean(reader.getElementText()));
    					break;

    				case AUTHORIZATION_VALIDATOR_MODULE_ELEMENT:
    				case POLICY_DECIDER_MODULE_ELEMENT:
    				case ASYNC_THREAD_GROUP_ELEMENT:
    					bootServices.get(reader.getLocalName()).set(reader.getElementText());
    					break;
    					
    					// complex types
    				case BUFFER_SERVICE_ELEMENT:
    					bootServices.get(reader.getLocalName()).set(parseBufferConfiguration(reader));
    					break;
    				case RESULTSET_CACHE_ELEMENT:
    					bootServices.get(reader.getLocalName()).set(parseCacheConfiguration(reader));
    					break;
    				case PREPAREDPLAN_CACHE_ELEMENT:
    					bootServices.get(reader.getLocalName()).set(parseCacheConfiguration(reader));
    					break;
    				case CACHE_FACORY_ELEMENT:
    					bootServices.get(reader.getLocalName()).set(parseCacheFacoryConfiguration(reader));
    					break;

                    case QUERY_ENGINE_ELEMENT:
                        ModelNode engineNode = parseQueryEngine(reader, new ModelNode());
                        
                        final ModelNode engineAddress = address.clone();
                        engineAddress.add(Configuration.QUERY_ENGINE, engineNode.require(Configuration.ENGINE_NAME).asString());
                        engineAddress.protect();
                        engineNode.get(OP).set(ADD);
                        engineNode.get(OP_ADDR).set(engineAddress);
                        
                        list.add(engineNode);  
                        break;
                        
                    case TRANSLATOR_ELEMENT:
                    	ModelNode translatorNode = parseTranslator(reader, new ModelNode());

                        final ModelNode translatorAddress = address.clone();
                        translatorAddress.add(Configuration.TRANSLATOR, translatorNode.require(Configuration.TRANSLATOR_NAME).asString());
                        translatorAddress.protect();
                        
                        translatorNode.get(OP).set(ADD);
                        translatorNode.get(OP_ADDR).set(translatorAddress);
                    	
                        list.add(translatorNode);  
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
    
    private ModelNode parseQueryEngine(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
    	
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
				case EXCEPTION_ON_MAX_SOURCE_ROWS_ELEMENT:
				case DETECTING_CHANGE_EVENTS_ELEMENT:
				case ALLOW_ENV_FUNCTION_ELEMENT:
					node.get(reader.getLocalName()).set(Boolean.parseBoolean(reader.getElementText()));
					break;

				//Strings
				case EVENT_DISTRIBUTOR_NAME_ELEMENT:
				case JDBC_SECURITY_DOMAIN_ELEMENT:
					node.get(reader.getLocalName()).set(reader.getElementText());
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
			case PROCESSOR_BATCH_SIZE_ELEMENT:
			case CONNECTOR_BATCH_SIZE_ELEMENT:
			case MAX_PROCESSING_KB_ELEMENT:
			case MAX_RESERVED_KB_ELEMENT:
			case MAX_OPEN_FILES_ELEMENT:
				node.get(reader.getLocalName()).set(Integer.parseInt(reader.getElementText()));
				break;
			case MAX_FILE_SIZE_ELEMENT:				
			case MAX_BUFFER_SPACE_ELEMENT:
				node.get(reader.getLocalName()).set(Long.parseLong(reader.getElementText()));
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
    
    private ModelNode parseTranslator(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
    	if (reader.getAttributeCount() > 0) {
    		for(int i=0; i<reader.getAttributeCount(); i++) {
    			String attrName = reader.getAttributeLocalName(i);
    			String attrValue = reader.getAttributeValue(i);
    			
    			Element element = Element.forName(attrName);
    			switch(element) {
    			case TRANSLATOR_NAME_ATTRIBUTE:
    			case TRANSLATOR_MODULE_ATTRIBUTE:
    				node.get(attrName).set(attrValue);
    				break;
    			default: 
                	throw ParseUtils.unexpectedElement(reader);
    			}
    		}
    	}      	
    	while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
    		throw ParseUtils.unexpectedElement(reader);
    	}
    	return node;
    }
}
