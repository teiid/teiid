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

import java.util.ArrayList;
import java.util.Collections;
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
        
        writeElement(writer, Element.ALLOW_ENV_FUNCTION_ELEMENT, node);
        writeElement(writer, Element.ASYNC_THREAD_POOL_ELEMENT, node);
        
    	if (like(node, Element.BUFFER_SERVICE_ELEMENT)){
    		writer.writeStartElement(Element.BUFFER_SERVICE_ELEMENT.getLocalName());
    		writeBufferService(writer, node);
    		writer.writeEndElement();
    	}
    	
    	writeElement(writer, Element.MAX_THREADS_ELEMENT, node);
    	writeElement(writer, Element.MAX_ACTIVE_PLANS_ELEMENT, node);
    	writeElement(writer, Element.USER_REQUEST_SOURCE_CONCURRENCY_ELEMENT, node);
    	writeElement(writer, Element.TIME_SLICE_IN_MILLI_ELEMENT, node);
    	writeElement(writer, Element.MAX_ROWS_FETCH_SIZE_ELEMENT, node);
    	writeElement(writer, Element.LOB_CHUNK_SIZE_IN_KB_ELEMENT, node);
    	writeElement(writer, Element.QUERY_THRESHOLD_IN_SECS_ELEMENT, node);
    	writeElement(writer, Element.MAX_SOURCE_ROWS_ELEMENT, node);
    	writeElement(writer, Element.EXCEPTION_ON_MAX_SOURCE_ROWS_ELEMENT, node);
    	writeElement(writer, Element.DETECTING_CHANGE_EVENTS_ELEMENT, node);
    	writeElement(writer, Element.QUERY_TIMEOUT, node);
    	writeElement(writer, Element.WORKMANAGER, node);

    	writeElement(writer, Element.AUTHORIZATION_VALIDATOR_MODULE_ELEMENT, node);
    	writeElement(writer, Element.POLICY_DECIDER_MODULE_ELEMENT, node);
    	
    	if (like(node, Element.RESULTSET_CACHE_ELEMENT)){
    		writer.writeStartElement(Element.RESULTSET_CACHE_ELEMENT.getLocalName());
    		writeResultsetCacheConfiguration(writer, node);
    		writer.writeEndElement();
    	}    	
    	
    	if (like(node, Element.PREPAREDPLAN_CACHE_ELEMENT)){
    		writer.writeStartElement(Element.PREPAREDPLAN_CACHE_ELEMENT.getLocalName());
    		writePreparedPlanCacheConfiguration(writer, node);
    		writer.writeEndElement();
    	}
    	
    	if (like(node, Element.DISTRIBUTED_CACHE)){
    		writer.writeStartElement(Element.DISTRIBUTED_CACHE.getLocalName());
    		writeObjectReplicatorConfiguration(writer, node);
    		writer.writeEndElement();
    	}
    	   	
    	if (has(node, Element.TRANSPORT_ELEMENT.getLocalName())) {
	    	ArrayList<String> transports = new ArrayList<String>(node.get(Element.TRANSPORT_ELEMENT.getLocalName()).keys());
	    	Collections.sort(transports);
	    	if (!transports.isEmpty()) {
	    		for (String transport:transports) {
	    	        writer.writeStartElement(Element.TRANSPORT_ELEMENT.getLocalName());
	    	        writeTransportConfiguration(writer, node.get(Element.TRANSPORT_ELEMENT.getLocalName(), transport), transport);
	    	        writer.writeEndElement();    			
	    		}
	    	}        
    	}    	
    	
    	if (has(node, Element.TRANSLATOR_ELEMENT.getLocalName())) {
	    	ArrayList<String> translators = new ArrayList<String>(node.get(Element.TRANSLATOR_ELEMENT.getLocalName()).keys());
	    	Collections.sort(translators);
	    	if (!translators.isEmpty()) {
	    		for (String translator:translators) {
	    	        writer.writeStartElement(Element.TRANSLATOR_ELEMENT.getLocalName());
	    	        writeTranslator(writer, node.get(Element.TRANSLATOR_ELEMENT.getLocalName(), translator), translator);
	    	        writer.writeEndElement();    			
	    		}
	    	}        
    	}
        writer.writeEndElement(); // End of subsystem element
    }
    
    private void writeObjectReplicatorConfiguration(XMLExtendedStreamWriter writer, ModelNode node) throws XMLStreamException {
    	writeAttribute(writer, Element.DC_STACK_ATTRIBUTE, node);
	}

	private void writeTranslator(XMLExtendedStreamWriter writer, ModelNode node, String translatorName) throws XMLStreamException {
    	writer.writeAttribute(Element.TRANSLATOR_NAME_ATTRIBUTE.getLocalName(), translatorName);
    	writeAttribute(writer, Element.TRANSLATOR_MODULE_ATTRIBUTE, node);
    }
    
    // write the elements according to the schema defined.
    private void writeTransportConfiguration( XMLExtendedStreamWriter writer, ModelNode node, String transportName) throws XMLStreamException {
    	
    	writer.writeAttribute(Element.TRANSPORT_NAME_ATTRIBUTE.getLocalName(), transportName);
    	writeAttribute(writer, Element.TRANSPORT_SOCKET_BINDING_ATTRIBUTE, node);
    	writeAttribute(writer, Element.TRANSPORT_PROTOCOL_ATTRIBUTE, node);
    	writeAttribute(writer, Element.TRANSPORT_MAX_SOCKET_THREADS_ATTRIBUTE, node);
    	writeAttribute(writer, Element.TRANSPORT_IN_BUFFER_SIZE_ATTRIBUTE, node);
    	writeAttribute(writer, Element.TRANSPORT_OUT_BUFFER_SIZE_ATTRIBUTE, node);
    	
    	// authentication
    	if (like(node, Element.AUTHENTICATION_ELEMENT)) {
			writer.writeStartElement(Element.AUTHENTICATION_ELEMENT.getLocalName());
			writeAttribute(writer, Element.AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE, node);
			writeAttribute(writer, Element.AUTHENTICATION_MAX_SESSIONS_ALLOWED_ATTRIBUTE, node);
			writeAttribute(writer, Element.AUTHENTICATION_SESSION_EXPIRATION_TIME_LIMIT_ATTRIBUTE, node);
			writeAttribute(writer, Element.AUTHENTICATION_KRB5_DOMAIN_ATTRIBUTE, node);
			writer.writeEndElement();
    	}
    	
    	if (like(node, Element.PG_ELEMENT)) {
			writer.writeStartElement(Element.PG_ELEMENT.getLocalName());
			writeAttribute(writer, Element.PG_MAX_LOB_SIZE_ALLOWED_ELEMENT, node);			
			writer.writeEndElement();
    	}    	
    	
    	if (like(node, Element.SSL_ELEMENT)) {
			writer.writeStartElement(Element.SSL_ELEMENT.getLocalName());
			
			writeAttribute(writer, Element.SSL_ENABLE_ATTRIBUTE, node);
			writeAttribute(writer, Element.SSL_MODE_ATTRIBUTE, node);
			writeAttribute(writer, Element.SSL_AUTH_MODE_ATTRIBUTE, node);
			writeAttribute(writer, Element.SSL_SSL_PROTOCOL_ATTRIBUTE, node);
			writeAttribute(writer, Element.SSL_KEY_MANAGEMENT_ALG_ATTRIBUTE, node);
			writeAttribute(writer, Element.SSL_ENABLED_CIPHER_SUITES_ATTRIBUTE, node);

			if (like(node, Element.SSL_KETSTORE_ELEMENT)) {
				writer.writeStartElement(Element.SSL_KETSTORE_ELEMENT.getLocalName());
				writeAttribute(writer, Element.SSL_KETSTORE_NAME_ATTRIBUTE, node);
				writeAttribute(writer, Element.SSL_KETSTORE_PASSWORD_ATTRIBUTE, node);
				writeAttribute(writer, Element.SSL_KETSTORE_TYPE_ATTRIBUTE, node);
				writer.writeEndElement();
			}
			
			if (like(node, Element.SSL_TRUSTSTORE_ELEMENT)) {
				writer.writeStartElement(Element.SSL_TRUSTSTORE_ELEMENT.getLocalName());
				writeAttribute(writer, Element.SSL_TRUSTSTORE_NAME_ATTRIBUTE, node);
				writeAttribute(writer, Element.SSL_TRUSTSTORE_PASSWORD_ATTRIBUTE, node);
				writer.writeEndElement();
			}			
			writer.writeEndElement();
    	}
    }
    
	private void writeBufferService(XMLExtendedStreamWriter writer, ModelNode node) throws XMLStreamException {
		writeAttribute(writer, Element.USE_DISK_ATTRIBUTE, node);
		writeAttribute(writer, Element.INLINE_LOBS, node);
		writeAttribute(writer, Element.PROCESSOR_BATCH_SIZE_ATTRIBUTE, node);
		writeAttribute(writer, Element.CONNECTOR_BATCH_SIZE_ATTRIBUTE, node);
		writeAttribute(writer, Element.MAX_PROCESSING_KB_ATTRIBUTE, node);
		writeAttribute(writer, Element.MAX_RESERVED_KB_ATTRIBUTE, node);
		writeAttribute(writer, Element.MAX_FILE_SIZE_ATTRIBUTE, node);
		writeAttribute(writer, Element.MAX_BUFFER_SPACE_ATTRIBUTE, node);
		writeAttribute(writer, Element.MAX_OPEN_FILES_ATTRIBUTE, node);
		writeAttribute(writer, Element.MEMORY_BUFFER_SPACE_ATTRIBUTE, node);
		writeAttribute(writer, Element.MEMORY_BUFFER_OFFHEAP_ATTRIBUTE, node);
		writeAttribute(writer, Element.MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE, node);
	}

	private void writeResultsetCacheConfiguration(XMLExtendedStreamWriter writer, ModelNode node) throws XMLStreamException {
		writeAttribute(writer, Element.RSC_NAME_ELEMENT, node);
		writeAttribute(writer, Element.RSC_CONTAINER_NAME_ELEMENT, node);
		writeAttribute(writer, Element.RSC_ENABLE_ATTRIBUTE, node);
		writeAttribute(writer, Element.RSC_MAX_STALENESS_ELEMENT, node);
	}

	private void writePreparedPlanCacheConfiguration(XMLExtendedStreamWriter writer, ModelNode node) throws XMLStreamException {
		writeAttribute(writer, Element.PPC_MAX_ENTRIES_ATTRIBUTE, node);
		writeAttribute(writer, Element.PPC_MAX_AGE_IN_SECS_ATTRIBUTE, node);
	}

	private boolean has(ModelNode node, String name) {
        return node.has(name) && node.get(name).isDefined();
    }
	
	private boolean like(ModelNode node, Element element) {
		if (node.isDefined()) {			
			Set<String> keys = node.keys();
			for (String key:keys) {
				if (key.startsWith(element.getLocalName())) {
					return true;
				}
			}
		}
        return false;
    }

    private void writeElement(final XMLExtendedStreamWriter writer, final Element element, final ModelNode node) throws XMLStreamException {
    	if (has(node, element.getModelName())) {
    		String value = node.get(element.getModelName()).asString();
	        if (!element.sameAsDefault(value)) {
	    		writer.writeStartElement(element.getLocalName());
		        writer.writeCharacters(value);
		        writer.writeEndElement();
	        }
    	}
    }     
    
    private void writeAttribute(final XMLExtendedStreamWriter writer, final Element element, final ModelNode node) throws XMLStreamException {
    	if (has(node, element.getModelName())) {
    		String value = node.get(element.getModelName()).asString();
    		if (!element.sameAsDefault(value)) {
    			writer.writeAttribute(element.getLocalName(), value);
    		}
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
    				case EXCEPTION_ON_MAX_SOURCE_ROWS_ELEMENT:
    				case DETECTING_CHANGE_EVENTS_ELEMENT:    					
    					bootServices.get(reader.getLocalName()).set(Boolean.parseBoolean(reader.getElementText()));
    					break;

    				case POLICY_DECIDER_MODULE_ELEMENT:
    				case AUTHORIZATION_VALIDATOR_MODULE_ELEMENT:
    				case WORKMANAGER:    					
    					bootServices.get(reader.getLocalName()).set(reader.getElementText());
    					break;
    					
    				case MAX_THREADS_ELEMENT:
    				case MAX_ACTIVE_PLANS_ELEMENT:
    				case USER_REQUEST_SOURCE_CONCURRENCY_ELEMENT:
    				case TIME_SLICE_IN_MILLI_ELEMENT:
    				case MAX_ROWS_FETCH_SIZE_ELEMENT:
    				case LOB_CHUNK_SIZE_IN_KB_ELEMENT:
    				case QUERY_THRESHOLD_IN_SECS_ELEMENT:
    				case MAX_SOURCE_ROWS_ELEMENT:
    				case QUERY_TIMEOUT:    					
    					bootServices.get(reader.getLocalName()).set(Integer.parseInt(reader.getElementText()));
    					break;

    				case ASYNC_THREAD_POOL_ELEMENT:
    					bootServices.get(reader.getLocalName()).set(reader.getElementText());
    					break;
    					
  					// complex types
    				case DISTRIBUTED_CACHE:
    					parseObjectReplicator(reader, bootServices);
    					break;
    					
    				case BUFFER_SERVICE_ELEMENT:
    					parseBufferService(reader, bootServices);
    					break;
    				
    				case PREPAREDPLAN_CACHE_ELEMENT:
    					parsePreparedPlanCacheConfiguration(reader, bootServices);
    					break;
    				
    				case RESULTSET_CACHE_ELEMENT:
    					parseResultsetCacheConfiguration(reader, bootServices);
    					break;

                    case TRANSPORT_ELEMENT:
                        ModelNode transport = new ModelNode();
                        
                        String name = parseTransport(reader, transport);
                        if (name != null) {
	                        final ModelNode transportAddress = address.clone();
	                        transportAddress.add("transport", name); //$NON-NLS-1$
	                        transportAddress.protect();
	                        transport.get(OP).set(ADD);
	                        transport.get(OP_ADDR).set(transportAddress);
	                        
	                        list.add(transport);  
                        }
                        else {
                        	throw new XMLStreamException();
                        }
                        break;
                        
                    case TRANSLATOR_ELEMENT:
                    	ModelNode translatorNode = new ModelNode();
                    	
                    	String translatorName = parseTranslator(reader, translatorNode);

                    	if (translatorName != null) {
	                        final ModelNode translatorAddress = address.clone();
	                        translatorAddress.add("translator", translatorName); //$NON-NLS-1$
	                        translatorAddress.protect();
	                        translatorNode.get(OP).set(ADD);
	                        translatorNode.get(OP_ADDR).set(translatorAddress);
	                    	
	                        list.add(translatorNode);  
                    	}
                    	else {
                        	throw new XMLStreamException();
                    	}
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
    
    private ModelNode parseObjectReplicator(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
    	if (reader.getAttributeCount() > 0) {
    		for(int i=0; i<reader.getAttributeCount(); i++) {
    			String attrName = reader.getAttributeLocalName(i);
    			String attrValue = reader.getAttributeValue(i);
    			
    			Element element = Element.forName(attrName, Element.DISTRIBUTED_CACHE);
    			switch(element) {
    			case DC_STACK_ATTRIBUTE:
    				node.get(element.getModelName()).set(attrValue);
    				break;
                default: 
                    throw ParseUtils.unexpectedAttribute(reader, i);
    			}    			
    		}
    	}
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT));
    	return node;
	}

	private String parseTransport(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
		String transportName = null; 
    	if (reader.getAttributeCount() > 0) {
    		for(int i=0; i<reader.getAttributeCount(); i++) {
    			String attrName = reader.getAttributeLocalName(i);
    			String attrValue = reader.getAttributeValue(i);
    			Element element = Element.forName(attrName);
    			switch(element) {
    			case TRANSPORT_NAME_ATTRIBUTE:
    			case TRANSLATOR_NAME_ATTRIBUTE:
    				transportName = attrValue;
    				break;    			
    			case TRANSPORT_SOCKET_BINDING_ATTRIBUTE:
    				node.get(element.getModelName()).set(attrValue);
    				break;
    			case TRANSPORT_PROTOCOL_ATTRIBUTE:
    				node.get(element.getModelName()).set(attrValue);
    				break;
    			case TRANSPORT_MAX_SOCKET_THREADS_ATTRIBUTE:
    				node.get(element.getModelName()).set(Integer.parseInt(attrValue));
    				break;
    			case TRANSPORT_IN_BUFFER_SIZE_ATTRIBUTE:
    				node.get(element.getModelName()).set(Integer.parseInt(attrValue));
    				break;
    			case TRANSPORT_OUT_BUFFER_SIZE_ATTRIBUTE:
    				node.get(element.getModelName()).set(Integer.parseInt(attrValue));
    				break;
                default: 
                    throw ParseUtils.unexpectedAttribute(reader, i);    				
    			}
    		}
    	}    	
    	
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
				case AUTHENTICATION_ELEMENT:
					parseAuthentication(reader, node);
					break;
				case PG_ELEMENT:
					parsePg(reader, node);
					break;					
				case SSL_ELEMENT:
					parseSSL(reader, node);
					break;
	
                default: 
                    throw ParseUtils.unexpectedElement(reader);
            }
        }
        return transportName;
    }
	
    private ModelNode parseAuthentication(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
    	if (reader.getAttributeCount() > 0) {
    		for(int i=0; i<reader.getAttributeCount(); i++) {
    			String attrName = reader.getAttributeLocalName(i);
    			String attrValue = reader.getAttributeValue(i);
    			Element element = Element.forName(attrName, Element.AUTHENTICATION_ELEMENT);
    			
    			switch(element) {
    			case AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE:
    				node.get(element.getModelName()).set(attrValue);
    				break;

    			case AUTHENTICATION_KRB5_DOMAIN_ATTRIBUTE:
    				node.get(element.getModelName()).set(attrValue);
    				break;
    				
    			case AUTHENTICATION_MAX_SESSIONS_ALLOWED_ATTRIBUTE:
    				node.get(element.getModelName()).set(Integer.parseInt(attrValue));
    				break;
    				
    			case AUTHENTICATION_SESSION_EXPIRATION_TIME_LIMIT_ATTRIBUTE:
    				node.get(element.getModelName()).set(Integer.parseInt(attrValue));
    				break;

    			default:
    				throw ParseUtils.unexpectedAttribute(reader, i);    			
    			}
    		}
    	}
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT));
    	return node;
    }	
    
    private ModelNode parsePg(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
    	if (reader.getAttributeCount() > 0) {
    		for(int i=0; i<reader.getAttributeCount(); i++) {
    			String attrName = reader.getAttributeLocalName(i);
    			String attrValue = reader.getAttributeValue(i);
    			Element element = Element.forName(attrName, Element.PG_ELEMENT);
    			
    			switch(element) {
    			case PG_MAX_LOB_SIZE_ALLOWED_ELEMENT:
    				node.get(element.getModelName()).set(attrValue);
    				break;
    			default:
    				throw ParseUtils.unexpectedAttribute(reader, i);    			
    			}
    		}
    	}
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT));
    	return node;
    }    
	
    private ModelNode parseSSL(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
    	if (reader.getAttributeCount() > 0) {
    		for(int i=0; i<reader.getAttributeCount(); i++) {
    			String attrName = reader.getAttributeLocalName(i);
    			String attrValue = reader.getAttributeValue(i);
    			Element element = Element.forName(attrName, Element.SSL_ELEMENT);
    			
    			switch(element) {
    			case SSL_ENABLE_ATTRIBUTE:
    			case SSL_MODE_ATTRIBUTE:
    			case SSL_AUTH_MODE_ATTRIBUTE:
    			case SSL_SSL_PROTOCOL_ATTRIBUTE:
    			case SSL_KEY_MANAGEMENT_ALG_ATTRIBUTE:
    			case SSL_ENABLED_CIPHER_SUITES_ATTRIBUTE:
    				node.get(element.getModelName()).set(attrValue);
    				break;

    			default:
    				throw ParseUtils.unexpectedAttribute(reader, i);    			
    			}
    		}
    	}
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
				case SSL_KETSTORE_ELEMENT:
					parseKeystore(reader, node);
					break;
				case SSL_TRUSTSTORE_ELEMENT:
					parseTruststore(reader, node);
					break;
	
                default: 
                    throw ParseUtils.unexpectedElement(reader);
            }
        }
    	return node;
    }		
    
    private ModelNode parseKeystore(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
    	if (reader.getAttributeCount() > 0) {
    		for(int i=0; i<reader.getAttributeCount(); i++) {
    			String attrName = reader.getAttributeLocalName(i);
    			String attrValue = reader.getAttributeValue(i);
    			Element element = Element.forName(attrName, Element.SSL_KETSTORE_ELEMENT);
    			
    			switch(element) {
    			case SSL_KETSTORE_NAME_ATTRIBUTE:
    				node.get(element.getModelName()).set(attrValue);
    				break;
    				
    			case SSL_KETSTORE_PASSWORD_ATTRIBUTE:
    				node.get(element.getModelName()).set(attrValue);
    				break;
    				
    			case SSL_KETSTORE_TYPE_ATTRIBUTE:
    				node.get(element.getModelName()).set(attrValue);
    				break;

    			default:
    				throw ParseUtils.unexpectedAttribute(reader, i);    			
    			}
    		}
    	}
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT));
    	return node;
    }	    
    
    private ModelNode parseTruststore(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
    	if (reader.getAttributeCount() > 0) {
    		for(int i=0; i<reader.getAttributeCount(); i++) {
    			String attrName = reader.getAttributeLocalName(i);
    			String attrValue = reader.getAttributeValue(i);
    			Element element = Element.forName(attrName, Element.SSL_TRUSTSTORE_ELEMENT);
    			
    			switch(element) {
    			case SSL_TRUSTSTORE_NAME_ATTRIBUTE:
    				node.get(element.getModelName()).set(attrValue);
    				break;
    				
    			case SSL_TRUSTSTORE_PASSWORD_ATTRIBUTE:
    				node.get(element.getModelName()).set(attrValue);
    				break;
    				
    			default:
    				throw ParseUtils.unexpectedAttribute(reader, i);    			
    			}
    		}
    	}
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT));
    	return node;
    }	    
    
    private ModelNode parseBufferService(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
    	
    	if (reader.getAttributeCount() > 0) {
    		for(int i=0; i<reader.getAttributeCount(); i++) {
    			String attrName = reader.getAttributeLocalName(i);
    			String attrValue = reader.getAttributeValue(i);
    			Element element = Element.forName(attrName, Element.BUFFER_SERVICE_ELEMENT);
    			
    			switch(element) {
    			case USE_DISK_ATTRIBUTE:
    				node.get(element.getModelName()).set(Boolean.parseBoolean(attrValue));
    				break;
    			case INLINE_LOBS:
    				node.get(element.getModelName()).set(Boolean.parseBoolean(attrValue));
    				break;
    			case PROCESSOR_BATCH_SIZE_ATTRIBUTE:
    				node.get(element.getModelName()).set(Integer.parseInt(attrValue));
    				break;
    			case CONNECTOR_BATCH_SIZE_ATTRIBUTE:
    				node.get(element.getModelName()).set(Integer.parseInt(attrValue));
    				break;
    			case MAX_PROCESSING_KB_ATTRIBUTE:
    				node.get(element.getModelName()).set(Integer.parseInt(attrValue));
    				break;
    			case MAX_RESERVED_KB_ATTRIBUTE:
    				node.get(element.getModelName()).set(Integer.parseInt(attrValue));
    				break;
    			case MAX_OPEN_FILES_ATTRIBUTE:
    				node.get(element.getModelName()).set(Integer.parseInt(attrValue));
    				break;
    			case MAX_FILE_SIZE_ATTRIBUTE:
    				node.get(element.getModelName()).set(Long.parseLong(attrValue));
    				break;
    			case MAX_BUFFER_SPACE_ATTRIBUTE:
    				node.get(element.getModelName()).set(Long.parseLong(attrValue));
    				break;
    			case MEMORY_BUFFER_SPACE_ATTRIBUTE:
    				node.get(element.getModelName()).set(Integer.parseInt(attrValue));
    				break;
    			case MEMORY_BUFFER_OFFHEAP_ATTRIBUTE:
    				node.get(element.getModelName()).set(Boolean.parseBoolean(attrValue));
    				break;
    			case MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE:
    				node.get(element.getModelName()).set(Integer.parseInt(attrValue));
    				break;    				
    			default:
    				throw ParseUtils.unexpectedAttribute(reader, i);    			
    			}
    		}
    	}
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT));
    	return node;
    }
    
    private ModelNode parsePreparedPlanCacheConfiguration(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
    	if (reader.getAttributeCount() > 0) {
    		for(int i=0; i<reader.getAttributeCount(); i++) {
    			String attrName = reader.getAttributeLocalName(i);
    			String attrValue = reader.getAttributeValue(i);
    			Element element = Element.forName(attrName, Element.PREPAREDPLAN_CACHE_ELEMENT);
    			switch(element) {
                case PPC_MAX_ENTRIES_ATTRIBUTE:
                	node.get(element.getModelName()).set(Integer.parseInt(attrValue));
                	break;
                	
                case PPC_MAX_AGE_IN_SECS_ATTRIBUTE:
                	node.get(element.getModelName()).set(Integer.parseInt(attrValue));
                	break;
                	
                default: 
                    throw ParseUtils.unexpectedAttribute(reader, i);
    			}
    		}
    	}    	
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT));
    	return node;
    }    
    
    private ModelNode parseResultsetCacheConfiguration(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
    	if (reader.getAttributeCount() > 0) {
    		for(int i=0; i<reader.getAttributeCount(); i++) {
    			String attrName = reader.getAttributeLocalName(i);
    			String attrValue = reader.getAttributeValue(i);
    			Element element = Element.forName(attrName, Element.RESULTSET_CACHE_ELEMENT);
    			switch(element) {
    			case RSC_CONTAINER_NAME_ELEMENT:
    				node.get(element.getModelName()).set(attrValue);
    				break;
    			case RSC_ENABLE_ATTRIBUTE:
    				node.get(element.getModelName()).set(Boolean.parseBoolean(attrValue));
    				break;
    			case RSC_MAX_STALENESS_ELEMENT:
    				node.get(element.getModelName()).set(Integer.parseInt(attrValue));
    				break;
    			case RSC_NAME_ELEMENT:
    				node.get(element.getModelName()).set(attrValue);
    				break;
    			default: 
                	throw ParseUtils.unexpectedAttribute(reader, i);
    			}    			
    		}
    	}
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT));
    	return node;
    }       
    
    private String parseTranslator(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
    	String translatorName = null;
    	if (reader.getAttributeCount() > 0) {
    		for(int i=0; i<reader.getAttributeCount(); i++) {
    			String attrName = reader.getAttributeLocalName(i);
    			String attrValue = reader.getAttributeValue(i);
    			
    			Element element = Element.forName(attrName);
    			switch(element) {
    			case TRANSPORT_NAME_ATTRIBUTE:
    			case TRANSLATOR_NAME_ATTRIBUTE:
    				translatorName = attrValue;
    				break;
    			case TRANSLATOR_MODULE_ATTRIBUTE:
    				node.get(element.getModelName()).set(attrValue);
    				break;
    			default: 
                	throw ParseUtils.unexpectedAttribute(reader, i);
    			}
    		}
    	}      	
    	while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
    		throw ParseUtils.unexpectedElement(reader);
    	}
    	return translatorName;
    }
}
