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
        writeElement(writer, Element.ASYNC_THREAD_GROUP_ELEMENT, node);
        
    	if (like(node, Element.BUFFER_SERVICE_ELEMENT)){
    		writer.writeStartElement(Element.BUFFER_SERVICE_ELEMENT.getLocalName());
    		writeBufferService(writer, node);
    		writer.writeEndElement();
    	}

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
    	
    	if (like(node, Element.OBJECT_REPLICATOR_ELEMENT)){
    		writer.writeStartElement(Element.OBJECT_REPLICATOR_ELEMENT.getLocalName());
    		writeObjectReplicatorConfiguration(writer, node);
    		writer.writeEndElement();
    	}
    	
    	if (has(node, Element.QUERY_ENGINE_ELEMENT.getLocalName())) {
	    	ArrayList<String> engines = new ArrayList<String>(node.get(Element.QUERY_ENGINE_ELEMENT.getLocalName()).keys());
	    	Collections.sort(engines);
	    	if (!engines.isEmpty()) {
	    		for (String engine:engines) {
	    	        writer.writeStartElement(Element.QUERY_ENGINE_ELEMENT.getLocalName());
	    	        writeQueryEngine(writer, node.get(Element.QUERY_ENGINE_ELEMENT.getLocalName(), engine), engine);
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
    	writeAttribute(writer, Element.OR_STACK_ATTRIBUTE, node);
    	writeAttribute(writer, Element.OR_CLUSTER_NAME_ATTRIBUTE, node);
	}

	private void writeTranslator(XMLExtendedStreamWriter writer, ModelNode node, String translatorName) throws XMLStreamException {
    	writer.writeAttribute(Element.TRANSLATOR_NAME_ATTRIBUTE.getLocalName(), translatorName);
    	writeAttribute(writer, Element.TRANSLATOR_MODULE_ATTRIBUTE, node);
    }
    
    // write the elements according to the schema defined.
    private void writeQueryEngine( XMLExtendedStreamWriter writer, ModelNode node, String engineName) throws XMLStreamException {
    	writer.writeAttribute(Element.ENGINE_NAME_ATTRIBUTE.getLocalName(), engineName);
    	
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
    	writeElement(writer, Element.OBJECT_REPLICATOR_ELEMENT, node);
    	writeElement(writer, Element.DETECTING_CHANGE_EVENTS_ELEMENT, node);
    	
    	if (node.hasDefined(Element.SECURITY_DOMAIN_ELEMENT.getLocalName())) {
    		List<ModelNode> domains = node.get(Element.SECURITY_DOMAIN_ELEMENT.getLocalName()).asList();
    		writeElement(writer, Element.SECURITY_DOMAIN_ELEMENT, domains);
    	}
    	
    	writeElement(writer, Element.MAX_SESSIONS_ALLOWED_ELEMENT, node);
    	writeElement(writer, Element.SESSION_EXPIRATION_TIME_LIMIT_ELEMENT, node);
    	
    	    	
    	//jdbc
    	if (like(node, Element.JDBC_ELEMENT)) {
			writer.writeStartElement(Element.JDBC_ELEMENT.getLocalName());
			writeJDBCSocketConfiguration(writer, node);
			writer.writeEndElement();
    	}
    	
    	//odbc
    	if (like(node, Element.ODBC_ELEMENT)) {
			writer.writeStartElement(Element.ODBC_ELEMENT.getLocalName());
			writeODBCSocketConfiguration(writer, node);
			writer.writeEndElement();
    	}
    }
    
    private void writeJDBCSocketConfiguration(XMLExtendedStreamWriter writer, ModelNode node) throws XMLStreamException {
    	writeAttribute(writer, Element.JDBC_SOCKET_BINDING_ATTRIBUTE, node);
    	writeAttribute(writer, Element.JDBC_MAX_SOCKET_THREAD_SIZE_ATTRIBUTE, node);
    	writeAttribute(writer, Element.JDBC_IN_BUFFER_SIZE_ATTRIBUTE, node);
    	writeAttribute(writer, Element.JDBC_OUT_BUFFER_SIZE_ATTRIBUTE, node);

    	// SSL
    	if (like(node, Element.JDBC_SSL_ELEMENT)) {
			writer.writeStartElement(Element.JDBC_SSL_ELEMENT.getLocalName());
			writeJDBCSSLConfiguration(writer, node);
			writer.writeEndElement();
    	}
	}

	private void writeJDBCSSLConfiguration(XMLExtendedStreamWriter writer, ModelNode node) throws XMLStreamException {
		writeElement(writer, Element.JDBC_SSL_MODE_ELEMENT, node);
		writeElement(writer, Element.JDBC_AUTH_MODE_ELEMENT, node);
		writeElement(writer, Element.JDBC_SSL_PROTOCOL_ELEMENT, node);
		writeElement(writer, Element.JDBC_KEY_MANAGEMENT_ALG_ELEMENT, node);
		writeElement(writer, Element.JDBC_KEY_STORE_FILE_ELEMENT, node);
		writeElement(writer, Element.JDBC_KEY_STORE_PASSWD_ELEMENT, node);
		writeElement(writer, Element.JDBC_KEY_STORE_TYPE_ELEMENT, node);
		writeElement(writer, Element.JDBC_TRUST_FILE_ELEMENT, node);
		writeElement(writer, Element.JDBC_TRUST_PASSWD_ELEMENT, node);
	}
	
    private void writeODBCSocketConfiguration(XMLExtendedStreamWriter writer, ModelNode node) throws XMLStreamException {
    	writeAttribute(writer, Element.ODBC_SOCKET_BINDING_ATTRIBUTE, node);
    	writeAttribute(writer, Element.ODBC_MAX_SOCKET_THREAD_SIZE_ATTRIBUTE, node);
    	writeAttribute(writer, Element.ODBC_IN_BUFFER_SIZE_ATTRIBUTE, node);
    	writeAttribute(writer, Element.ODBC_OUT_BUFFER_SIZE_ATTRIBUTE, node);

    	// SSL
    	if (like(node, Element.ODBC_SSL_ELEMENT)) {
			writer.writeStartElement(Element.ODBC_SSL_ELEMENT.getLocalName());
			writeODBCSSLConfiguration(writer, node);
			writer.writeEndElement();
    	}
	}

	private void writeODBCSSLConfiguration(XMLExtendedStreamWriter writer, ModelNode node) throws XMLStreamException {
		writeElement(writer, Element.ODBC_SSL_MODE_ELEMENT, node);
		writeElement(writer, Element.ODBC_AUTH_MODE_ELEMENT, node);
		writeElement(writer, Element.ODBC_SSL_PROTOCOL_ELEMENT, node);
		writeElement(writer, Element.ODBC_KEY_MANAGEMENT_ALG_ELEMENT, node);
		writeElement(writer, Element.ODBC_KEY_STORE_FILE_ELEMENT, node);
		writeElement(writer, Element.ODBC_KEY_STORE_PASSWD_ELEMENT, node);
		writeElement(writer, Element.ODBC_KEY_STORE_TYPE_ELEMENT, node);
		writeElement(writer, Element.ODBC_TRUST_FILE_ELEMENT, node);
		writeElement(writer, Element.ODBC_TRUST_PASSWD_ELEMENT, node);
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

	private void writeResultsetCacheConfiguration(XMLExtendedStreamWriter writer, ModelNode node) throws XMLStreamException {
		writeAttribute(writer, Element.RSC_NAME_ELEMENT, node);
		writeAttribute(writer, Element.RSC_CONTAINER_NAME_ELEMENT, node);
		writeAttribute(writer, Element.RSC_ENABLE_ATTRIBUTE, node);
		writeAttribute(writer, Element.RSC_MAX_STALENESS_ELEMENT, node);
	}

	private void writePreparedPlanCacheConfiguration(XMLExtendedStreamWriter writer, ModelNode node) throws XMLStreamException {
		writeAttribute(writer, Element.PPC_MAX_ENTRIES_ATTRIBUTE, node);
		writeAttribute(writer, Element.PPC_MAX_AGE_IN_SECS_ATTRIBUTE, node);
		writeAttribute(writer, Element.PPC_MAX_STALENESS_ATTRIBUTE, node);
	}

	private boolean has(ModelNode node, String name) {
        return node.has(name) && node.get(name).isDefined();
    }
	
	private boolean like(ModelNode node, Element element) {
		if (node.isDefined()) {			
			Set<String> keys = node.keys();
			for (String key:keys) {
				if (key.startsWith(element.getModelName())) {
					return true;
				}
			}
		}
        return false;
    }

    private void writeElement(final XMLExtendedStreamWriter writer, final Element element, final ModelNode node) throws XMLStreamException {
    	if (has(node, element.getModelName())) {
	        writer.writeStartElement(element.getLocalName());
	        writer.writeCharacters(node.get(element.getModelName()).asString());
	        writer.writeEndElement();
    	}
    }     
    
    private void writeElement(final XMLExtendedStreamWriter writer, final Element element, final List<ModelNode> nodes) throws XMLStreamException {
    	for (ModelNode node:nodes) {
	    	writer.writeStartElement(element.getLocalName());
	        writer.writeCharacters(node.asString());
	        writer.writeEndElement();
    	}
    }      
    
    private void writeAttribute(final XMLExtendedStreamWriter writer, final Element element, final ModelNode node) throws XMLStreamException {
    	if (has(node, element.getModelName())) {
	        writer.writeAttribute(element.getLocalName(), node.get(element.getModelName()).asString());
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
    				case OBJECT_REPLICATOR_ELEMENT:
    					parseObjectReplicator(reader, bootServices);
    					break;
    					
    				case BUFFER_SERVICE_ELEMENT:
    					parseBufferConfiguration(reader, bootServices);
    					break;
    				case PREPAREDPLAN_CACHE_ELEMENT:
    					parsePreparedPlanCacheConfiguration(reader, bootServices);
    					break;
    				case RESULTSET_CACHE_ELEMENT:
    					parseResultsetCacheConfiguration(reader, bootServices);
    					break;

                    case QUERY_ENGINE_ELEMENT:
                        ModelNode engineNode = new ModelNode();
                        
                        String name = parseQueryEngine(reader, engineNode);
                        
                        final ModelNode engineAddress = address.clone();
                        engineAddress.add(Configuration.QUERY_ENGINE, name);
                        engineAddress.protect();
                        engineNode.get(OP).set(ADD);
                        engineNode.get(OP_ADDR).set(engineAddress);
                        
                        list.add(engineNode);  
                        break;
                        
                    case TRANSLATOR_ELEMENT:
                    	ModelNode translatorNode = new ModelNode();
                    	
                    	String translatorName = parseTranslator(reader, translatorNode);

                        final ModelNode translatorAddress = address.clone();
                        translatorAddress.add(Configuration.TRANSLATOR, translatorName);
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
    
    private ModelNode parseObjectReplicator(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
    	if (reader.getAttributeCount() > 0) {
    		for(int i=0; i<reader.getAttributeCount(); i++) {
    			String attrName = reader.getAttributeLocalName(i);
    			String attrValue = reader.getAttributeValue(i);
    			
    			Element element = Element.forName(attrName, Configuration.OBJECT_REPLICATOR);
    			switch(element) {
    			case OR_STACK_ATTRIBUTE:
    				node.get(Element.OR_STACK_ATTRIBUTE.getModelName()).set(attrValue);
    				break;
    			case OR_CLUSTER_NAME_ATTRIBUTE:
    				node.get(Element.OR_CLUSTER_NAME_ATTRIBUTE.getModelName()).set(attrValue);
    				break;
                default: 
                    throw ParseUtils.unexpectedElement(reader);
    			}    			
    		}
    	}
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT));
    	return node;
	}

	private String parseQueryEngine(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
    	String engineName = "default"; //$NON-NLS-1$
    	
    	if (reader.getAttributeCount() > 0) {
    		for(int i=0; i<reader.getAttributeCount(); i++) {
    			String attrName = reader.getAttributeLocalName(i);
    			String attrValue = reader.getAttributeValue(i);
    			if (attrName.equals(Element.ENGINE_NAME_ATTRIBUTE.getLocalName())) {
    				engineName = attrValue;
    			}
    			else {
        			node.get(attrName).set(attrValue);
    			}
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
					node.get(reader.getLocalName()).set(Boolean.parseBoolean(reader.getElementText()));
					break;

					//List
				case SECURITY_DOMAIN_ELEMENT:
					node.get(reader.getLocalName()).add(reader.getElementText());
					break;
	
				case JDBC_ELEMENT:
					parseJDBCSocketConfiguration(reader, node);
					break;
					
				case ODBC_ELEMENT:
					parseODBCSocketConfiguration(reader, node);
					break;                   
                    
                default: 
                    throw ParseUtils.unexpectedElement(reader);
            }
        }
    	return engineName;
    }
    
    private ModelNode parseBufferConfiguration(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName(), Configuration.BUFFER_SERVICE);
			switch (element) {
			case USE_DISK_ELEMENT:
				node.get(Element.USE_DISK_ELEMENT.getModelName()).set(Boolean.parseBoolean(reader.getElementText()));
				break;
			case PROCESSOR_BATCH_SIZE_ELEMENT:
				node.get(Element.PROCESSOR_BATCH_SIZE_ELEMENT.getModelName()).set(Integer.parseInt(reader.getElementText()));
				break;
			case CONNECTOR_BATCH_SIZE_ELEMENT:
				node.get(Element.CONNECTOR_BATCH_SIZE_ELEMENT.getModelName()).set(Integer.parseInt(reader.getElementText()));
				break;
			case MAX_PROCESSING_KB_ELEMENT:
				node.get(Element.MAX_PROCESSING_KB_ELEMENT.getModelName()).set(Integer.parseInt(reader.getElementText()));
				break;
			case MAX_RESERVED_KB_ELEMENT:
				node.get(Element.MAX_RESERVED_KB_ELEMENT.getModelName()).set(Integer.parseInt(reader.getElementText()));
				break;
			case MAX_OPEN_FILES_ELEMENT:
				node.get(Element.MAX_OPEN_FILES_ELEMENT.getModelName()).set(Integer.parseInt(reader.getElementText()));
				break;
			case MAX_FILE_SIZE_ELEMENT:
				node.get(Element.MAX_FILE_SIZE_ELEMENT.getModelName()).set(Long.parseLong(reader.getElementText()));
				break;
			case MAX_BUFFER_SPACE_ELEMENT:
				node.get(Element.MAX_BUFFER_SPACE_ELEMENT.getModelName()).set(Long.parseLong(reader.getElementText()));
				break;
			default:
				throw ParseUtils.unexpectedElement(reader);
			}
		}
    	return node;
    }
    
    private ModelNode parsePreparedPlanCacheConfiguration(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
    	if (reader.getAttributeCount() > 0) {
    		for(int i=0; i<reader.getAttributeCount(); i++) {
    			String attrName = reader.getAttributeLocalName(i);
    			String attrValue = reader.getAttributeValue(i);
    			Element element = Element.forName(attrName, Configuration.PREPAREDPLAN_CACHE);
    			switch(element) {
                case PPC_MAX_ENTRIES_ATTRIBUTE:
                	node.get(Element.PPC_MAX_ENTRIES_ATTRIBUTE.getModelName()).set(Integer.parseInt(attrValue));
                	break;
                	
                case PPC_MAX_AGE_IN_SECS_ATTRIBUTE:
                	node.get(Element.PPC_MAX_AGE_IN_SECS_ATTRIBUTE.getModelName()).set(Integer.parseInt(attrValue));
                	break;
                	
                case PPC_MAX_STALENESS_ATTRIBUTE:
                	node.get(Element.PPC_MAX_STALENESS_ATTRIBUTE.getModelName()).set(Integer.parseInt(attrValue));
                	break;
                default: 
                    throw ParseUtils.unexpectedElement(reader);
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
    			Element element = Element.forName(attrName, Configuration.RESULTSET_CACHE);
    			switch(element) {
    			case RSC_CONTAINER_NAME_ELEMENT:
    				node.get(Element.RSC_CONTAINER_NAME_ELEMENT.getModelName()).set(attrValue);
    				break;
    			case RSC_ENABLE_ATTRIBUTE:
    				node.get(Element.RSC_ENABLE_ATTRIBUTE.getModelName()).set(Boolean.parseBoolean(attrValue));
    				break;
    			case RSC_MAX_STALENESS_ELEMENT:
    				node.get(Element.RSC_MAX_STALENESS_ELEMENT.getModelName()).set(Integer.parseInt(attrValue));
    				break;
    			case RSC_NAME_ELEMENT:
    				node.get(Element.RSC_NAME_ELEMENT.getModelName()).set(attrValue);
    				break;
    			default: 
                	throw ParseUtils.unexpectedElement(reader);
    			}    			
    		}
    	}
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT));
    	return node;
    }       
    
    private ModelNode parseJDBCSocketConfiguration(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {

    	if (reader.getAttributeCount() > 0) {
    		for(int i=0; i<reader.getAttributeCount(); i++) {
    			String attrName = reader.getAttributeLocalName(i);
    			String attrValue = reader.getAttributeValue(i);

    			Element element = Element.forName(attrName, Configuration.JDBC);
                switch (element) {
    			case JDBC_SOCKET_BINDING_ATTRIBUTE:
    				node.get(Element.JDBC_SOCKET_BINDING_ATTRIBUTE.getModelName()).set(attrValue);
    				break;
                case JDBC_MAX_SOCKET_THREAD_SIZE_ATTRIBUTE:
    				node.get(Element.JDBC_MAX_SOCKET_THREAD_SIZE_ATTRIBUTE.getModelName()).set(Integer.parseInt(attrValue));
    				break;
                case JDBC_IN_BUFFER_SIZE_ATTRIBUTE:
    				node.get(Element.JDBC_IN_BUFFER_SIZE_ATTRIBUTE.getModelName()).set(Integer.parseInt(attrValue));
    				break;
                case JDBC_OUT_BUFFER_SIZE_ATTRIBUTE:
    				node.get(Element.JDBC_OUT_BUFFER_SIZE_ATTRIBUTE.getModelName()).set(Integer.parseInt(attrValue));
    				break;
    			default: 
                	throw ParseUtils.unexpectedElement(reader);
                }
    		}
    	}    	
 
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName(), Configuration.JDBC);
            switch (element) {
            case JDBC_SSL_ELEMENT:            	
            	parseJDBCSSLConfiguration(reader, node);
            	break;
            default: 
                throw ParseUtils.unexpectedElement(reader);
            }
        }
    	return node;
    }
    
    private ModelNode parseODBCSocketConfiguration(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {

    	if (reader.getAttributeCount() > 0) {
    		for(int i=0; i<reader.getAttributeCount(); i++) {
    			String attrName = reader.getAttributeLocalName(i);
    			String attrValue = reader.getAttributeValue(i);

    			Element element = Element.forName(attrName, Configuration.ODBC);
                switch (element) {
    			case ODBC_SOCKET_BINDING_ATTRIBUTE:
    				node.get(Element.ODBC_SOCKET_BINDING_ATTRIBUTE.getModelName()).set(attrValue);
    				break;
                case ODBC_MAX_SOCKET_THREAD_SIZE_ATTRIBUTE:
    				node.get(Element.ODBC_MAX_SOCKET_THREAD_SIZE_ATTRIBUTE.getModelName()).set(Integer.parseInt(attrValue));
    				break;
                case ODBC_IN_BUFFER_SIZE_ATTRIBUTE:
    				node.get(Element.ODBC_IN_BUFFER_SIZE_ATTRIBUTE.getModelName()).set(Integer.parseInt(attrValue));
    				break;
                case ODBC_OUT_BUFFER_SIZE_ATTRIBUTE:
    				node.get(Element.ODBC_OUT_BUFFER_SIZE_ATTRIBUTE.getModelName()).set(Integer.parseInt(attrValue));
    				break;
    			default: 
                	throw ParseUtils.unexpectedElement(reader);
                }
    		}
    	}    	
 
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName(), Configuration.ODBC);
            switch (element) {
            case ODBC_SSL_ELEMENT:            	
            	parseODBCSSLConfiguration(reader, node);
            	break;
            default: 
                throw ParseUtils.unexpectedElement(reader);
            }
        }
    	return node;
    }    
    
    private ModelNode parseJDBCSSLConfiguration(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName(), Configuration.JDBC, Configuration.SSL);
            switch (element) {
            case JDBC_SSL_MODE_ELEMENT:
            	node.get(Element.JDBC_SSL_MODE_ELEMENT.getModelName()).set(reader.getElementText());
            	break;
            case JDBC_KEY_STORE_FILE_ELEMENT:
            	node.get(Element.JDBC_KEY_STORE_FILE_ELEMENT.getModelName()).set(reader.getElementText());
            	break;
            case JDBC_KEY_STORE_PASSWD_ELEMENT:
            	node.get(Element.JDBC_KEY_STORE_PASSWD_ELEMENT.getModelName()).set(reader.getElementText());
            	break;
            case JDBC_KEY_STORE_TYPE_ELEMENT:
            	node.get(Element.JDBC_KEY_STORE_TYPE_ELEMENT.getModelName()).set(reader.getElementText());
            	break;
            case JDBC_SSL_PROTOCOL_ELEMENT:
            	node.get(Element.JDBC_SSL_PROTOCOL_ELEMENT.getModelName()).set(reader.getElementText());
            	break;
            case JDBC_TRUST_FILE_ELEMENT:
            	node.get(Element.JDBC_TRUST_FILE_ELEMENT.getModelName()).set(reader.getElementText());
            	break;
            case JDBC_TRUST_PASSWD_ELEMENT:
            	node.get(Element.JDBC_TRUST_PASSWD_ELEMENT.getModelName()).set(reader.getElementText());
            	break;
            case JDBC_AUTH_MODE_ELEMENT:
            	node.get(Element.JDBC_AUTH_MODE_ELEMENT.getModelName()).set(reader.getElementText());
            	break;
            case JDBC_KEY_MANAGEMENT_ALG_ELEMENT:
            	node.get(Element.JDBC_KEY_MANAGEMENT_ALG_ELEMENT.getModelName()).set(reader.getElementText());
            	break;
            default: 
                throw ParseUtils.unexpectedElement(reader);
            }
        }
    	return node;
    }
    
    private ModelNode parseODBCSSLConfiguration(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName(), Configuration.ODBC, Configuration.SSL);
            switch (element) {
            case ODBC_SSL_MODE_ELEMENT:
            	node.get(Element.ODBC_SSL_MODE_ELEMENT.getModelName()).set(reader.getElementText());
            	break;
            case ODBC_KEY_STORE_FILE_ELEMENT:
            	node.get(Element.ODBC_KEY_STORE_FILE_ELEMENT.getModelName()).set(reader.getElementText());
            	break;
            case ODBC_KEY_STORE_PASSWD_ELEMENT:
            	node.get(Element.ODBC_KEY_STORE_PASSWD_ELEMENT.getModelName()).set(reader.getElementText());
            	break;
            case ODBC_KEY_STORE_TYPE_ELEMENT:
            	node.get(Element.ODBC_KEY_STORE_TYPE_ELEMENT.getModelName()).set(reader.getElementText());
            	break;
            case ODBC_SSL_PROTOCOL_ELEMENT:
            	node.get(Element.ODBC_SSL_PROTOCOL_ELEMENT.getModelName()).set(reader.getElementText());
            	break;
            case ODBC_TRUST_FILE_ELEMENT:
            	node.get(Element.ODBC_TRUST_FILE_ELEMENT.getModelName()).set(reader.getElementText());
            	break;
            case ODBC_TRUST_PASSWD_ELEMENT:
            	node.get(Element.ODBC_TRUST_PASSWD_ELEMENT.getModelName()).set(reader.getElementText());
            	break;
            case ODBC_AUTH_MODE_ELEMENT:
            	node.get(Element.ODBC_AUTH_MODE_ELEMENT.getModelName()).set(reader.getElementText());
            	break;
            case ODBC_KEY_MANAGEMENT_ALG_ELEMENT:
            	node.get(Element.ODBC_KEY_MANAGEMENT_ALG_ELEMENT.getModelName()).set(reader.getElementText());
            	break;
            default: 
                throw ParseUtils.unexpectedElement(reader);
            }
        }
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
    			case TRANSLATOR_NAME_ATTRIBUTE:
    				translatorName = attrValue;
    				break;
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
    	return translatorName;
    }
}
