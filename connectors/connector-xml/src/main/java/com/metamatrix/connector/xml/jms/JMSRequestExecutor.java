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


package com.metamatrix.connector.xml.jms;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.jdom.Document;

import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ConnectorLogger;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.xml.AsynchronousDocumentProducer;
import com.metamatrix.connector.xml.SOAPConnectorState;
import com.metamatrix.connector.xml.base.CriteriaDesc;
import com.metamatrix.connector.xml.base.DocumentBuilder;
import com.metamatrix.connector.xml.base.DocumentInfo;
import com.metamatrix.connector.xml.base.ExecutionInfo;
import com.metamatrix.connector.xml.base.RequestResponseDocumentProducer;
import com.metamatrix.connector.xml.base.Response;
import com.metamatrix.connector.xml.base.XMLDocument;
import com.metamatrix.connector.xml.base.XMLExtractor;
import com.metamatrix.connector.xml.cache.DocumentCache;
import com.metamatrix.connector.xml.cache.IDocumentCache;
import com.metamatrix.connector.xml.soap.SOAPDocBuilder;

public class JMSRequestExecutor implements AsynchronousDocumentProducer , MessageListener {

	private int requestNumber;
	private JMSXMLConnectorState jmsState;
	private List messages;
	private JMSExecution execution;
	private JMSConnection connection;
	private MessageConsumer consumer;
	private MessageProducer producer;
	private XMLExtractor xmlExtractor;
	private String correlationID;
	private String messageSelector;
	private String requestDoc;
	
	//holds an exception produced when receiving a message.
	private JMSException onMessageException;
	
	public JMSRequestExecutor(JMSXMLConnectorState state, JMSExecution execution) throws ConnectorException {
		jmsState = state;
		this.execution = execution;
		connection = (JMSConnection) execution.getConnection();
		requestNumber = execution.getRequestNumber();
		messages = new ArrayList();
		String cacheLocation = jmsState.getCacheLocation();
		File cacheFolder = (cacheLocation == null || cacheLocation.trim().length() ==0) ? null : new File(cacheLocation);
		xmlExtractor = new XMLExtractor(jmsState.getMaxInMemoryStringSize(), jmsState.isPreprocess(), jmsState.isLogRequestResponse(), cacheFolder, jmsState.getLogger());
		correlationID = connection.getQueryId() + "_" + requestNumber;
		messageSelector = "JMSCorrelationID" + " = '" + correlationID + "'";
		try {
			synchronized (this) {
				Session jmsSession =  connection.getJMSSession();
				consumer = jmsSession.createConsumer(connection.getInboundDestination());
				consumer.setMessageListener(this);
				ExecutionInfo info = execution.getInfo();
				connection.start();
				if(RequestResponseDocumentProducer.checkIfRequestIsNeeded(info)) {
                    Properties otherInfo = info.getOtherProperties();
                    String inputParmsXPath = otherInfo.getProperty(DocumentBuilder.PARM_INPUT_XPATH_TABLE_PROPERTY_NAME);
                    String namespacePrefixes = otherInfo.getProperty(RequestResponseDocumentProducer.NAMESPACE_PREFIX_PROPERTY_NAME);
                    if(jmsState instanceof SOAPConnectorState) {
						SOAPDocBuilder builder = new SOAPDocBuilder();
						requestDoc = builder.createSOAPRequest((JMSSOAPConnectorState) jmsState,
								info.getParameters(), namespacePrefixes, inputParmsXPath);
					} else {
						DocumentBuilder builder = new DocumentBuilder();
                        Document doc = builder.buildDocument(info.getParameters(), inputParmsXPath, namespacePrefixes);
						requestDoc = DocumentBuilder.outputDocToString(doc);
					}
					TextMessage outboundMessage = jmsSession.createTextMessage(requestDoc);
					outboundMessage.setJMSCorrelationID(correlationID);
					producer = jmsSession.createProducer(connection.getOutboundDestination());
					producer.send(connection.getOutboundDestination(), outboundMessage,
							jmsState.getMessageDeliveryMode(), jmsState.getMesssagePriority(),
							jmsState.getMessageDuration());
				}
			}
		} catch (JMSException e) {
			throw new ConnectorException(e);
		}
	}

	public XMLDocument recreateDocument(Serializable requestObject)	throws ConnectorException {
		XMLDocument result = null;
		try {
			Session jmsSession = connection.getJMSSession();
			MessageConsumer consumer = 
				jmsSession.createConsumer(connection.getInboundDestination(), messageSelector);
			
			TextMessage outboundMessage = jmsSession.createTextMessage(requestDoc.toString());
			outboundMessage.setJMSCorrelationID(correlationID);
			MessageProducer producer = jmsSession.createProducer(connection.getOutboundDestination());
			producer.send(connection.getOutboundDestination(), outboundMessage,
					jmsState.getMessageDeliveryMode(), jmsState.getMesssagePriority(),
					jmsState.getMessageDuration());
			TextMessage message = (TextMessage) consumer.receive(jmsState.getCacheTimeoutMillis());
			InputStream stream = new ByteArrayInputStream(message.getText().getBytes());
			DocumentInfo info = xmlExtractor.createDocumentFromStream(stream, "", jmsState.getSAXFilterProvider());
	        Document domDoc = info.m_domDoc;
	        result = new XMLDocument(domDoc, info.m_externalFiles);
		} catch (JMSException e) {
			throw new ConnectorException(e);
		}
		return result;
	}

	public Serializable getRequestObject(int i) throws ConnectorException {
		return requestDoc;
	}

	public int getDocumentCount() throws ConnectorException {
		return 1;
	}

	public String getCacheKey(int i) throws ConnectorException {
		return correlationID;
	}

	public Response getXMLResponse(int invocationNumber) throws ConnectorException {
		IDocumentCache cache = execution.getCache();
        ExecutionContext exeContext = execution.getExeContext();
        String requestID = exeContext.getRequestIdentifier();
        String partID = exeContext.getPartIdentifier();
        String executionID = exeContext.getExecutionCountIdentifier();
        String cacheReference = requestID + partID + executionID + Integer.toString(invocationNumber);
		CriteriaDesc criterion = execution.getInfo().getResponseIDCriterion(); 
		if(null != criterion) {
			String responseid = (String)(criterion.getValues().get(0));
            connection.getConnector().createCacheObjectRecord(requestID, partID, executionID,
                  Integer.toString(invocationNumber), responseid);
            return new Response(responseid, this, cache, cacheReference);
		}
		int documentCount = getDocumentCount();
        String[] cacheKeys = new String[documentCount];
        XMLDocument[] docs = new XMLDocument[documentCount];
        for (int i = 0; i < documentCount; i++) {
            String cacheKey = getCacheKey(i);
            XMLDocument doc = DocumentCache.cacheLookup(cache, cacheKey, null);
            if(doc == null) {
                String documentDistinguishingId = "";
                if (documentCount > 1) {
                    documentDistinguishingId = new Integer(i).toString();
                }
                DocumentInfo info;
                TextMessage message = (TextMessage)messages.get(i);
                InputStream stream;
                try {
                	String messageText = message.getText();
                	if(messageText.length() ==  0) {
                		throw new ConnectorException("Message " + message.getJMSMessageID() + 
                				"with CorrelataionID " + message.getJMSCorrelationID() + " was empty");
                	}
                	stream = new ByteArrayInputStream(messageText.getBytes());
                } catch (JMSException e) {
                	String exMessage = Messages.getString("JMSRequestExecutor.exception.getting.message.text");
                	throw new ConnectorException(e, exMessage);
                }
                info = xmlExtractor.createDocumentFromStream(stream, documentDistinguishingId, jmsState.getSAXFilterProvider());
                Document domDoc = info.m_domDoc;
                doc = new XMLDocument(domDoc, info.m_externalFiles);

                cache.addToCache(cacheKey, doc, info.m_memoryCacheSize, cacheReference);
                execution.getConnection().getConnector().createCacheObjectRecord(requestID, partID, executionID,
                		Integer.toString(invocationNumber), cacheKey);

            }
            docs[i] = doc;
            cacheKeys[i] = cacheKey;
        }
        connection.getConnector().createCacheObjectRecord(requestID, partID, executionID,
              Integer.toString(invocationNumber), cacheKeys[0]);
        return new Response(docs, cacheKeys, this, cache, cacheReference);
	}

	public boolean hasResponse() throws ConnectorException {
		synchronized (this) {
			if(this.onMessageException != null) {
                throw new ConnectorException(this.onMessageException, 
                        Messages.getString("JMSRequestExecutor.exception.onMessage"));
            }
            boolean result = true;
			if(null != producer) {
				result =  messages.isEmpty() ? false : true;
			}
			return result;
		}
	}

	/////////////////////////////////////////////////////
	// MessageListener implementation
	public void onMessage(Message message) {
		synchronized (this) {
			ConnectorLogger logger = connection.getConnector().getLogger();
			try {
				logger.logTrace(message.getJMSMessageID());
				logger.logTrace(message.getJMSCorrelationID());
				logger.logTrace(message.getJMSType());
				this.messages.add(message);
			} catch (JMSException e) {
				this.onMessageException = e;
			}
		}
	}
	/////////////////////////////////////////////////////

}
