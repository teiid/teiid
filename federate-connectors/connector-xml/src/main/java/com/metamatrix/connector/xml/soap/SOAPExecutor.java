/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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


package com.metamatrix.connector.xml.soap;


import java.io.Serializable;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.security.auth.callback.CallbackHandler;
import javax.xml.soap.SOAPBody;
import javax.xml.soap.SOAPException;
import javax.xml.soap.SOAPFault;

import org.apache.axis.AxisFault;
import org.apache.axis.EngineConfiguration;
import org.apache.axis.Handler;
import org.apache.axis.Message;
import org.apache.axis.SimpleChain;
import org.apache.axis.SimpleTargetedChain;
import org.apache.axis.client.Call;
import org.apache.axis.client.Service;
import org.apache.axis.configuration.SimpleProvider;
import org.apache.axis.handlers.SimpleSessionHandler;
import org.apache.axis.message.MessageElement;
import org.apache.axis.message.SOAPBodyElement;
import org.apache.axis.message.SOAPEnvelope;
import org.apache.axis.soap.SOAPConstants;
import org.apache.axis.transport.http.HTTPTransport;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.DOMBuilder;
import org.jdom.output.DOMOutputter;
import org.w3c.dom.NodeList;

import com.metamatrix.connector.xml.CachingConnector;
import com.metamatrix.connector.xml.SOAPConnectorState;
import com.metamatrix.connector.xml.TrustedPayloadHandler;
import com.metamatrix.connector.xml.XMLConnectorState;
import com.metamatrix.connector.xml.XMLExecution;
import com.metamatrix.connector.xml.base.CriteriaDesc;
import com.metamatrix.connector.xml.base.DocumentBuilder;
import com.metamatrix.connector.xml.base.DocumentInfo;
import com.metamatrix.connector.xml.base.RequestGenerator;
import com.metamatrix.connector.xml.base.RequestResponseDocumentProducer;
import com.metamatrix.connector.xml.base.Response;
import com.metamatrix.connector.xml.base.XMLDocument;
import com.metamatrix.connector.xml.cache.IDocumentCache;
import com.metamatrix.connector.xml.http.Messages;
import com.metamatrix.connector.xmlsource.soap.SecurityToken;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.exception.ConnectorException;

public class SOAPExecutor extends RequestResponseDocumentProducer {
	
	SOAPEnvelope envelope = new SOAPEnvelope(SOAPConstants.SOAP11_CONSTANTS);
	SecurityToken secToken;
	SOAPBody requestDoc;
	
	public SOAPExecutor(SOAPConnectorState state, XMLExecution execution) throws ConnectorException {
        super((XMLConnectorState)state, execution);
    }

    public Response getXMLResponse(int invocationNumber) throws ConnectorException {
    	Response result;
        CachingConnector connector = getExecution().getConnection().getConnector();
        IDocumentCache cache = getExecution().getCache();
        ExecutionContext exeContext = getExecution().getExeContext();
        String requestID = exeContext.getRequestIdentifier();
        String partID = exeContext.getPartIdentifier();
        String executionID = exeContext.getExecutionCountIdentifier();
        String cacheReference = requestID + partID + executionID + Integer.toString(invocationNumber);
        
        CriteriaDesc criterion = getExecutionInfo().getResponseIDCriterion();
        if (null != criterion) {
            String responseid = (String) (criterion.getValues().get(0));
            getExecution().getConnection().getConnector().createCacheObjectRecord(requestID, partID, executionID,
                  Integer.toString(invocationNumber), responseid);
            result = new Response(responseid, this, cache, cacheReference);
        } else {
        	createRequest(getExecutionInfo().getParameters());
        	Document response = executeCall();
        	DocumentInfo info = xmlExtractor.createDocumentFromJDOM(response, "");
        	XMLDocument doc = new XMLDocument(info.m_domDoc, info.m_externalFiles);
			SOAPDocBuilder.removeEnvelope((SOAPConnectorState)getState(), doc);
			XMLDocument[] docs = new XMLDocument[]{doc};
			String cacheKey = getCacheKey(1);
			String[] cacheKeys = new String[]{cacheKey};
			
			cache.addToCache(cacheKey, doc, info.m_memoryCacheSize, cacheReference);
            connector.createCacheObjectRecord(requestID, partID, executionID,
                  Integer.toString(invocationNumber), cacheKey);
            result = new Response(docs, cacheKeys, this, cache, cacheReference);
			
        }
        return result;
    }

	private Document executeCall() throws ConnectorException {
		try {
			TrustedPayloadHandler handler = getExecution().getConnection().getTrustedPayloadHandler();
			ConnectorEnvironment env = getExecution().getConnection().getConnectorEnv();
			secToken = SecurityToken.getSecurityToken(env, handler);
			
			Service service = new Service(getMyConfig());
			Call call = new Call(service);
			call.setSOAPActionURI(getExecutionInfo().getOtherProperties().getProperty("SOAPAction"));
			call.setTransport(new HTTPTransport());
			call.setTargetEndpointAddress(removeAngleBrackets(buildUriString()));
			secToken.handleSecurity(call);
			
			Message message = new Message(envelope);
			attemptConditionalLog("XML Connector Framework: request body set to: " + envelope.getBody().getValue()); //$NON-NLS-1$
			getLogger().logDetail("XML Connector Framework: request created"); //$NON-NLS-1$
			SOAPEnvelope response = call.invoke(message);
			SOAPBody responseBody = response.getBody();
			if(responseBody.hasFault() && ((SOAPConnectorState)getState()).isExceptionOnFault()) {
				SOAPFault fault = responseBody.getFault();
				String msgRaw = Messages.getString("SOAPExecutor.soap.fault.message"); //$NON-NLS-1$
				String msg = MessageFormat.format(msgRaw, new Object[] {
						fault.getFaultString(),
						fault.getFaultCode(),
						fault.getDetail(),
						 });
				throw new ConnectorException(msg);
			}
			org.w3c.dom.Document doc = responseBody.getOwnerDocument();
			return new DOMBuilder().build(doc);
		} catch (AxisFault e) {
			throw new ConnectorException(e);
		} catch (Exception e) {
			throw new ConnectorException(e);
		}
	}


	private EngineConfiguration getMyConfig() {
		SimpleProvider clientConfig=new SimpleProvider(); 
	       Handler sessionHandler=(Handler)new SimpleSessionHandler(); 
	       SimpleChain reqHandler = new SimpleChain();
	       if (secToken instanceof CallbackHandler) {
               reqHandler.addHandler(new org.apache.ws.axis.security.WSDoAllSender());
           } 
	       SimpleChain respHandler=new SimpleChain(); 
	       reqHandler.addHandler(sessionHandler); 
	       respHandler.addHandler(sessionHandler); 
	       Handler pivot=(Handler)new FilteringHTTPSender(getState(), getLogger()); 
	       Handler transport=new SimpleTargetedChain(reqHandler, pivot, respHandler); 
	       clientConfig.deployTransport(HTTPTransport.DEFAULT_TRANSPORT_NAME,transport); 
	       return clientConfig;    
	}

	protected void createRequest(List params)
			throws ConnectorException {
		
		SOAPConnectorState state = (SOAPConnectorState) getState();
		ArrayList requestPerms = RequestGenerator.getRequestPerms(params);
		CriteriaDesc[] queryParameters = (CriteriaDesc[]) requestPerms.get(0);
		
		java.util.List newList = java.util.Arrays.asList(queryParameters);
		ArrayList queryList = new ArrayList(newList);

		ArrayList headerParams = new ArrayList();
		ArrayList bodyParams = new ArrayList();
		sortParams(queryList, headerParams, bodyParams);

		String namespacePrefixes = getExecutionInfo().getOtherProperties().getProperty(RequestResponseDocumentProducer.NAMESPACE_PREFIX_PROPERTY_NAME);
		String inputParmsXPath = getExecutionInfo().getOtherProperties().getProperty(DocumentBuilder.PARM_INPUT_XPATH_TABLE_PROPERTY_NAME); 
		SOAPDocBuilder builder = new SOAPDocBuilder();
		Document doc = builder.createXMLRequestDoc(bodyParams, (SOAPConnectorState)getState(), namespacePrefixes, inputParmsXPath);
		Element docRoot = doc.getRootElement();
		DOMOutputter domOutputter = new DOMOutputter();
		try {
			if (docRoot.getNamespaceURI().equals(SOAPDocBuilder.DUMMY_NS_NAME)) {
				// Since there is no real root - these should all be elements
				org.w3c.dom.Document dummyNode = domOutputter.output(doc);
				NodeList children = dummyNode.getChildNodes().item(0).getChildNodes();
				for (int j = 0; j < children.getLength(); j++) {
					org.w3c.dom.Element child = (org.w3c.dom.Element) children
							.item(j);
					envelope.getBody().addChildElement(new SOAPBodyElement(child));
				}
			} else {
				org.w3c.dom.Document document = domOutputter.output(doc);
				org.w3c.dom.Element child = (org.w3c.dom.Element) document
						.getFirstChild();
				envelope.getBody().addChildElement(new SOAPBodyElement(child));
			}
		} catch (Exception e) {
			throw new ConnectorException(e);
		}
	}

	public String getCacheKey(int i) throws ConnectorException {
        StringBuffer cacheKey = new StringBuffer();
        cacheKey.append("|"); //$NON-NLS-1$
        cacheKey.append(getExecution().getConnection().getUser());
        cacheKey.append("|");
        cacheKey.append(getExecution().getConnection().getQueryId());
        cacheKey.append("|");
        cacheKey.append("|");
        cacheKey.append(buildUriString());
        cacheKey.append("|"); //$NON-NLS-1$
        cacheKey.append(requestDoc);
        return cacheKey.toString();
	}

	public int getDocumentCount() throws ConnectorException {
		return 1;
	}
    private void sortParams(List allParams, List headerParams, List bodyParams) throws ConnectorException {
    	// sort the parameter list into header and body content
    	//replace this later with model extensions
    	Iterator paramIter = allParams.iterator();
    	while(paramIter.hasNext()) {
    		CriteriaDesc desc = (CriteriaDesc) paramIter.next();
    		if(desc.getInputXpath().startsWith(SOAPDocBuilder.soapNSLabel + ":" + SOAPDocBuilder.soapHeader)) { //$NON-NLS-1$
    			headerParams.add(desc);    			
    		} else {
    			bodyParams.add(desc);
    		}
    	}
	}

    public Serializable getRequestObject(int i) throws ConnectorException {
		try {
			return envelope.getBody().getValue();
		} catch (SOAPException e) {
			throw new ConnectorException(e);
		}
	}


}
