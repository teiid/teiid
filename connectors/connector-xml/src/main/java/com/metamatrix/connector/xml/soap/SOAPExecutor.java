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


package com.metamatrix.connector.xml.soap;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import javax.xml.namespace.QName;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.ws.Dispatch;
import javax.xml.ws.Service;
import javax.xml.ws.soap.SOAPBinding;

import net.sf.saxon.dom.DOMNodeList;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.output.DOMOutputter;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.w3c.dom.NodeList;

import com.metamatrix.connector.xml.Constants;
import com.metamatrix.connector.xml.SOAPConnectorState;
import com.metamatrix.connector.xml.TrustedPayloadHandler;
import com.metamatrix.connector.xml.XMLConnectorState;
import com.metamatrix.connector.xml.XMLExecution;
import com.metamatrix.connector.xml.base.CriteriaDesc;
import com.metamatrix.connector.xml.base.DocumentBuilder;
import com.metamatrix.connector.xml.base.ExecutionInfo;
import com.metamatrix.connector.xml.base.RequestGenerator;
import com.metamatrix.connector.xml.http.HTTPExecutor;
import com.metamatrix.connector.xmlsource.soap.SecurityToken;

public class SOAPExecutor extends HTTPExecutor {
	
	SecurityToken secToken;
	NodeList requestNodes;
	
	public SOAPExecutor(SOAPConnectorState state, XMLExecution execution, ExecutionInfo exeInfo) throws ConnectorException {
        super((XMLConnectorState)state, execution, exeInfo);
    }

	protected InputStream getDocumentStream() throws ConnectorException {
		try {
			TrustedPayloadHandler handler = execution.getConnection().getTrustedPayloadHandler();
			ConnectorEnvironment env = execution.getConnection().getConnectorEnv();
			Properties schemaProperties = getExeInfo().getSchemaProperties();
			secToken = SecurityToken.getSecurityToken(env, handler);
			
			
            QName svcQname = new QName(
            		schemaProperties.getProperty(Constants.SERVICE_NAMESPACE),
            		schemaProperties.getProperty(Constants.SERVICE_NAME));
            QName portQName = new QName(
            		schemaProperties.getProperty(Constants.PORT_NAMESPACE),
            		schemaProperties.getProperty(Constants.PORT_NAME));
            Service svc = Service.create(svcQname);
            svc.addPort(
                    portQName, 
                    SOAPBinding.SOAP11HTTP_BINDING,
                    removeAngleBrackets(buildUriString()));

            Dispatch<Source> dispatch = svc.createDispatch(
                    portQName, 
                    Source.class, 
                    Service.Mode.PAYLOAD);

            StringBuffer buffer = new StringBuffer();
            for (int j = 0; j < requestNodes.getLength(); j++) {
    			org.w3c.dom.Element child = (org.w3c.dom.Element) requestNodes.item(j);
    			buffer.append(child.getNodeValue());
    		}
            
            ByteArrayInputStream bais = new ByteArrayInputStream(buffer.toString().getBytes());
            Source input = new StreamSource(bais);
            // Invoke the operation.
            Source output = dispatch.invoke(input);

            // Process the response.
            StreamResult result = new StreamResult(new ByteArrayOutputStream());
            Transformer trans = TransformerFactory.newInstance().newTransformer();
            trans.transform(output, result);
            ByteArrayOutputStream baos = (ByteArrayOutputStream) result.getOutputStream();

            // Write out the response content.
            String responseContent = new String(baos.toByteArray());
            System.out.println(responseContent);

			return null;
		} catch (Exception e) {
			throw new ConnectorException(e);
		}
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

		String namespacePrefixes = getExeInfo().getOtherProperties().getProperty(Constants.NAMESPACE_PREFIX_PROPERTY_NAME);
		String inputParmsXPath = getExeInfo().getOtherProperties().getProperty(DocumentBuilder.PARM_INPUT_XPATH_TABLE_PROPERTY_NAME); 
		SOAPDocBuilder builder = new SOAPDocBuilder();
		Document doc = builder.createXMLRequestDoc(bodyParams, (SOAPConnectorState)getState(), namespacePrefixes, inputParmsXPath);
		Element docRoot = doc.getRootElement();
		DOMOutputter domOutputter = new DOMOutputter();
		try {
			if (docRoot.getNamespaceURI().equals(SOAPDocBuilder.DUMMY_NS_NAME)) {
				// Since there is no real root - these should all be elements
				org.w3c.dom.Document dummyNode = domOutputter.output(doc);
				requestNodes = dummyNode.getChildNodes().item(0).getChildNodes();
			} else {
				org.w3c.dom.Document document = domOutputter.output(doc);
				List singleNode = new ArrayList();
				singleNode.add(document.getFirstChild());
				requestNodes = new DOMNodeList(singleNode);
			}
		} catch (Exception e) {
			throw new ConnectorException(e);
		}
	}

	protected String getCacheKey(int i) throws ConnectorException {
        StringBuffer cacheKey = new StringBuffer();
        cacheKey.append("|"); //$NON-NLS-1$
        cacheKey.append(execution.getConnection().getUser());
        cacheKey.append("|");
        cacheKey.append(execution.getConnection().getQueryId());
        cacheKey.append("|");
        cacheKey.append(buildUriString());
        cacheKey.append("|"); //$NON-NLS-1$
		for (int j = 0; j < requestNodes.getLength(); j++) {
			org.w3c.dom.Element child = (org.w3c.dom.Element) requestNodes.item(j);
			cacheKey.append(child.toString());
		}
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
}
