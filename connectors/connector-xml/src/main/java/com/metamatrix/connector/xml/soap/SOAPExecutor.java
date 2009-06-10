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


import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.ws.Dispatch;
import javax.xml.ws.Service;
import javax.xml.ws.soap.SOAPBinding;

import org.jdom.Document;
import org.jdom.output.XMLOutputter;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;

import com.metamatrix.connector.xml.Constants;
import com.metamatrix.connector.xml.SOAPConnectorState;
import com.metamatrix.connector.xml.TrustedPayloadHandler;
import com.metamatrix.connector.xml.XMLConnectorState;
import com.metamatrix.connector.xml.XMLExecution;
import com.metamatrix.connector.xml.base.CriteriaDesc;
import com.metamatrix.connector.xml.base.DocumentBuilder;
import com.metamatrix.connector.xml.base.ExecutionInfo;
import com.metamatrix.connector.xml.base.RequestGenerator;
import com.metamatrix.connector.xml.cache.CachingOutputStream;
import com.metamatrix.connector.xml.http.HTTPExecutor;
import com.metamatrix.connector.xmlsource.soap.SecurityToken;

public class SOAPExecutor extends HTTPExecutor {
	
	SecurityToken secToken;
	XMLOutputter xmlOutputter = new XMLOutputter();
	Document doc;
	
	public SOAPExecutor(SOAPConnectorState state, XMLExecution execution, ExecutionInfo exeInfo) throws ConnectorException {
        super((XMLConnectorState)state, execution, exeInfo);
    }

	protected InputStream getDocumentStream() throws ConnectorException {
		try {
			TrustedPayloadHandler handler = execution.getConnection().getTrustedPayloadHandler();
			ConnectorEnvironment env = execution.getConnection().getConnectorEnv();
			secToken = SecurityToken.getSecurityToken(env, handler);
			
            QName svcQname = new QName("http://org.apache.cxf", "foo");
            QName portQName = new QName("http://org.apache.cxf", "bar");
            Service svc = Service.create(svcQname);
            svc.addPort(
                    portQName, 
                    SOAPBinding.SOAP11HTTP_BINDING,
                    removeAngleBrackets(buildUriString()));

            Dispatch<Source> dispatch = svc.createDispatch(
                    portQName, 
                    Source.class, 
                    Service.Mode.PAYLOAD);
            
            StringReader reader = new StringReader(xmlOutputter.outputString(doc));
            Source input = new StreamSource(reader);
            // Invoke the operation.
            Source output = dispatch.invoke(input);
            
            // Process the response.
            CachingOutputStream out = new CachingOutputStream(execution.getExeContext(), getCacheKey());
            StreamResult result = new StreamResult(out);
            Transformer trans = TransformerFactory.newInstance().newTransformer();
            trans.transform(output, result);
            return out.getCachedXMLStream();
		} catch (Exception e) {
			throw new ConnectorException(e);
		}
	}

	protected void setRequests(List params, String uriString)
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
		doc = builder.createXMLRequestDoc(bodyParams, (SOAPConnectorState)getState(), namespacePrefixes, inputParmsXPath);
	}

	protected String getCacheKey() throws ConnectorException {
        StringBuffer cacheKey = new StringBuffer();
        cacheKey.append("|"); //$NON-NLS-1$
        cacheKey.append(execution.getConnection().getUser());
        cacheKey.append("|");
        cacheKey.append(execution.getConnection().getQueryId());
        cacheKey.append("|");
        cacheKey.append(buildUriString());
        //cacheKey.append("|"); //$NON-NLS-1$
		//cacheKey.append(xmlOutputter.outputString(doc));
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
