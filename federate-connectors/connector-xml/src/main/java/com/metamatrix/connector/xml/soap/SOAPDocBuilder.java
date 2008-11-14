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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

import org.jdom.Attribute;
import org.jdom.Content;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.Namespace;

import com.metamatrix.connector.xml.SOAPConnectorState;
import com.metamatrix.connector.xml.base.CriteriaDesc;
import com.metamatrix.connector.xml.base.DocumentBuilder;
import com.metamatrix.connector.xml.base.Response;
import com.metamatrix.connector.xml.base.XMLDocument;
import com.metamatrix.connector.xml.jms.JMSSOAPConnectorState;
import com.metamatrix.data.exception.ConnectorException;

public class SOAPDocBuilder {

	public static final String encodingStyle = "encoding-style"; //$NON-NLS-1$
	public static final String encodingStyleUrl = "http://schemas.xmlsoap.org/soap/encoding/"; //$NON-NLS-1$
	public static final String xsiLabel = "xsi"; //$NON-NLS-1$
	public static final String xsiNS = "http://www.w3.org/1999/XMLSchema-instance"; //$NON-NLS-1$
	public static final String xsdLabel = "xsd"; //$NON-NLS-1$	
	public static final String xsLabel = "xs"; //$NON-NLS-1$
	public static final String xsNS = "http://www.w3.org/2001/XMLSchema";
	public static final String xsdNS = "http://www.w3.org/1999/XMLSchema"; //$NON-NLS-1$
	public final static String soapNSLabel = "SOAP-ENV"; //$NON-NLS-1$
	public final static String soapNS = "http://schemas.xmlsoap.org/soap/envelope/"; //$NON-NLS-1$
	private final static String soapEnvelope = "Envelope"; //$NON-NLS-1$
	public final static String soapBody = "Body"; //$NON-NLS-1$
	public static final String soapHeader= "Header"; //$NON-NLS-1$
	public static final String DUMMY_NS_PREFIX = "mm-dummy";  //$NON-NLS-1$	
	public static final String DUMMY_NS_NAME = "http://www.metamatrix.com/dummy";  //$NON-NLS-1$
	public static final String wsSecNS = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd";  //$NON-NLS-1$
	public static final String wsSecLabel = "wsse";  //$NON-NLS-1$
	public static final String wsSecUtilNS = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd";  //$NON-NLS-1$
	public static final String wsSecUtilLabel = "wsu";  //$NON-NLS-1$
	
	public static final Namespace soapNSObj;
	public static final Namespace xsiNSObj;
	public static final Namespace xsdNSObj;
	public static final Namespace xsNSObj;
	public static final Namespace wsSecNSObj;
	public static final Namespace wsSecUtilNSObj;

	static {
		soapNSObj = Namespace.getNamespace(soapNSLabel, soapNS);
		xsiNSObj = Namespace.getNamespace(xsiLabel, xsiNS);
		xsdNSObj = Namespace.getNamespace(xsdLabel, xsdNS);
		xsNSObj = Namespace.getNamespace(xsLabel, xsNS);
		wsSecNSObj = Namespace.getNamespace(wsSecLabel, wsSecNS);
		wsSecUtilNSObj = Namespace.getNamespace(wsSecUtilLabel, wsSecUtilNS);
	}
	
	public SOAPDocBuilder() {
		super();
	}
	
	public String createSOAPRequest(JMSSOAPConnectorState state, List queryList, String namespacePrefixes, String inputParmsXPath) throws ConnectorException {
		Element body = new Element(soapBody, SOAPDocBuilder.soapNSObj);
		Element envelope = new Element(soapEnvelope, SOAPDocBuilder.soapNSObj);
		envelope.addNamespaceDeclaration(SOAPDocBuilder.xsiNSObj);
		envelope.addNamespaceDeclaration(SOAPDocBuilder.xsdNSObj);
		envelope.addNamespaceDeclaration(SOAPDocBuilder.xsNSObj);
		
		if(state.isUseWSSec()) {
			envelope.addNamespaceDeclaration(SOAPDocBuilder.wsSecNSObj);
			envelope.addNamespaceDeclaration(SOAPDocBuilder.wsSecUtilNSObj);
		}
		// filter out header params here
		// and build the headers elements from them
		Element header = new Element(soapHeader, SOAPDocBuilder.soapNSObj);

		ArrayList headerParams = new ArrayList();
		ArrayList bodyParams = new ArrayList();
		sortParams(queryList, headerParams, bodyParams);

		// if there are headers, set them
		if (headerParams.size() > 0) {
			Document headerDoc = createSOAPHeaderDoc(headerParams, state, namespacePrefixes);
			Element headerRoot = headerDoc.getRootElement();
			Element headerHolder = (Element) headerRoot.getChild("Header").detach();
			List headerChilds = headerHolder.getChildren();
			Object[] children = headerChilds.toArray();
			for (int i = 0; i < children.length; i++) { 
				Element child = (Element) children[i];
				child.detach();
				header.addContent(child);
			}
			
		}
		envelope.addContent(header);

		Document doc = createXMLRequestDoc(bodyParams, state, namespacePrefixes, inputParmsXPath);
		Element docRoot = doc.getRootElement();
		docRoot = (Element) docRoot.detach();
		
		//copy namespaces to envelope
		List addNamespaces = docRoot.getAdditionalNamespaces(); 
		for(Iterator nsIter = addNamespaces.iterator(); nsIter.hasNext();) {
			Namespace ns = (Namespace) nsIter.next();
			// don't add them if they're already there
			if(!(ns.getPrefix().equals(SOAPDocBuilder.xsiNSObj.getPrefix()) 
				&& ns.getURI().equals(SOAPDocBuilder.xsiNSObj.getURI())) &&
			   !(ns.getPrefix().equals(SOAPDocBuilder.xsdNSObj.getPrefix()))
				&& ns.getURI().equals(SOAPDocBuilder.xsdNSObj.getURI())) {
				envelope.addNamespaceDeclaration((Namespace) nsIter.next());	
			}				
		}			
		//TODO: strip dummy root element
		// Here is where I need to check for a dummy element to hold the 
		// document together until we can attach it to the body
		if(docRoot.getNamespaceURI().equals(DUMMY_NS_NAME)) {
			List children = docRoot.getChildren();
			Object[] childarray = children.toArray();
			//Since there is no real root - these should all be elements
			//think about it.
			for (int j=0; j < childarray.length; j++) {
				Element elem = (Element) childarray[j];
				elem.detach();
				body.addContent(elem);
			}					
			docRoot = body;	
		} else {
			body.addContent(docRoot);
		}
		// set encoding attribute
		if (state.isEncoded()) {
			Attribute encStyle = new Attribute(
					SOAPDocBuilder.encodingStyle,
					SOAPDocBuilder.encodingStyleUrl,
					SOAPDocBuilder.xsiNSObj); 
			docRoot.setAttribute(encStyle);
		}				

		// support for SOAP basic auth
		if (state.isUseBasicAuth()) {
			addSoapBasicAuth(header, state.getAuthUser(), state.getAuthPassword());
		} else if (state.isUseWSSec()) {
			addWSSecurityUserToken(header, state.getAuthUser(), state.getAuthPassword());
		}

		envelope.addContent(body);
		doc.setRootElement(envelope);
		String xmlDoc = DocumentBuilder.outputDocToString(doc);
		return xmlDoc;
	}

    private void sortParams(List allParams, List headerParams, List bodyParams) throws ConnectorException {
    	// sort the parameter list into header and body content
    	//replace this later with model extensions
    	Iterator paramIter = allParams.iterator();
    	while(paramIter.hasNext()) {
    		CriteriaDesc desc = (CriteriaDesc) paramIter.next();
    		if(desc.getInputXpath().startsWith(SOAPDocBuilder.soapNSLabel + ":" + soapHeader)) { //$NON-NLS-1$
    			headerParams.add(desc);    			
    		} else {
    			bodyParams.add(desc);
    		}
    	}
	}

	public static void addSoapBasicAuth(Element header, String user, String password) {
		final String soapBasicAuth = "BasicAuth"; //$NON-NLS-1$
		final String authLabel = "auth"; //$NON-NLS-1$
		final String authNS = "http://soap-authentication.org/2001/10/"; //$NON-NLS-1$
		final String attrMustUnderstand = "mustUnderstand"; //$NON-NLS-1$
		final String nameLabel = "Name"; //$NON-NLS-1$
		final String passwordLabel = "Password"; //$NON-NLS-1$
		Namespace authNSObj = Namespace.getNamespace(authLabel, authNS);
		
		Element basicAuth = new Element(soapBasicAuth, authNSObj);
		Attribute mustUnderstand = new Attribute(attrMustUnderstand,
				"1", SOAPDocBuilder.soapNSObj); //$NON-NLS-1$
		basicAuth.setAttribute(mustUnderstand);
		header.addContent(basicAuth);
		Element name = new Element(nameLabel);
		name.addContent(user);
		basicAuth.addContent(name);
		Element pwd = new Element(passwordLabel);
		pwd.addContent(password);
		basicAuth.addContent(pwd);
	}
	
	public static void addWSSecurityUserToken(Element header, String user, String password) {
		final String securityLabel = "Security"; //$NON-NLS-1$
		final String attrMustUnderstand = "mustUnderstand"; //$NON-NLS-1$
		final String usernameTokenLabel = "UsernameToken"; //$NON-NLS-1$
		final String attrId = "Id"; //$NON-NLS-1$
		final String usernameLabel = "Username"; //$NON-NLS-1$
		final String passwordLabel = "Password"; //$NON-NLS-1$
		final String attrPassType = "Type"; //$NON-NLS-1$
		final String passwordTextType = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText";
		final String nonceLabel = "Nonce";  //$NON-NLS-1$
		final String createdLabel = "Created";  //$NON-NLS-1$
		
		Element security = new Element(securityLabel, SOAPDocBuilder.wsSecLabel, SOAPDocBuilder.wsSecNS);
		header.addContent(security);
		
		Attribute mustUnderstand = new Attribute(attrMustUnderstand,
				"1", SOAPDocBuilder.soapNSObj); //$NON-NLS-1$
		security.setAttribute(mustUnderstand);
		
		Element usernameToken = new Element(usernameTokenLabel, SOAPDocBuilder.wsSecLabel, SOAPDocBuilder.wsSecNS);
		security.addContent(usernameToken);
		
		Attribute id = new Attribute(attrId, "mm-soap", SOAPDocBuilder.wsSecUtilNSObj);
		
		usernameToken.setAttribute(id);
		
		Element usernameElem = new Element(usernameLabel, SOAPDocBuilder.wsSecLabel, SOAPDocBuilder.wsSecNS);
		usernameElem.setText(user);
		usernameToken.addContent(usernameElem);
		
		Element passwordElem = new Element(passwordLabel, SOAPDocBuilder.wsSecLabel, SOAPDocBuilder.wsSecNS);
		Attribute passTypeAttr = new Attribute(attrPassType, passwordTextType);
		passwordElem.setAttribute(passTypeAttr);
		passwordElem.setText(password);
		usernameToken.addContent(passwordElem);
		
		String nonce = String.valueOf(Calendar.getInstance().getTimeInMillis()) + user;
		Element nonceElem = new Element(nonceLabel, SOAPDocBuilder.wsSecLabel, SOAPDocBuilder.wsSecNS);
		nonceElem.addContent(String.valueOf(nonce.hashCode()));
		usernameToken.addContent(nonceElem);
		
		Element createdElem = new Element(createdLabel, SOAPDocBuilder.wsSecUtilLabel, SOAPDocBuilder.wsSecUtilNS);
		createdElem.addContent(String.valueOf(Calendar.getInstance().getTimeInMillis()));
		usernameToken.addContent(createdElem);
	}
	
	private Document createSOAPHeaderDoc(List params, SOAPConnectorState state, String namespacePrefixes) throws ConnectorException {
		DocumentBuilder builder = new DocumentBuilder();
		builder.setUseTypeAttributes(state.isEncoded());
		return builder.buildDocument(params, "SOAP-ENV:Header", namespacePrefixes);
	}

	public Document createXMLRequestDoc(List params, SOAPConnectorState state, String namespacePrefixes, String inputParmsXPath) throws ConnectorException {
		DocumentBuilder builder = new DocumentBuilder();
		builder.setUseTypeAttributes(state.isEncoded());
		//use dummy root if there is none
		final String slash = "/";
		final String dotSlash = "./";
		if(inputParmsXPath.equals(dotSlash) || inputParmsXPath.equals(slash) || inputParmsXPath.equals("")) {
			inputParmsXPath = SOAPDocBuilder.DUMMY_NS_PREFIX + ":dummy";
			namespacePrefixes = namespacePrefixes + " xmlns:" + SOAPDocBuilder.DUMMY_NS_PREFIX + "=\"" + SOAPDocBuilder.DUMMY_NS_NAME + "\"";
		}
		return builder.buildDocument(params, inputParmsXPath, namespacePrefixes);
    }
    
    public static void removeEnvelope(SOAPConnectorState state, Response response) throws ConnectorException {
        XMLDocument xmlDoc = response.getDocuments()[0];
        //remove envelope and body
        removeEnvelope(state, xmlDoc);
    }

	public static void removeEnvelope(SOAPConnectorState state,
			XMLDocument xmlDoc) throws ConnectorException {
        Object contextRoot = xmlDoc.getContextRoot();
        //if it came from the cache, its already the body element, not the document
        if(contextRoot instanceof Document) {
            Document doc = (Document)contextRoot;
            Element env = doc.getRootElement();
            Element bod = env.getChild(SOAPDocBuilder.soapBody, SOAPDocBuilder.soapNSObj);
            //is there a soap fault?
            if(bod != null) {
                if (state.isExceptionOnFault() == true) {
                    Element soapFault = null;
                    if ((soapFault = bod.getChild("fault", SOAPDocBuilder.soapNSObj)) != null) {  // yuck.
                        handleSoapFault(soapFault, state);
                    } else if ((soapFault = bod.getChild("Fault", SOAPDocBuilder.soapNSObj)) != null) {
                        handleSoapFault(soapFault, state);
                    }
                }
                xmlDoc.setContextRoot(bod);
            } else {
                xmlDoc.setContextRoot(env);
            }
        }
    }
    
    private static void handleSoapFault(Element soapFault, SOAPConnectorState state) throws ConnectorException {
        String strMessage = soapFault.getChildTextTrim("faultstring"); //$NON-NLS-1$
        Element detailElement = soapFault.getChild("detail");  //$NON-NLS-1$
        if(null != detailElement) {
        	Content detail = detailElement.detach();
        Document detailDoc = new Document((Element) detail);
        String strDetail = DocumentBuilder.outputDocToString(detailDoc);
        state.getLogger().logError(strMessage + " : \n" + strDetail);              //$NON-NLS-1$
        } else {
        	state.getLogger().logError(strMessage); //$NON-NLS-1$
        }
        throw new ConnectorException(strMessage);                       
    }
}
