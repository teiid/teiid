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


package com.metamatrix.connector.xml.http;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.jdom.Document;
import org.jdom.output.XMLOutputter;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;



/**
 *
 */
public class HTTPRequestor {
    
    private ConnectorLogger m_logger;
    private String m_accessMethod;
    private final byte[] emptyDocument = "<emptyDoc/>".getBytes(); //$NON-NLS-1$
    

    public HTTPRequestor(ConnectorLogger logger, String accessMethod) {
        super();
        m_logger = logger;
        m_accessMethod = accessMethod;
    }
    
    /**
     * @param method
     * @return
     */
    private InputStream executeMethod(HttpClient client, HttpMethod method, boolean allowHttp500) throws ConnectorException {
    	InputStream responseBody = null;
    	final int httpGood = 200;
    	final int http500 = 500;
        try {
        	client.executeMethod(method);
        	int code = method.getStatusCode();
	        if(code == httpGood || (code == http500 && allowHttp500)) {
        		responseBody = method.getResponseBodyAsStream();
        	} else {
        		m_logger.logError("Http Error: " + code + " - " + method.getStatusText());
        		throw new ConnectorException("Bad Http response:" + code + " - " + method.getStatusText());
        	}
            
            //Try to detect and empty HTTP Response and replace it to avoid a SAX error.
            Header contentLength = method.getResponseHeader("CONTENT-LENGTH");
            if(null != contentLength) {
                if(contentLength.getValue().equals("0")){
                    responseBody = new ByteArrayInputStream(emptyDocument);
                    m_logger.logDetail("XML Connector Framework: Empty HTTP response received."); //$NON-NLS-1$
                }
            } else {
                m_logger.logTrace("HTTP Response does not have a CONTENT-LENGTH header");
            }
            
            // Sometimes it is the http response that specifies the character encoding
        	// and sometimes the document itself. This code currently only supports the
        	// case where the document itself specifies the character encoding

        	// TODO: determine the character encoding of the http response, and use it properly
        } catch (HttpException he) {
            String excStr =
                "XML Connector Framework: Http error connecting"; //$NON-NLS-1$
            m_logger.logError(excStr, he);
            he.printStackTrace();
            throw new ConnectorException(excStr);
        } catch (IOException ioe) {
            String excStr =
                "XML Connector Framework: Unable to connect"; //$NON-NLS-1$
            m_logger.logError(excStr, ioe);
            ioe.printStackTrace();
            throw new ConnectorException(excStr);
        }
        return responseBody;
    }


    public HttpMethod generateMethod(String uriString) {        
        //create a method object
        HttpMethod method = null;
        if (m_accessMethod.equals(HTTPConnectorState.GET)) {
            method = new GetMethod(uriString);
            method.setFollowRedirects(true);
        } else {
            method = new PostMethod(uriString);
            method.setFollowRedirects(false);
        }
        method.getParams().makeLenient();
        setStdRequestHeaders(method);
        return method;
    }
    
    protected void setStdRequestHeaders(HttpMethod method) {
        // add standard request headers
        final String contentTypeHeader = "Content-Type"; //$NON-NLS-1$
        final String cType = "text/xml; charset=utf-8"; //$NON-NLS-1$

        final String userAgentHeader = "User-Agent"; //$NON-NLS-1$
        final String uAgent = "MetaMatrix Server"; //$NON-NLS-1$    	

        Header contentType = new Header(contentTypeHeader, cType);
        method.addRequestHeader(contentType);

        Header userAgent = new Header(userAgentHeader, uAgent);
        method.addRequestHeader(userAgent);
    }
    
    public InputStream fetchXMLDocument(HttpClient client, HttpMethod method, boolean allowHttp500)
    throws ConnectorException
    {
    	InputStream responseBody = null;

        //execute the method
        responseBody = executeMethod(client, method, allowHttp500);

        if (responseBody == null) {
                String excStr =
                    "XML Connector Framework: No response was received  '" //$NON-NLS-1$
                        + client.getHostConfiguration().getHost() + ":" + client.getHostConfiguration().getPort(); //$NON-NLS-1$
                m_logger.logError(excStr);
                throw new ConnectorException(excStr);
        }
    
        m_logger.logDetail("XML Connector Framework: Http method executed."); //$NON-NLS-1$
        return responseBody;
    }   
    
    public void validateURL(String uri) throws IOException {
        URL url = new URL(uri);
        URLConnection conn = url.openConnection();
        conn.connect();
    }
    
    protected static String outputStringFromDoc(Document doc) {
        XMLOutputter out = new XMLOutputter();
        return out.outputString(doc).trim();
        
    }
}
