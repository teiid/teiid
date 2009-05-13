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

package com.metamatrix.soap.util;

import java.util.ArrayList;
import java.util.List;

import com.metamatrix.common.util.WSDLServletUtil;

import junit.framework.TestCase;

/**
 */
public class TestWSDLServletUtil extends TestCase {
	
	public static String HTTP = "http";   //$NON-NLS-1$
	public static String HTTPS = "https"; //$NON-NLS-1$
	public static String DEFAULT_APP_CONTEXT = "/metamatrix-soap"; //$NON-NLS-1$
	public static String OTHER_APP_CONTEXT = "/metamatrix-soapiness"; //$NON-NLS-1$

    public TestWSDLServletUtil(String name) {
        super(name);
    }

    public void testGetMMSAPIUrlAllNullServerNameSecure() {
        String url = WSDLServletUtil.getSqlQueryWebServiceUrl(null, DEFAULT_APP_CONTEXT, true);
        assertEquals("https://null:8443/metamatrix-soap/services/SqlQueryWebService?wsdl", url); //$NON-NLS-1$
    }
    
    public void testGetMMSAPIUrlAllNullServerNameNonSecure() {
        String url = WSDLServletUtil.getSqlQueryWebServiceUrl(null, DEFAULT_APP_CONTEXT, false);
        assertEquals("http://null:8080/metamatrix-soap/services/SqlQueryWebService?wsdl", url); //$NON-NLS-1$
    }
    
    public void testGetMMSAPIUrlValidParametersSecure() {
        String url = WSDLServletUtil.getSqlQueryWebServiceUrl("slntmm01", DEFAULT_APP_CONTEXT, true);  //$NON-NLS-1$
        assertEquals("https://slntmm01:8443/metamatrix-soap/services/SqlQueryWebService?wsdl", url); //$NON-NLS-1$
    }
    
    public void testGetMMSAPIUrlValidParametersNonSecure() {
        String url = WSDLServletUtil.getSqlQueryWebServiceUrl("slntmm01", OTHER_APP_CONTEXT,false); //$NON-NLS-1$
        assertEquals("http://slntmm01:8080/metamatrix-soapiness/services/SqlQueryWebService?wsdl", url); //$NON-NLS-1$
    }
    
    public void testFormatUrlValidParametersNonSecure() {
    	List serverURLs = new ArrayList();
    	serverURLs.add("mm://chicago:31000"); //$NON-NLS-1$
        String url = WSDLServletUtil.formatURL(HTTP,"chicago","8080", DEFAULT_APP_CONTEXT, serverURLs,"testVDB","1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ 
        assertEquals("http://chicago:8080/metamatrix-soap/servlet/ArtifactDocumentService/MetaMatrixDataServices.wsdl?ServerURL=mm://chicago:31000&VDBName=testVDB&VDBVersion=1", url); //$NON-NLS-1$
    }
    
    public void testFormatUrlValidParametersSecure() {
    	List serverURLs = new ArrayList();
    	serverURLs.add("mms://chicago:31000"); //$NON-NLS-1$
        String url = WSDLServletUtil.formatURL(HTTPS,"chicago","8443", DEFAULT_APP_CONTEXT, serverURLs,"testVDB","1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ 
        assertEquals("https://chicago:8443/metamatrix-soap/servlet/ArtifactDocumentService/MetaMatrixDataServices.wsdl?ServerURL=mms://chicago:31000&VDBName=testVDB&VDBVersion=1", url); //$NON-NLS-1$
    }
    
    public void testFormatUrlValidParametersSecureNoPort() {
    	List serverURLs = new ArrayList();
    	serverURLs.add("mms://chicago:31000"); //$NON-NLS-1$
        String url = WSDLServletUtil.formatURL(HTTPS,"chicago",null, DEFAULT_APP_CONTEXT, serverURLs,"testVDB","1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        assertEquals("https://chicago/metamatrix-soap/servlet/ArtifactDocumentService/MetaMatrixDataServices.wsdl?ServerURL=mms://chicago:31000&VDBName=testVDB&VDBVersion=1", url); //$NON-NLS-1$
    }

    public void testFormatUrlValidParametersSecureNoPortNoVdbVersion() {
    	List serverURLs = new ArrayList();
    	serverURLs.add("mms://chicago:31000"); //$NON-NLS-1$
        String url = WSDLServletUtil.formatURL(HTTPS,"chicago",null, OTHER_APP_CONTEXT, serverURLs,"testVDB",null); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("https://chicago/metamatrix-soapiness/servlet/ArtifactDocumentService/MetaMatrixDataServices.wsdl?ServerURL=mms://chicago:31000&VDBName=testVDB", url); //$NON-NLS-1$
    }

    public void testFormatUrlValidParametersSecureNoVdbVersion() {
    	List serverURLs = new ArrayList();
    	serverURLs.add("mms://chicago:31000"); //$NON-NLS-1$
        String url = WSDLServletUtil.formatURL(HTTPS,"chicago","8443", DEFAULT_APP_CONTEXT, serverURLs,"testVDB",""); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ 
        assertEquals("https://chicago:8443/metamatrix-soap/servlet/ArtifactDocumentService/MetaMatrixDataServices.wsdl?ServerURL=mms://chicago:31000&VDBName=testVDB", url); //$NON-NLS-1$
    } 
    
    public void testFormatUrlMultipleServers() {
    	List serverURLs = new ArrayList();
    	serverURLs.add("mm://chicago:31000"); //$NON-NLS-1$
    	serverURLs.add("boston:31000"); //$NON-NLS-1$
        String url = WSDLServletUtil.formatURL(HTTP,"chicago","8080", DEFAULT_APP_CONTEXT, serverURLs,"testVDB","1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ 
        assertEquals("http://chicago:8080/metamatrix-soap/servlet/ArtifactDocumentService/MetaMatrixDataServices.wsdl?ServerURL=mm://chicago:31000%2Cboston:31000&VDBName=testVDB&VDBVersion=1", url); //$NON-NLS-1$
    }
    
}
