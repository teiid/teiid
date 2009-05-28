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

package com.metamatrix.connector.xml.base;

import java.io.File;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

import org.jdom.Document;
import org.jdom.input.SAXBuilder;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.cdk.api.TranslationUtility;
import com.metamatrix.connector.xml.SecureConnectorState;
import com.metamatrix.connector.xml.XMLConnectorState;
import com.metamatrix.connector.xml.file.FileConnectorState;
import com.metamatrix.connector.xml.http.HTTPConnectorState;
import com.metamatrix.connector.xml.soap.SOAPConnectorStateImpl;
import com.metamatrix.connector.xmlsource.soap.SecurityToken;
import com.metamatrix.connector.xmlsource.soap.SoapConnectorProperties;
import com.metamatrix.core.util.UnitTestUtil;
/**
 *
 */
public class ProxyObjectFactory {
	
    public static final String JMS_DESTINATION = "dynamicQueues/topic1";
	public static final String INITIAL_CONTEXT_FACTORY = "org.apache.activemq.jndi.ActiveMQInitialContextFactory";
	private static String docFolder = null;
	
    
    private ProxyObjectFactory() {
 	        
    }
    
    public static ConnectorEnvironment getDefaultTestConnectorEnvironment() {
       Properties props = getDefaultFileProps(); 
       ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, false);
       return env;
    }
    
    public static ConnectorEnvironment getHTTPTestConnectorEnvironment(Properties props) {
        if (props == null) {
        	props = getDefaultHTTPProps(); 
        }
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, false);
        return env;
     }
    
    public static ExecutionContext getDefaultSecurityContext() {
        return EnvironmentUtility.createSecurityContext("MetaMatrixAdmin");
    }
    
    public static ExecutionContext getDefaultExecutionContext() {
        return EnvironmentUtility.createExecutionContext("Request", "testPartId");        
    }
    
    public static ExecutionContext getExecutionContext(String requestID, String partId) {
       return EnvironmentUtility.createExecutionContext(requestID, partId);        
    }
    
    public static Properties getDefaultFileProps() {
       Properties testFileProps = new Properties();
        testFileProps.put(XMLConnectorStateImpl.CACHE_TIMEOUT, new String("500000"));
        testFileProps.put(XMLConnectorStateImpl.MAX_MEMORY_CACHE_SIZE, new String("5000"));
        testFileProps.put(XMLConnectorStateImpl.MAX_FILE_CACHE_SIZE, new String("5000"));
        testFileProps.setProperty(XMLConnectorStateImpl.CACHE_ENABLED, "false");
        testFileProps.put(XMLConnectorStateImpl.FILE_CACHE_LOCATION, UnitTestUtil.getTestScratchPath()+"/test/cache");
        testFileProps.setProperty(XMLConnectorState.STATE_CLASS_PROP, "com.metamatrix.connector.xml.file.FileConnectorState");
        testFileProps.setProperty(XMLConnectorStateImpl.QUERY_PREPROCESS_CLASS, "com.metamatrix.connector.xml.base.NoQueryPreprocessing");
        testFileProps.setProperty(XMLConnectorStateImpl.SAX_FILTER_PROVIDER_CLASS, "com.metamatrix.connector.xml.base.NoExtendedFilters"); 
        testFileProps.put(XMLConnectorStateImpl.CONNECTOR_CAPABILITES, "com.metamatrix.connector.xml.base.XMLCapabilities");
        testFileProps.put(FileConnectorState.FILE_NAME, "state_college2.xml");

        URL url = ProxyObjectFactory.class.getClassLoader().getResource("documents");
    	try {
			testFileProps.setProperty(FileConnectorState.DIRECTORY_PATH, new File(url.toURI()).toString());
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
        
        return testFileProps;
    }
    
    public static Properties getDefaultHTTPProps() {
        Properties testHTTPProps = new Properties();
        testHTTPProps.setProperty(XMLConnectorStateImpl.CACHE_TIMEOUT, new String("5000")); //$NON-NLS-1$
        testHTTPProps.setProperty(XMLConnectorStateImpl.MAX_MEMORY_CACHE_SIZE, new String("50")); //$NON-NLS-1$
        testHTTPProps.setProperty(XMLConnectorStateImpl.MAX_FILE_CACHE_SIZE, new String("50")); //$NON-NLS-1$
        testHTTPProps.setProperty(XMLConnectorStateImpl.CACHE_ENABLED, Boolean.TRUE.toString());
        testHTTPProps.setProperty(XMLConnectorStateImpl.FILE_CACHE_LOCATION, UnitTestUtil.getTestScratchPath()+"/test/cache");//$NON-NLS-1$
        testHTTPProps.setProperty(XMLConnectorStateImpl.MAX_IN_MEMORY_STRING_SIZE, "10"); //$NON-NSL-1$
        testHTTPProps.setProperty(HTTPConnectorState.URI, "http://localhost:8673"); //$NON-NLS-1$
        testHTTPProps.setProperty(HTTPConnectorState.REQUEST_TIMEOUT, "60");	 //$NON-NLS-1$
        testHTTPProps.setProperty(XMLConnectorState.STATE_CLASS_PROP, "com.metamatrix.connector.xml.http.HTTPConnectorState"); //$NON-NLS-1$
        testHTTPProps.setProperty(HTTPConnectorState.HTTP_BASIC_USER, "");
        testHTTPProps.setProperty(HTTPConnectorState.HTTP_BASIC_PASSWORD, "");
        testHTTPProps.setProperty(SecureConnectorState.SECURITY_DESERIALIZER_CLASS, "com.metamatrix.connector.xml.http.DefaultTrustDeserializer");
        testHTTPProps.setProperty(HTTPConnectorState.PARAMETER_METHOD, HTTPConnectorState.PARAMETER_NAME_VALUE);
        testHTTPProps.setProperty(HTTPConnectorState.ACCESS_METHOD, HTTPConnectorState.GET);
        return testHTTPProps;
     }
    
    public static Properties getDefaultXMLRequestProps() {
    	Properties defaultHTTPProps = getDefaultHttpProps();
    	defaultHTTPProps.setProperty(HTTPConnectorState.PARAMETER_METHOD, HTTPConnectorState.PARAMETER_XML_REQUEST);
    	return defaultHTTPProps;
    }
    
    public static Properties getDefaultNameValueRequestProps() {
    	Properties defaultHTTPProps = getDefaultHttpProps();
    	defaultHTTPProps.setProperty(HTTPConnectorState.PARAMETER_METHOD, HTTPConnectorState.PARAMETER_NAME_VALUE);
    	return defaultHTTPProps;
    }
    
    public static Properties getDefaultHttpProps() {
        Properties testHTTPProps = new Properties();
         testHTTPProps.put(XMLConnectorStateImpl.CACHE_TIMEOUT, new String("5000"));
         testHTTPProps.put(XMLConnectorStateImpl.MAX_MEMORY_CACHE_SIZE, new String("5000"));
         testHTTPProps.put(XMLConnectorStateImpl.MAX_FILE_CACHE_SIZE, new String("5000"));
         testHTTPProps.put(XMLConnectorStateImpl.CACHE_ENABLED, Boolean.TRUE);
         testHTTPProps.put(XMLConnectorStateImpl.FILE_CACHE_LOCATION, UnitTestUtil.getTestScratchPath()+"/test/cache");
         testHTTPProps.put(XMLConnectorStateImpl.CONNECTOR_CAPABILITES, "com.metamatrix.connector.xml.base.XMLCapabilities");
         testHTTPProps.setProperty(XMLConnectorState.STATE_CLASS_PROP, "com.metamatrix.connector.xml.http.HTTPConnectorState");
         testHTTPProps.setProperty(XMLConnectorStateImpl.QUERY_PREPROCESS_CLASS, "com.metamatrix.connector.xml.base.NoQueryPreprocessing");
         testHTTPProps.setProperty(XMLConnectorStateImpl.SAX_FILTER_PROVIDER_CLASS, "com.metamatrix.connector.xml.base.NoExtendedFilters");
         testHTTPProps.setProperty(HTTPConnectorState.ACCESS_METHOD, HTTPConnectorState.GET);
         testHTTPProps.setProperty(HTTPConnectorState.PARAMETER_METHOD, HTTPConnectorState.PARAMETER_XML_REQUEST);
         testHTTPProps.setProperty(HTTPConnectorState.URI, "http://0.0.0.0:8673");
         //testHTTPProps.setProperty(HTTPConnectorState.PROXY_URI, "http://0.0.0.0:8673");
         testHTTPProps.setProperty(HTTPConnectorState.REQUEST_TIMEOUT, "60");
         testHTTPProps.setProperty(HTTPConnectorState.XML_PARAMETER_NAME, "XMLRequest");
         testHTTPProps.setProperty(HTTPConnectorState.HTTP_BASIC_USER, "");
         testHTTPProps.setProperty(HTTPConnectorState.HTTP_BASIC_PASSWORD, "");
         testHTTPProps.setProperty(SecureConnectorState.SECURITY_DESERIALIZER_CLASS, "com.metamatrix.connector.xml.http.DefaultTrustDeserializer");
         return testHTTPProps;
     }
    
	
	public static Properties createSOAPState() {
		Properties soapProps = new Properties();
		soapProps.setProperty(SOAPConnectorStateImpl.ENCODING_STYLE_PROPERTY_NAME, SOAPConnectorStateImpl.RPC_ENC_STYLE);
		soapProps.setProperty(SOAPConnectorStateImpl.CONNECTOR_EXCEPTION_ON_SOAP_FAULT, "true");
		return soapProps;
	}
	
	public static ConnectorEnvironment createSOAPStateHTTPBasic() {
		Properties props = createSOAPState();
        props.setProperty(SoapConnectorProperties.AUTHORIZATION_TYPE, SecurityToken.HTTP_BASIC_AUTH); 
        props.setProperty(SoapConnectorProperties.USERNAME, "foo"); //$NON-NLS-1$ 
        props.setProperty(SoapConnectorProperties.PASSWORD, "foopassword"); //$NON-NLS-1$ 
		return EnvironmentUtility.createEnvironment(props);
	}

    public static XMLConnector getDefaultXMLConnector() {
        XMLConnector conn;
        try {
            conn = new XMLConnector();
            conn.start(getDefaultTestConnectorEnvironment());
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            conn = null;
        }
        return conn;
    }

    public static XMLConnector getDefaultHTTPConnector(Properties props) {
        XMLConnector conn;
        try {
            conn = new XMLConnector();
            conn.start(getHTTPTestConnectorEnvironment(props));
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            conn = null;
        }
        return conn;
    }
    
    public static XMLConnectionImpl getDefaultXMLConnection() {
        XMLConnectionImpl connection;
        try {
            ExecutionContext ctx = getDefaultSecurityContext();
            connection = (XMLConnectionImpl) getDefaultXMLConnector().getConnection(ctx);           
        } catch (ConnectorException ce) {
            connection = null;
        } catch (NullPointerException ne) {
            connection = null;
        }
        return connection;        
    }
    
    private static XMLConnectionImpl getHTTPXMLConnection(Properties props) {
    	XMLConnectionImpl connection;
        try {
            ExecutionContext ctx = getDefaultSecurityContext();
            connection = (XMLConnectionImpl) getDefaultHTTPConnector(props).getConnection(ctx);           
        } catch (ConnectorException ce) {
            connection = null;
        } catch (NullPointerException ne) {
            connection = null;
        }
        return connection;
	}
    
    
    public static IQuery getDefaultIQuery(String vdbPath, String queryString) {
        TranslationUtility transUtil = new TranslationUtility(vdbPath);
        IQuery query = (IQuery) transUtil.parseCommand(queryString);
        return query;
    }
    
    public static RuntimeMetadata getDefaultRuntimeMetadata(String vdbPath) {
        TranslationUtility transUtil = new TranslationUtility(vdbPath);
        RuntimeMetadata meta = transUtil.createRuntimeMetadata();        
        return meta;
    }
    
    public static Document getDefaultDocument() throws Exception {
        Document doc = null;        
        SAXBuilder builder = new SAXBuilder();
        StringReader reader = new StringReader("<foo><bar>baz</bar></foo>");        
        doc = builder.build(reader);
        return doc;
    }
    
    public static String getStateCollegeVDBLocation() {
    	return getDocumentsFolder() + "/UnitTests.vdb";
    }
    
    public static String getDocumentsFolder() {
        //is the test running locally or in CruiseControl?
        if (docFolder == null) {
        	URL url = ProxyObjectFactory.class.getClassLoader().getResource("documents");
        	try {
    			docFolder = new File(url.toURI()).toString();
    		} catch (URISyntaxException e) {
    		}
        }
        return docFolder;
    }
    
	public static ConnectorEnvironment getDefaultTestConnectorEnvironment(Properties props) {        
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, false);
        return env;
     }

}
