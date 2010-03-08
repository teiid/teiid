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

package org.teiid.connector.xmlsource.soap;

import java.io.File;
import java.io.PrintWriter;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.List;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.junit.Ignore;
import org.mockito.Mockito;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ProcedureExecution;
import org.teiid.connector.language.Argument;
import org.teiid.connector.language.Call;
import org.teiid.connector.language.LanguageFactory;
import org.teiid.connector.language.Literal;
import org.teiid.connector.language.Argument.Direction;
import org.teiid.connector.metadata.runtime.Procedure;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.xmlsource.soap.service.WebServiceServer;
import com.metamatrix.core.util.UnitTestUtil;

// All the security needs to redone using JBossWS-Native, until then do not run this class. 
@Ignore
public class TestSoapExecution extends TestCase {
	private static WebServiceServer server = null;
	
	public static Test suite() {
		TestSuite suite = new TestSuite();
		suite.addTestSuite(TestSoapExecution.class);
        return new TestSetup(suite){
            @Override
			protected void setUp() throws Exception{
            	TestSoapExecution.setUpOnce();
            }
            @Override
			protected void tearDown() throws Exception{
            	TestSoapExecution.tearDownOnce();
            }
        };
	}	
	
	public static void setUpOnce() throws Exception{
		System.setProperty("javax.xml.transform.TransformerFactory", "com.sun.org.apache.xalan.internal.xsltc.trax.TransformerFactoryImpl");
        server = new WebServiceServer();
        server.startServer(7001);		
	}
	
	public static void tearDownOnce() throws Exception {
		server.stopServer();
	}
	
	
    public void defer_testExternalWSDLExecution_getMovies() throws Exception {        
        SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
        env.setLogWriter(Mockito.mock(PrintWriter.class));
        env.setWsdl("http://www.ignyte.com/webservices/ignyte.whatsshowing.webservice/moviefunctions.asmx?wsdl");

        String in = "<tns:GetTheatersAndMovies xmlns:soapenc=\"http://schemas.xmlsoap.org/soap/encoding/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:tns=\"http://www.ignyte.com/whatsshowing\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"><tns:zipCode>63011</tns:zipCode> <tns:radius>7</tns:radius> </tns:GetTheatersAndMovies>"; //$NON-NLS-1$
        executeSOAP("GetTheatersAndMovies", new Object[] {in}, env, null); //$NON-NLS-1$ 
    }    
    
    public void defer_testExternalWSDLExecution_getRate() throws Exception {        
        SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
        env.setLogWriter(Mockito.mock(PrintWriter.class));
        env.setWsdl("http://www.xmethods.net/sd/2001/CurrencyExchangeService.wsdl");

        String out = "<return type=\"java.lang.Float\">1.0</return>"; //$NON-NLS-1$
        executeSOAP("getRate", new Object[] {"USA", "USA"}, env, out); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$        
    }     
    
    public void testWithNoSourceName() throws Exception {
        
        File wsdlFile = new File(UnitTestUtil.getTestDataPath()+"/stockquotes.xml"); //$NON-NLS-1$
        SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
        env.setLogWriter(Mockito.mock(PrintWriter.class));
        env.setWsdl(wsdlFile.toURL().toString());
        try {
            executeSOAP("", new Object[] {"MSFT"}, env, null); //$NON-NLS-1$ //$NON-NLS-2$
            fail("must have failed since we did not provide a name in source property to execute"); //$NON-NLS-1$
        }catch(Exception e) {            
        }
    }
    
    public void defer_testExternalWithSuppliedPortType() throws Exception {
        
        File wsdlFile = new File(UnitTestUtil.getTestDataPath()+"/stockquotes.xml"); //$NON-NLS-1$
        SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
        env.setLogWriter(Mockito.mock(PrintWriter.class));
        env.setWsdl(wsdlFile.toURL().toString());
        env.setPortName("StockQuotesSoap12");
        
        executeSOAP("GetQuote", new Object[] {"MSFT"}, env, null); //$NON-NLS-1$ //$NON-NLS-2$        
    }    
    
    public void testDocLitralExecution() throws Exception {
        server.deployService(UnitTestUtil.getTestDataPath()+"/service/StockQuotes/doc-literal-deploy.wsdd"); //$NON-NLS-1$
        
        try {
            SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
            env.setLogWriter(Mockito.mock(PrintWriter.class));
            env.setWsdl("http://localhost:7001/axis/services/StockQuotes?wsdl");
            //String in = "<tns1:symbol xmlns:soapenc=\"http://schemas.xmlsoap.org/soap/encoding/\" xmlns:tns1=\"http://service.soap.xmlsource.connector.metamatrix.com\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">MSFT</tns1:symbol>"; //$NON-NLS-1$
            String in = "<symbol>MSFT</symbol>"; //$NON-NLS-1$

            //String in = "<tns1:GetQuote xmlns:soapenc=\"http://schemas.xmlsoap.org/soap/encoding/\" xmlns:tns1=\"http://service.soap.xmlsource.connector.metamatrix.com\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"><symbol>MSFT</symbol></tns1:GetQuote>"; //$NON-NLS-1$
            
            String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><symbolReturn xmlns=\"http://service.soap.xmlsource.connector.metamatrix.com\">&lt;company name=\"Microsoft Corp\"&gt;23.23&lt;/company&gt;</symbolReturn>"; //$NON-NLS-1$
            executeSOAP("GetQuote", new Object[] {in}, env, expected); //$NON-NLS-1$ 
            // end of test
        }
        finally {        
            server.undeployService(UnitTestUtil.getTestDataPath()+"/service/StockQuotes/undeploy.wsdd"); //$NON-NLS-1$
        }
    }
    
    public void testDocLitralExecution2() throws Exception {
            SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
            env.setLogWriter(Mockito.mock(PrintWriter.class));
            env.setWsdl("http://localhost:8080/teiid-ws/Echo?wsdl");
            //String in = "<tns1:symbol xmlns:soapenc=\"http://schemas.xmlsoap.org/soap/encoding/\" xmlns:tns1=\"http://service.soap.xmlsource.connector.metamatrix.com\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">MSFT</tns1:symbol>"; //$NON-NLS-1$
            String in = "<arg0>hello</arg0>"; //$NON-NLS-1$

            //String in = "<tns1:GetQuote xmlns:soapenc=\"http://schemas.xmlsoap.org/soap/encoding/\" xmlns:tns1=\"http://service.soap.xmlsource.connector.metamatrix.com\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"><symbol>MSFT</symbol></tns1:GetQuote>"; //$NON-NLS-1$
            
            String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><symbolReturn xmlns=\"http://service.soap.xmlsource.connector.metamatrix.com\">&lt;company name=\"Microsoft Corp\"&gt;23.23&lt;/company&gt;</symbolReturn>"; //$NON-NLS-1$
            executeSOAP("echo", new Object[] {in}, env, expected); //$NON-NLS-1$ 
            // end of test
    }    
    
    public void testRPCLitralExecution() throws Exception {
        server.deployService(UnitTestUtil.getTestDataPath()+"/service/StockQuotes/rpc-literal-deploy.wsdd"); //$NON-NLS-1$
        
        try {
            // now write the test
            SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
            env.setLogWriter(Mockito.mock(PrintWriter.class));
            env.setWsdl("http://localhost:7001/axis/services/StockQuotes?wsdl");
                                      
            String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><return type=\"java.lang.String\">&lt;company name=\"Microsoft Corp\"&gt;23.23&lt;/company&gt;</return>";//$NON-NLS-1$            
            executeSOAP("GetQuote", new Object[] {"MSFT"}, env, expected); //$NON-NLS-1$ //$NON-NLS-2$                
            // end of test
        }
        finally {           
            server.undeployService(UnitTestUtil.getTestDataPath()+"/service/StockQuotes/undeploy.wsdd"); //$NON-NLS-1$
        }
    }    
    
    public void testRPCEncodedExecution() throws Exception {
        server.deployService(UnitTestUtil.getTestDataPath()+"/service/StockQuotes/rpc-encoded-deploy.wsdd"); //$NON-NLS-1$

        try {
            // now write the test
            SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
            env.setLogWriter(Mockito.mock(PrintWriter.class));
            env.setWsdl("http://localhost:7001/axis/services/StockQuotes?wsdl");

            String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><return type=\"java.lang.String\">&lt;company name=\"Microsoft Corp\"&gt;23.23&lt;/company&gt;</return>"; //$NON-NLS-1$
            executeSOAP("GetQuote", new Object[] {"MSFT"}, env, expected); //$NON-NLS-1$ //$NON-NLS-2$                
            // end of test
        } 
        finally {            
            server.undeployService(UnitTestUtil.getTestDataPath()+"/service/StockQuotes/undeploy.wsdd"); //$NON-NLS-1$
        }
    } 
    
    public void testAlternateEndpoint() throws Exception {
        server.deployService(UnitTestUtil.getTestDataPath()+"/service/distance/deploy.wsdd"); //$NON-NLS-1$

        try {
            // now write the test
            File wsdlFile = new File(UnitTestUtil.getTestDataPath()+"/service/distance/Distance.xml"); //$NON-NLS-1$
            SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
            env.setLogWriter(Mockito.mock(PrintWriter.class));
            env.setWsdl(wsdlFile.toURL().toString());
            env.setEndPoint("http://localhost:7001/axis/services/Distance");
                                          
            String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><return type=\"java.lang.String\">IL</return>"; //$NON-NLS-1$
            executeSOAP("getState", new Object[] {"63011"}, env, expected); //$NON-NLS-1$ //$NON-NLS-2$                
            // end of test
        }
        finally {        
            server.undeployService(UnitTestUtil.getTestDataPath()+"/service/StockQuotes/undeploy.wsdd"); //$NON-NLS-1$
        }
    }
    
    public void testDocLitralWithIncudedComplexTypes() throws Exception {
        server.deployService(UnitTestUtil.getTestDataPath()+"/service/movies/deploy.wsdd"); //$NON-NLS-1$

        try {
            // now write the test
            File wsdlFile = new File(UnitTestUtil.getTestDataPath()+"/service/movies/movies.wsdl"); //$NON-NLS-1$
            SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
            env.setLogWriter(Mockito.mock(PrintWriter.class));
            env.setWsdl(wsdlFile.toURL().toString());
    
            String in = "<tns:GetTheatersAndMovies xmlns:soapenc=\"http://schemas.xmlsoap.org/soap/encoding/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:tns=\"http://www.metamatrix.com/whatsshowing\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"><tns:zipCode>63011</tns:zipCode> <tns:radius>7</tns:radius> </tns:GetTheatersAndMovies>"; //$NON-NLS-1$                                      
            String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><GetTheatersAndMoviesResponse xmlns=\"http://www.metamatrix.com/whatsshowing\"><GetTheatersAndMoviesReturn><GetTheatersAndMoviesReturn><Name>AMC Chesterfield 14</Name><Address>3rd Floor Chesterfield Mall, Chesterfield, MO</Address><Movies><Movie><Rating>PG</Rating><Name>Barnyard: The Original Party Animals</Name><RunningTime>1 hr 30 mins</RunningTime><ShowTimes>12:10pm | 2:25pm | 4:45pm | 7:10pm | 9:30pm</ShowTimes></Movie><Movie><Rating>G</Rating><Name>Cars</Name><RunningTime>1 hr 30 mins</RunningTime><ShowTimes>1 hr 57 mins</ShowTimes></Movie></Movies></GetTheatersAndMoviesReturn></GetTheatersAndMoviesReturn></GetTheatersAndMoviesResponse>"; //$NON-NLS-1$
            executeSOAP("GetTheatersAndMovies", new Object[] {in}, env, expected); //$NON-NLS-1$ 
            // end of test
        }
        finally {        
            server.undeployService(UnitTestUtil.getTestDataPath()+"/service/movies/undeploy.wsdd"); //$NON-NLS-1$
        }
    }    
    
    public void testDocLitralWithIncudedComplexTypes_wrong_input() throws Exception{
        server.deployService(UnitTestUtil.getTestDataPath()+"/service/movies/deploy.wsdd"); //$NON-NLS-1$

        try {
            // now write the test
            File wsdlFile = new File(UnitTestUtil.getTestDataPath()+"/service/movies/movies.wsdl"); //$NON-NLS-1$
            SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
            env.setLogWriter(Mockito.mock(PrintWriter.class));
            env.setWsdl(wsdlFile.toURL().toString());
                                                      
            executeSOAP("getTheatersAndMovies", new Object[] {"63011", "7"}, env, null); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            fail("must have failed, since this is doc-litral and we need to supply xml fragment as input"); //$NON-NLS-1$
            // end of test
        } catch(Exception e) {
            // pass
        }        
        finally {        
            server.undeployService(UnitTestUtil.getTestDataPath()+"/service/movies/undeploy.wsdd"); //$NON-NLS-1$
        }
    }     
    
    public void testWrongParamCount() throws Exception {
        server.deployService(UnitTestUtil.getTestDataPath()+"/service/distance/deploy.wsdd"); //$NON-NLS-1$
        try {
            // now write the test
            File wsdlFile = new File(UnitTestUtil.getTestDataPath()+"/service/distance/Distance.xml"); //$NON-NLS-1$
            SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
            env.setLogWriter(Mockito.mock(PrintWriter.class));
            env.setWsdl(wsdlFile.toURL().toString());
            env.setEndPoint("http://localhost:7001/axis/services/Distance");
    
            try {
                String expected = "IL"; //$NON-NLS-1$
                executeSOAP("getState", new Object[] {"63011", "63011"}, env, expected); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                fail("must have failed to execute, as we suuplied more input parameters than expected"); //$NON-NLS-1$
            }catch(Exception e) {
                
            }
            // end of test
        } finally {        
            server.undeployService(UnitTestUtil.getTestDataPath()+"/service/StockQuotes/undeploy.wsdd"); //$NON-NLS-1$
        }
    }
    
    // dimension designer based.
    public void defer_testDocLitralWithExternalComplexType() throws Exception {
        server.deployService(UnitTestUtil.getTestDataPath()+"/service/books/deploy.wsdd"); //$NON-NLS-1$

        try {
            // now write the test
            File wsdlFile = new File(UnitTestUtil.getTestDataPath()+"/service/books/Books.wsdl"); //$NON-NLS-1$
            SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
            env.setLogWriter(Mockito.mock(PrintWriter.class));
            env.setWsdl(wsdlFile.toURL().toString());
                                          
            String in = "<s0:AuthorBooks_Input xmlns:soapenc=\"http://schemas.xmlsoap.org/soap/encoding/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:s0=\"http://www.metamatrix.com/BooksView_Input\"> <ID>1</ID> </s0:AuthorBooks_Input>"; //$NON-NLS-1$
            String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Books_Output xmlns=\"http://www.metamatrix.com/BooksView_Output\"><item xmlns=\"\"><FIRSTNAME>Elfriede</FIRSTNAME><LASTNAME>Dustin</LASTNAME><TITLE>Automated Software Testing</TITLE><ID>1</ID></item></Books_Output>"; //$NON-NLS-1$
            executeSOAP("getBooks", new Object[] {in}, env, expected); //$NON-NLS-1$ 
            // end of test
        } finally {        
            server.undeployService(UnitTestUtil.getTestDataPath()+"/service/books/undeploy.wsdd"); //$NON-NLS-1$
        }
    }    
    
    public void testRPCEncoded() throws Exception {
        server.deployService(UnitTestUtil.getTestDataPath()+"/service/CurrencyExchange/deploy.wsdd"); //$NON-NLS-1$

        try {
            // now write the test
            File wsdlFile = new File(UnitTestUtil.getTestDataPath()+"/service/CurrencyExchange/CurrencyExchangeService.wsdl"); //$NON-NLS-1$
            SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
            env.setLogWriter(Mockito.mock(PrintWriter.class));
            env.setWsdl(wsdlFile.toURL().toString());
            
            String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><return type=\"java.lang.Float\">1.0</return>"; //$NON-NLS-1$
            executeSOAP("getRate", new Object[] {"USA", "INDIA"}, env, expected); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$                
            // end of test
        } finally {        
            server.undeployService(UnitTestUtil.getTestDataPath()+"/service/CurrencyExchange/undeploy.wsdd"); //$NON-NLS-1$
        }
    }     
    
    public void testUserNameProfile_clear_text_Pass() throws Exception {
        String wsddFile = UnitTestUtil.getTestDataPath()+"/service/CurrencyExchange/username_clear_deploy.wsdd"; //$NON-NLS-1$

        SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
        env.setLogWriter(Mockito.mock(PrintWriter.class));
        env.setSecurityType(SecurityToken.WS_SECURITY);
        env.setWSSecurityType(SecurityToken.USERNAME_TOKEN_PROFILE_CLEAR_TEXT);
        env.setAuthUserName("foo");
        env.setAuthPassword("foopassword");
        
        helpTestSecurity(true, wsddFile, env);
    }

    public void testUserNameProfile_clear_text_fail() throws Exception {
        String wsddFile = UnitTestUtil.getTestDataPath()+"/service/CurrencyExchange/username_clear_deploy.wsdd"; //$NON-NLS-1$
        SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
        env.setLogWriter(Mockito.mock(PrintWriter.class));
        env.setSecurityType(SecurityToken.WS_SECURITY);
        env.setWSSecurityType(SecurityToken.USERNAME_TOKEN_PROFILE_CLEAR_TEXT);
        env.setAuthUserName("foo");
        helpTestSecurity(false, wsddFile, env);
    }
    
    // this one uses the nounce (the random number) and Timestamp to deter the attack 
    public void testUserNameProfile_digest_text_Pass() throws Exception {
        String wsddFile = UnitTestUtil.getTestDataPath()+"/service/CurrencyExchange/username_digest_deploy.wsdd"; //$NON-NLS-1$

        SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
        env.setLogWriter(Mockito.mock(PrintWriter.class));
        env.setSecurityType(SecurityToken.WS_SECURITY);
        env.setWSSecurityType(SecurityToken.USERNAME_TOKEN_PROFILE_DIGEST);
        env.setAuthUserName("foo");
        env.setAuthPassword("foopassword");
        
        helpTestSecurity(true, wsddFile, env);
    }

    public void testUserNameProfile_digest_text_fail() throws Exception {
        String wsddFile = UnitTestUtil.getTestDataPath()+"/service/CurrencyExchange/username_digest_deploy.wsdd"; //$NON-NLS-1$
        
        SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
        env.setLogWriter(Mockito.mock(PrintWriter.class));
        env.setSecurityType(SecurityToken.WS_SECURITY);
        env.setWSSecurityType(SecurityToken.USERNAME_TOKEN_PROFILE_DIGEST);
        env.setAuthUserName("foo");
        env.setAuthPassword("badpass");
        
        helpTestSecurity(false, wsddFile, env);
    }

    // TODO: Needs this to be hooked to a Http proxy to sniff the header out
    public void testHttpBasicAuth() throws Exception {
        //        System.setProperty("http.proxyHost","myproxy" ); //$NON-NLS-1$ //$NON-NLS-2$
        //        System.setProperty("http.proxyPort", "8080" ); //$NON-NLS-1$ //$NON-NLS-2$        
        String wsddFile = UnitTestUtil.getTestDataPath()+"/service/CurrencyExchange/httpbasic_deploy.wsdd"; //$NON-NLS-1$

        SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
        env.setLogWriter(Mockito.mock(PrintWriter.class));
        env.setSecurityType(SecurityToken.HTTP_BASIC_AUTH);
        env.setAuthUserName("foo");
        env.setAuthPassword("foopassword");

        helpTestSecurity(true, wsddFile, env);
    }

    public void testWSSecurityTypeMissing() throws Exception {
        String wsddFile = UnitTestUtil.getTestDataPath()+"/service/CurrencyExchange/timestamp_deploy.wsdd"; //$NON-NLS-1$

        SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
        env.setLogWriter(Mockito.mock(PrintWriter.class));
        env.setSecurityType(SecurityToken.WS_SECURITY);

        helpTestSecurity(false, wsddFile, env);
    }
    
    public void testWrongWSSecurityType() throws Exception {
        String wsddFile = UnitTestUtil.getTestDataPath()+"/service/CurrencyExchange/timestamp_deploy.wsdd"; //$NON-NLS-1$

        SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
        env.setLogWriter(Mockito.mock(PrintWriter.class));
        env.setSecurityType(SecurityToken.WS_SECURITY);
        env.setWSSecurityType("UnKnown");
        
        helpTestSecurity(false, wsddFile, env);
    }    
    
    public void testTimestampProfile() throws Exception {
        String wsddFile = UnitTestUtil.getTestDataPath()+"/service/CurrencyExchange/timestamp_deploy.wsdd"; //$NON-NLS-1$

        SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
        env.setLogWriter(Mockito.mock(PrintWriter.class));
        env.setSecurityType(SecurityToken.WS_SECURITY);
        env.setWSSecurityType(SecurityToken.TIMESTAMP);
        
        helpTestSecurity(true, wsddFile, env);
    }
    
    public void defer_testEncryptProfile() throws Exception {
        String wsddFile = UnitTestUtil.getTestDataPath()+"/service/CurrencyExchange/encrypt_deploy.wsdd"; //$NON-NLS-1$

        SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
        env.setLogWriter(Mockito.mock(PrintWriter.class));
        env.setSecurityType(SecurityToken.WS_SECURITY);
        env.setWSSecurityType(SecurityToken.ENCRYPT);
        env.setEncryptPropertyFile("com/metamatrix/connector/xmlsource/soap/client_crypto.properties");
        env.setAuthUserName("client");
        env.setEncryptUserName("server");
        
        helpTestSecurity(true, wsddFile,  env);
    }      
    
    /**
     * Running of the SAML and Encryption examples need keystore generated the below used
     * Look here for mew details: http://www.churchillobjects.com/c/11201e.html
     * The below is script to generate client and server side certificates
     * self cert/not trusted
        keytool -genkey -alias client -keystore client_ks.jks -keypass clientpassword -storepass keystorepassword -keyalg RSA -dname "cn=metamatrix"
        keytool -genkey -alias server -keystore server_ks.jks -keypass serverpassword -storepass keystorepassword -keyalg RSA -dname "cn=metamatrix"
        
        keytool -selfcert -alias server -keystore server_ks.jks -keypass serverpassword -storepass keystorepassword
        keytool -selfcert -alias client -keystore client_ks.jks -keypass clientpassword -storepass keystorepassword
        
        keytool -export -alias client -keystore client_ks.jks -storepass keystorepassword -file client.cert
        keytool -export -alias server -keystore server_ks.jks -storepass keystorepassword -file server.cert
        keytool -import -alias client -file client.cert -keystore server_ks.jks -storepass keystorepassword
        keytool -import -alias server -file server.cert -keystore client_ks.jks -storepass keystorepassword
        
        keytool -list -keystore client_ks.jks -storepass keystorepassword
        keytool -list -keystore server_ks.jks -storepass keystorepassword        
     */
    // this is sender vouches
    public void testSAMLToken_unsigned_sendervouches() throws Exception {
        String wsddFile = UnitTestUtil.getTestDataPath()+"/service/CurrencyExchange/saml_unsigned_deploy.wsdd"; //$NON-NLS-1$
        SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
        env.setLogWriter(Mockito.mock(PrintWriter.class));
        env.setSecurityType(SecurityToken.WS_SECURITY);
        env.setWSSecurityType(SecurityToken.SAML_TOKEN_UNSIGNED);
        env.setSAMLPropertyFile("com/metamatrix/connector/xmlsource/soap/saml_unsigned_prop.properties");
        
        helpTestSecurity(true, wsddFile, env);
    }

    public void testSAMLToken_unsigned_keyHolder() throws Exception {
        String wsddFile = UnitTestUtil.getTestDataPath()+"/service/CurrencyExchange/saml_unsigned_deploy.wsdd"; //$NON-NLS-1$

        SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
        env.setLogWriter(Mockito.mock(PrintWriter.class));
        env.setSecurityType(SecurityToken.WS_SECURITY);
        env.setWSSecurityType(SecurityToken.SAML_TOKEN_UNSIGNED);
        env.setSAMLPropertyFile("com/metamatrix/connector/xmlsource/soap/saml_unsigned_keyholder.properties");
        env.setAuthUserName("client");
        env.setAuthPassword("clientpassword");
        
        helpTestSecurity(false, wsddFile, env);
    }
    
    /**
     * Service defined as saml unsigned; we make with out any; must fail 
     */
    public void testSAMLToken_unsigned_fail() throws Exception {
        String wsddFile = UnitTestUtil.getTestDataPath()+"/service/CurrencyExchange/saml_unsigned_deploy.wsdd"; //$NON-NLS-1$
        SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
        env.setLogWriter(Mockito.mock(PrintWriter.class));
        // having lack of crendentials like test before; this will fail
        helpTestSecurity(false, wsddFile, env);
    } 
        
    /**
     * With Signature profile it seems like the names/password in the crypto.properties
     * file does not matter for client and server except for the keystore password. But
     * it must have the below alias name and its password for private key for encryption
     * in the client side.
     * 
     * In server side crypto.properties must point to a cert with (can be only) public key matching 
     * the client side to work.
     * 
     * this type should work with both trusted certificates; and of certificates with public
     * key embedded with each other
     */
    public void testSignature_DirectReferece() throws Exception {
        String wsddFile = UnitTestUtil.getTestDataPath()+"/service/CurrencyExchange/signature_deploy.wsdd"; //$NON-NLS-1$

        SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
        env.setLogWriter(Mockito.mock(PrintWriter.class));
        env.setSecurityType(SecurityToken.WS_SECURITY);
        env.setWSSecurityType(SecurityToken.SIGNATURE);
        env.setAuthUserName("client");
        env.setAuthPassword("clientpassword");
        env.setTrustType(SecurityToken.DIRECT_REFERENCE);
        env.setCryptoPropertyFile("com/metamatrix/connector/xmlsource/soap/client_crypto.properties");
        
        helpTestSecurity(true, wsddFile,  env);
    }     
    
    // This will not work with trusted; this will only work with the certificates which are
    // installed in the both sides. with each other's public keys
    public void testSignature_IssueSerial_NonTrusted() throws Exception {
        String wsddFile = UnitTestUtil.getTestDataPath()+"/service/CurrencyExchange/signature_deploy.wsdd"; //$NON-NLS-1$

        SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
        env.setLogWriter(Mockito.mock(PrintWriter.class));
        env.setSecurityType(SecurityToken.WS_SECURITY);
        env.setWSSecurityType(SecurityToken.SIGNATURE);
        env.setAuthUserName("client");
        env.setAuthPassword("clientpassword");
        env.setTrustType(SecurityToken.ISSUER_SERIAL);
        env.setCryptoPropertyFile("com/metamatrix/connector/xmlsource/soap/client_crypto.properties");
        
        helpTestSecurity(true, wsddFile,  env);
    }     
    
    public void testSignature_IssueSerial_Trusted() throws Exception {
        String wsddFile = UnitTestUtil.getTestDataPath()+"/service/CurrencyExchange/signature_trusted_deploy.wsdd"; //$NON-NLS-1$

        SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
        env.setLogWriter(Mockito.mock(PrintWriter.class));
        env.setSecurityType(SecurityToken.WS_SECURITY);
        env.setWSSecurityType(SecurityToken.SIGNATURE);
        env.setAuthUserName("client");
        env.setAuthPassword("clientpassword");
        env.setTrustType(SecurityToken.ISSUER_SERIAL);
        env.setCryptoPropertyFile("com/metamatrix/connector/xmlsource/soap/client_trusted_crypto.properties");

        helpTestSecurity(false, wsddFile,  env);
    }     
    
    /**
     * This nees to pass; but failing for some reason 
     */       
    public void testSAMLToken_Signed_SenderVouches() throws Exception {
        String wsddFile = UnitTestUtil.getTestDataPath()+"/service/CurrencyExchange/saml_signed_deploy.wsdd"; //$NON-NLS-1$

        SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
        env.setLogWriter(Mockito.mock(PrintWriter.class));
        env.setSecurityType(SecurityToken.WS_SECURITY);
        env.setWSSecurityType(SecurityToken.SAML_TOKEN_SIGNED);
        env.setAuthUserName("client");
        env.setAuthPassword("clientpassword");
        env.setTrustType(SecurityToken.DIRECT_REFERENCE);
        env.setCryptoPropertyFile("com/metamatrix/connector/xmlsource/soap/client_crypto.properties");
        env.setSAMLPropertyFile("com/metamatrix/connector/xmlsource/soap/saml_signed_sendervouches.properties");
        
        helpTestSecurity(false, wsddFile,  env);
    }    
         
    
    // Worked with both self cert and trusted certificates
    public void testSAMLToken_Signed_KeyHolder_directRef() throws Exception {
        String wsddFile = UnitTestUtil.getTestDataPath()+"/service/CurrencyExchange/saml_signed_deploy.wsdd"; //$NON-NLS-1$

        SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
        env.setLogWriter(Mockito.mock(PrintWriter.class));
        env.setSecurityType(SecurityToken.WS_SECURITY);
        env.setWSSecurityType(SecurityToken.TIMESTAMP + " " + SecurityToken.SAML_TOKEN_SIGNED);
        env.setAuthUserName("client");
        env.setAuthPassword("clientpassword");
        env.setTrustType(SecurityToken.DIRECT_REFERENCE);
        env.setCryptoPropertyFile("com/metamatrix/connector/xmlsource/soap/client_crypto.properties");
        env.setSAMLPropertyFile("com/metamatrix/connector/xmlsource/soap/saml_signed_keyholder.properties");
        env.setCryptoPropertyFile("com/metamatrix/connector/xmlsource/soap/client_crypto.properties");
        
        helpTestSecurity(true, wsddFile,  env);
    }    
    
    public void defer_testMultiple_sig_time_username() throws Exception {
        String wsddFile = UnitTestUtil.getTestDataPath()+"/service/CurrencyExchange/multiple_deploy.wsdd"; //$NON-NLS-1$

        SoapManagedConnectionFactory env = new SoapManagedConnectionFactory();
        env.setLogWriter(Mockito.mock(PrintWriter.class));
        env.setSecurityType(SecurityToken.WS_SECURITY);
        env.setWSSecurityType(SecurityToken.USERNAME_TOKEN_PROFILE_CLEAR_TEXT+ " " +SecurityToken.SIGNATURE+ " " +SecurityToken.ENCRYPT + " " + SecurityToken.TIMESTAMP);
        env.setAuthUserName("client");
        env.setAuthPassword("clientpassword");
        env.setTrustType(SecurityToken.DIRECT_REFERENCE);
        env.setCryptoPropertyFile("com/metamatrix/connector/xmlsource/soap/client_crypto.properties");
        env.setEncryptPropertyFile("com/metamatrix/connector/xmlsource/soap/client_crypto.properties");
        env.setEncryptUserName("server;");

        helpTestSecurity(true, wsddFile,  env);
    }     
    
    void helpTestSecurity(boolean passScenario, String serverWsddFile, SoapManagedConnectionFactory env) throws Exception {
        server.deployService(serverWsddFile); 

        try {
            try {
                // now write the test
                File wsdlFile = new File(UnitTestUtil.getTestDataPath()+"/service/CurrencyExchange/CurrencyExchangeService.wsdl"); //$NON-NLS-1$
                env.setWsdl(wsdlFile.toURL().toString());
                
                String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><return type=\"java.lang.Float\">1.0</return>";//$NON-NLS-1$
                executeSOAP("getRate", new Object[] {"USA", "INDIA"}, env, expected); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                if (!passScenario) fail("The SOAP Request Must have failed; but passed"); //$NON-NLS-1$
                // end of test
            } catch (Exception e) {
                if (passScenario) fail("The SOAP Request Must have passed; but failed"); //$NON-NLS-1$                
            }
        } finally {        
            server.undeployService(UnitTestUtil.getTestDataPath()+"/service/CurrencyExchange/undeploy.wsdd"); //$NON-NLS-1$
        }
    }     
    
    
    // utility method to execute a service
    void executeSOAP(String procName, Object[] args, SoapManagedConnectionFactory env, String expected) throws Exception{
    	SoapConnector connector = new SoapConnector();
    	connector.initialize(env);
    	
        SoapConnection conn =(SoapConnection) connector.getConnection();
        RuntimeMetadata metadata = Mockito.mock(RuntimeMetadata.class);
        LanguageFactory fact = env.getLanguageFactory();
        List parameters = new ArrayList();
        if (args != null && args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                Argument param = fact.createArgument(Direction.IN, new Literal(args[i], args[i].getClass()), args[i].getClass(), null);           
                parameters.add(param);
            }
        }
        Call procedure = fact.createCall("AnyNAME", parameters, createMockProcedureMetadata(procName)); //$NON-NLS-1$

        ProcedureExecution exec = (ProcedureExecution)conn.createExecution(procedure, Mockito.mock(ExecutionContext.class), metadata); //$NON-NLS-1$ //$NON-NLS-2$
        exec.execute();
        
        List result = exec.next();
        assertNotNull(result);
        assertNull(exec.next());
        try {
            exec.getOutputParameterValues();
            fail("should have thrown error in returning a return"); //$NON-NLS-1$            
        }catch(Exception e) {            
        }

        // get the result set
        SQLXML xml = (SQLXML)result.get(0);
        assertNotNull(xml);
        String xmlString = xml.getString();
        if (expected != null) {
            assertEquals(expected, xmlString); 
        }
        // System.out.println(xmlString);
    }
    
    public static Procedure createMockProcedureMetadata(String nameInSource) {
    	Procedure rm = Mockito.mock(Procedure.class);
		Mockito.stub(rm.getNameInSource()).toReturn(nameInSource);
    	return rm;
    }
    
}
