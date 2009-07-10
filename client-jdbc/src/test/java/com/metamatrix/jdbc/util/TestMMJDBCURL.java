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

package com.metamatrix.jdbc.util;

import java.net.URLEncoder;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.common.api.MMURL;
import com.metamatrix.jdbc.BaseDataSource;
import com.metamatrix.jdbc.api.ConnectionProperties;
import com.metamatrix.jdbc.api.ExecutionProperties;


/** 
 * @since 4.3
 */
public class TestMMJDBCURL extends TestCase {

    // Need to allow embedded spaces and ='s within optional properties
    public final void testCredentials() throws Exception {
        String credentials = URLEncoder.encode("defaultToLogon,(system=BQT1 SQL Server 2000 Simple Cap,user=xyz,password=xyz)", "UTF-8"); //$NON-NLS-1$ //$NON-NLS-2$
        MMJDBCURL url = new MMJDBCURL("jdbc:metamatrix:QT_sqls2kds@mm://slwxp136:43100;credentials="+credentials); //$NON-NLS-1$
        Properties p = url.getProperties();
        assertEquals("defaultToLogon,(system=BQT1 SQL Server 2000 Simple Cap,user=xyz,password=xyz)", p.getProperty("credentials"));  //$NON-NLS-1$//$NON-NLS-2$        
    }
       
    public void testJDBCURLWithProperties() {
        String URL = "jdbc:metamatrix:bqt@mm://localhost:12345;version=1;user=xyz;password=***;logLevel=1;configFile=testdata/bqt/dqp_stmt_e2e.xmi;disableLocalTxn=true;autoFailover=false"; //$NON-NLS-1$
        
        Properties expectedProperties = new Properties();
        expectedProperties.setProperty("version", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("user", "xyz"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("password", "***"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("logLevel", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("configFile", "testdata/bqt/dqp_stmt_e2e.xmi"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty(ExecutionProperties.DISABLE_LOCAL_TRANSACTIONS, "true"); //$NON-NLS-1$
        expectedProperties.setProperty(MMURL.CONNECTION.AUTO_FAILOVER, "false"); //$NON-NLS-1$
        MMJDBCURL url = new MMJDBCURL(URL); 
        assertEquals("bqt", url.getVDBName()); //$NON-NLS-1$
        assertEquals("mm://localhost:12345", url.getConnectionURL()); //$NON-NLS-1$
        assertEquals(expectedProperties, url.getProperties());
    }
    
    public void testJDBCURLWithoutProperties() {
        String URL = "jdbc:metamatrix:bqt@mm://localhost:12345"; //$NON-NLS-1$
        
        MMJDBCURL url = new MMJDBCURL(URL); 
        assertEquals("bqt", url.getVDBName()); //$NON-NLS-1$
        assertEquals("mm://localhost:12345", url.getConnectionURL()); //$NON-NLS-1$
        assertEquals(new Properties(), url.getProperties());
    }
    
    public void testCaseConversion() {
        // Different case ------------------------------------HERE -v  ----------------and HERE  -v
        String URL = "jdbc:metamatrix:bqt@mm://localhost:12345;VERSION=1;user=xyz;password=***;configFile=testdata/bqt/dqp_stmt_e2e.xmi"; //$NON-NLS-1$
        
        Properties expectedProperties = new Properties();
        expectedProperties.setProperty("version", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("user", "xyz"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("password", "***"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("configFile", "testdata/bqt/dqp_stmt_e2e.xmi"); //$NON-NLS-1$ //$NON-NLS-2$
        MMJDBCURL url = new MMJDBCURL(URL); 
        assertEquals("bqt", url.getVDBName()); //$NON-NLS-1$
        assertEquals("mm://localhost:12345", url.getConnectionURL()); //$NON-NLS-1$
        assertEquals(expectedProperties, url.getProperties());
    }
    
    public void testWithExtraSemicolons() {
        String URL = "jdbc:metamatrix:bqt@mm://localhost:12345;version=1;user=xyz;password=***;logLevel=1;;;configFile=testdata/bqt/dqp_stmt_e2e.xmi;;"; //$NON-NLS-1$
        
        Properties expectedProperties = new Properties();
        expectedProperties.setProperty("version", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("user", "xyz"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("password", "***"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("logLevel", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("configFile", "testdata/bqt/dqp_stmt_e2e.xmi"); //$NON-NLS-1$ //$NON-NLS-2$
        MMJDBCURL url = new MMJDBCURL(URL); 
        assertEquals("bqt", url.getVDBName()); //$NON-NLS-1$
        assertEquals("mm://localhost:12345", url.getConnectionURL()); //$NON-NLS-1$
        assertEquals(expectedProperties, url.getProperties());
    }
    
    public void testWithWhitespace() {
        String URL = "jdbc:metamatrix:bqt@mm://localhost:12345; version =1;user= xyz ;password=***; logLevel = 1 ; configFile=testdata/bqt/dqp_stmt_e2e.xmi ;"; //$NON-NLS-1$
        
        Properties expectedProperties = new Properties();
        expectedProperties.setProperty("version", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("user", "xyz"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("password", "***"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("logLevel", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("configFile", "testdata/bqt/dqp_stmt_e2e.xmi"); //$NON-NLS-1$ //$NON-NLS-2$
        MMJDBCURL url = new MMJDBCURL(URL); 
        assertEquals("bqt", url.getVDBName()); //$NON-NLS-1$
        assertEquals("mm://localhost:12345", url.getConnectionURL()); //$NON-NLS-1$
        assertEquals(expectedProperties, url.getProperties());
    }
    
    public void testNoPropertyValue() {
        String URL = "jdbc:metamatrix:bqt@mm://localhost:12345;version=1;user=xyz;password=***;logLevel=;configFile="; //$NON-NLS-1$
        
        Properties expectedProperties = new Properties();
        expectedProperties.setProperty("version", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("user", "xyz"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("password", "***"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("logLevel", ""); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("configFile", ""); //$NON-NLS-1$ //$NON-NLS-2$
        MMJDBCURL url = new MMJDBCURL(URL); 
        assertEquals("bqt", url.getVDBName()); //$NON-NLS-1$
        assertEquals("mm://localhost:12345", url.getConnectionURL()); //$NON-NLS-1$
        assertEquals(expectedProperties, url.getProperties());
    }
    
    public void testInvalidProtocol() {
        String URL = "jdbc:monkeymatrix:bqt@mm://localhost:12345;version=1;user=xyz;password=***;logLevel=1"; //$NON-NLS-1$
        try {
            new MMJDBCURL(URL);
            fail("Illegal argument should have failed."); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
    
    public void testNoVDBName() {
        String URL = "jdbc:metamatrix:@mm://localhost:12345;version=1;user=xyz;password=***;logLevel=1"; //$NON-NLS-1$
        try {
            new MMJDBCURL(URL);
            fail("Illegal argument should have failed."); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // expected
        }        
    }
    
    public void testNoAtSignInURL() {
        String URL = "jdbc:metamatrix:bqt!mm://localhost:12345;version=1;user=xyz;password=***;logLevel=1"; //$NON-NLS-1$
        try {
            new MMJDBCURL(URL);
            // No @ sign is llowed as part of embedded driver now, 
            // but this form of URL rejected in the acceptURL
            //fail("Illegal argument should have failed.");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
    
    public void testMoreThanOneAtSign() {
        String URL = "jdbc:metamatrix:bqt@mm://localhost:12345;version=1;user=xy@;password=***;logLevel=1"; //$NON-NLS-1$
        try {
            // this allowed as customer properties can have @ in their properties
            new MMJDBCURL(URL);            
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
    
    public void testNoEqualsInProperty() {
        String URL = "jdbc:metamatrix:bqt@mm://localhost:12345;version=1;user=xyz;password***;logLevel=1"; //$NON-NLS-1$
        try {
            new MMJDBCURL(URL);
            fail("Illegal argument should have failed."); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
    
    public void testMoreThanOneEqualsInProperty() {
        String URL = "jdbc:metamatrix:bqt@mm://localhost:12345;version=1;user=xyz;password==***;logLevel=1"; //$NON-NLS-1$
        try {
            new MMJDBCURL(URL);
            fail("Illegal argument should have failed."); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // expected
        }
        URL = "jdbc:metamatrix:bqt@mm://localhost:12345;version=1;user=xyz;password=***=;logLevel=1"; //$NON-NLS-1$
        try {
            new MMJDBCURL(URL);
            fail("Illegal argument should have failed."); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // expected
        }
        URL = "jdbc:metamatrix:bqt@mm://localhost:12345;version=1;user=xyz;=password=***;logLevel=1"; //$NON-NLS-1$
        try {
            new MMJDBCURL(URL);
            fail("Illegal argument should have failed."); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
    
    public void testNoKeyInProperty() {
        String URL = "jdbc:metamatrix:bqt@mm://localhost:12345;version=1;user=xyz;=***;logLevel=1"; //$NON-NLS-1$
        try {
            new MMJDBCURL(URL);
            fail("Illegal argument should have failed."); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
    
    public void testConstructor() {
        MMJDBCURL url = new MMJDBCURL("myVDB", "mm://myhost:12345",null); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("jdbc:teiid:myVDB@mm://myhost:12345", url.getJDBCURL()); //$NON-NLS-1$
        
        Properties props = new Properties();
        props.setProperty(BaseDataSource.USER_NAME, "myuser"); //$NON-NLS-1$
        props.setProperty(BaseDataSource.PASSWORD, "mypassword"); //$NON-NLS-1$
        props.put("ClieNTtOKeN", new Integer(1)); //$NON-NLS-1$
        url = new MMJDBCURL("myVDB", "mm://myhost:12345", props); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("jdbc:teiid:myVDB@mm://myhost:12345;user=myuser;password=mypassword", url.getJDBCURL()); //$NON-NLS-1$
    }
    
    public void testConstructor_Exception() {
        try {
            new MMJDBCURL(null, "myhost", null); //$NON-NLS-1$
            fail("Should have failed."); //$NON-NLS-1$
        } catch (Exception e) {
            
        }
        try {
            new MMJDBCURL("  ", "myhost", null); //$NON-NLS-1$ //$NON-NLS-2$
            fail("Should have failed."); //$NON-NLS-1$
        } catch (Exception e) {
            
        }
        try {
            new MMJDBCURL("myVDB", null, null); //$NON-NLS-1$
            fail("Should have failed."); //$NON-NLS-1$
        } catch (Exception e) {
            
        }
        try {
            new MMJDBCURL("myVDB", "  ", null); //$NON-NLS-1$ //$NON-NLS-2$
            fail("Should have failed."); //$NON-NLS-1$
        } catch (Exception e) {
            
        }
    }
    
    public void testNormalize() {
        Properties props = new Properties();
        props.setProperty("UsEr", "myuser"); //$NON-NLS-1$ //$NON-NLS-2$
        props.setProperty("pAssWOrD", "mypassword"); //$NON-NLS-1$ //$NON-NLS-2$
        props.put("ClieNTtOKeN", new Integer(1)); //$NON-NLS-1$
        MMJDBCURL.normalizeProperties(props);
        assertEquals("myuser", props.getProperty(BaseDataSource.USER_NAME)); //$NON-NLS-1$
        assertEquals("mypassword", props.getProperty(BaseDataSource.PASSWORD)); //$NON-NLS-1$
        assertEquals(new Integer(1), props.get(ConnectionProperties.PROP_CLIENT_SESSION_PAYLOAD));
    }
    
    public final void testEncodedPropertyProperties() throws Exception {
        String password = "=@#^&*()+!%$^%@#_-)_~{}||\\`':;,./<>?password has = & %"; //$NON-NLS-1$
        Properties props = new Properties();
        props.setProperty("UsEr", "foo"); //$NON-NLS-1$ //$NON-NLS-2$
        props.setProperty("PASswoRd", password); //$NON-NLS-1$
        MMJDBCURL.normalizeProperties(props);
        
        assertEquals(password, props.getProperty("password"));  //$NON-NLS-1$
        assertEquals("foo", props.getProperty("user"));  //$NON-NLS-1$//$NON-NLS-2$
    }    
   
    public final void testEncodedPropertyInURL() throws Exception {
        String password = "=@#^&*()+!%$^%@#_-)_~{}||\\`':;,./<>?password has = & %"; //$NON-NLS-1$
        String encPassword = URLEncoder.encode(password, "UTF-8"); //$NON-NLS-1$
        MMJDBCURL url = new MMJDBCURL("jdbc:metamatrix:QT_sqls2kds@mm://slwxp136:43100;PASswoRd="+encPassword); //$NON-NLS-1$
        Properties p = url.getProperties();
        assertEquals(password, p.getProperty("password"));  //$NON-NLS-1$
    }   
    
    
    public void testGetServerURL_NoProperties() {        
        String result = new MMJDBCURL("jdbc:metamatrix:designtimecatalog@mm://slwxp172:44401;user=ddifranco;password=mm").getConnectionURL(); //$NON-NLS-1$
        assertEquals("mm://slwxp172:44401", result);         //$NON-NLS-1$
    }

    public void testGetServerURL_Properties() {        
        String result = new MMJDBCURL("jdbc:metamatrix:designtimecatalog@mm://slwxp172:44401;user=ddifranco;password=mm").getConnectionURL(); //$NON-NLS-1$
        assertEquals("mm://slwxp172:44401", result);         //$NON-NLS-1$
    }
    
    /**
     * Test getServerURL with a valid URL and password that contains at least 
     * one ASCII character in the range of 32 to 126 excluding the ; and = sign.
     *
     * @since 5.0.2
     */
    public void testGetServerURL_PasswordProperties() throws Exception {        
        String result = null;
        String srcURL = "jdbc:metamatrix:designtimecatalog@mm://slwxp172:44401;user=ddifranco;password="; //$NON-NLS-1$
        String password = null;
        String tgtURL = "mm://slwxp172:44401"; //$NON-NLS-1$
        

        for ( char ch = 32; ch <= 126; ch++ ) {
            //exclude URL reserved characters
        	if ( ch != ';' && ch != '=' && ch != '%') {
        		password = ch+"mm"; //$NON-NLS-1$
        		result = new MMJDBCURL(srcURL+URLEncoder.encode(password, "UTF-8")).getConnectionURL(); //$NON-NLS-1$ 
        		assertEquals("Failed to obtain correct ServerURL when using password "+password,tgtURL, result);         //$NON-NLS-1$
        	}
        }
        	
    }
    
    public void testGetServerURL_2Servers() {       
        String result = new MMJDBCURL("jdbc:metamatrix:designtimecatalog@mm://slwxp172:44401,slabc123:12345;user=ddifranco;password=mm").getConnectionURL(); //$NON-NLS-1$
        assertEquals("mm://slwxp172:44401,slabc123:12345", result);         //$NON-NLS-1$
    }

    public void testBuildEmbeedURL() {
    	MMJDBCURL url = new MMJDBCURL("vdb", "/home/foo/deploy.properties", new Properties()); //$NON-NLS-1$ //$NON-NLS-2$
    	assertEquals("jdbc:teiid:vdb@/home/foo/deploy.properties", url.getJDBCURL()); //$NON-NLS-1$
    	
    	Properties p = new Properties();
    	p.setProperty("user", "test"); //$NON-NLS-1$ //$NON-NLS-2$
    	p.setProperty("password", "pass"); //$NON-NLS-1$ //$NON-NLS-2$
    	p.setProperty("autoFailover", "true"); //$NON-NLS-1$ //$NON-NLS-2$
    	p.setProperty("any", "thing"); //$NON-NLS-1$ //$NON-NLS-2$
    	
    	url = new MMJDBCURL("vdb", "/home/foo/deploy.properties", p); //$NON-NLS-1$ //$NON-NLS-2$
    	assertTrue(url.getJDBCURL().startsWith("jdbc:teiid:vdb@/home/foo/deploy.properties;user=test;")); //$NON-NLS-1$
    	assertTrue(url.getJDBCURL().indexOf("any=thing")!=-1); //$NON-NLS-1$
    	assertTrue(url.getJDBCURL().indexOf("password=pass")!=-1); //$NON-NLS-1$
    	assertTrue(url.getJDBCURL().indexOf("autoFailover=true")!=-1); //$NON-NLS-1$
    	
    }
}
