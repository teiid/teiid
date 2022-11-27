/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.jdbc;

import static org.junit.Assert.*;

import java.net.URLEncoder;
import java.util.Properties;

import org.junit.Test;
import org.teiid.net.TeiidURL;



/**
 * @since 4.3
 */
public class TestJDBCURL {

    // Need to allow embedded spaces and ='s within optional properties
    @Test public final void testCredentials() throws Exception {
        String credentials = URLEncoder.encode("defaultToLogon,(system=BQT1 SQL Server 2000 Simple Cap,user=xyz,password=xyz)", "UTF-8"); //$NON-NLS-1$ //$NON-NLS-2$
        JDBCURL url = new JDBCURL("jdbc:teiid:QT_sqls2kds@mm://slwxp136:43100;credentials="+credentials); //$NON-NLS-1$
        Properties p = url.getProperties();
        assertEquals("defaultToLogon,(system=BQT1 SQL Server 2000 Simple Cap,user=xyz,password=xyz)", p.getProperty("credentials"));  //$NON-NLS-1$//$NON-NLS-2$
    }

    @Test public void testJDBCURLWithProperties() {
        String URL = "jdbc:teiid:bqt@mm://localhost:12345;version=1;user=%25xyz;password=***;logLevel=1;configFile=testdata/bqt/dqp_stmt_e2e.xmi;disableLocalTxn=true;autoFailover=false"; //$NON-NLS-1$

        Properties expectedProperties = new Properties();
        expectedProperties.setProperty("version", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("user", "%xyz"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("password", "***"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("logLevel", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("configFile", "testdata/bqt/dqp_stmt_e2e.xmi"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty(ExecutionProperties.DISABLE_LOCAL_TRANSACTIONS, "true"); //$NON-NLS-1$
        expectedProperties.setProperty(TeiidURL.CONNECTION.AUTO_FAILOVER, "false"); //$NON-NLS-1$
        JDBCURL url = new JDBCURL(URL);
        assertEquals("bqt", url.getVDBName()); //$NON-NLS-1$
        assertEquals("mm://localhost:12345", url.getConnectionURL()); //$NON-NLS-1$
        assertEquals(expectedProperties, url.getProperties());
        assertTrue(url.getJDBCURL().contains("user=%25xyz"));
    }

    @Test public void testJDBCURLWithoutProperties() {
        String URL = "jdbc:teiid:bqt@mm://localhost:12345"; //$NON-NLS-1$

        JDBCURL url = new JDBCURL(URL);
        assertEquals("bqt", url.getVDBName()); //$NON-NLS-1$
        assertEquals("mm://localhost:12345", url.getConnectionURL()); //$NON-NLS-1$
        assertEquals(new Properties(), url.getProperties());
    }

    @Test public void testCaseConversion() {
        // Different case ------------------------------------HERE -v  ----------------and HERE  -v
        String URL = "jdbc:teiid:bqt@mm://localhost:12345;VERSION=1;user=xyz;password=***;configFile=testdata/bqt/dqp_stmt_e2e.xmi"; //$NON-NLS-1$

        Properties expectedProperties = new Properties();
        expectedProperties.setProperty("version", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("user", "xyz"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("password", "***"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("configFile", "testdata/bqt/dqp_stmt_e2e.xmi"); //$NON-NLS-1$ //$NON-NLS-2$
        JDBCURL url = new JDBCURL(URL);
        assertEquals("bqt", url.getVDBName()); //$NON-NLS-1$
        assertEquals("mm://localhost:12345", url.getConnectionURL()); //$NON-NLS-1$
        assertEquals(expectedProperties, url.getProperties());
    }

    @Test public void testWithExtraSemicolons() {
        String URL = "jdbc:teiid:bqt@mm://localhost:12345;version=1;user=xyz;password=***;logLevel=1;;;configFile=testdata/bqt/dqp_stmt_e2e.xmi;;"; //$NON-NLS-1$

        Properties expectedProperties = new Properties();
        expectedProperties.setProperty("version", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("user", "xyz"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("password", "***"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("logLevel", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("configFile", "testdata/bqt/dqp_stmt_e2e.xmi"); //$NON-NLS-1$ //$NON-NLS-2$
        JDBCURL url = new JDBCURL(URL);
        assertEquals("bqt", url.getVDBName()); //$NON-NLS-1$
        assertEquals("mm://localhost:12345", url.getConnectionURL()); //$NON-NLS-1$
        assertEquals(expectedProperties, url.getProperties());
    }

    @Test public void testWithWhitespace() {
        String URL = "jdbc:teiid:bqt@mm://localhost:12345; version =1;user= xyz ;password=***; logLevel = 1 ; configFile=testdata/bqt/dqp_stmt_e2e.xmi ;"; //$NON-NLS-1$

        Properties expectedProperties = new Properties();
        expectedProperties.setProperty("version", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("user", "xyz"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("password", "***"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("logLevel", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("configFile", "testdata/bqt/dqp_stmt_e2e.xmi"); //$NON-NLS-1$ //$NON-NLS-2$
        JDBCURL url = new JDBCURL(URL);
        assertEquals("bqt", url.getVDBName()); //$NON-NLS-1$
        assertEquals("mm://localhost:12345", url.getConnectionURL()); //$NON-NLS-1$
        assertEquals(expectedProperties, url.getProperties());
    }

    @Test public void testNoPropertyValue() {
        String URL = "jdbc:teiid:bqt@mm://localhost:12345;version=1;user=xyz;password=***;logLevel=;configFile="; //$NON-NLS-1$

        Properties expectedProperties = new Properties();
        expectedProperties.setProperty("version", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("user", "xyz"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("password", "***"); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("logLevel", ""); //$NON-NLS-1$ //$NON-NLS-2$
        expectedProperties.setProperty("configFile", ""); //$NON-NLS-1$ //$NON-NLS-2$
        JDBCURL url = new JDBCURL(URL);
        assertEquals("bqt", url.getVDBName()); //$NON-NLS-1$
        assertEquals("mm://localhost:12345", url.getConnectionURL()); //$NON-NLS-1$
        assertEquals(expectedProperties, url.getProperties());
    }

    @Test public void testInvalidProtocol() {
        String URL = "jdbc:monkeymatrix:bqt@mm://localhost:12345;version=1;user=xyz;password=***;logLevel=1"; //$NON-NLS-1$
        try {
            new JDBCURL(URL);
            fail("Illegal argument should have failed."); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test public void testNoVDBName() {
        String URL = "jdbc:teiid:@mm://localhost:12345;version=1;user=xyz;password=***;logLevel=1"; //$NON-NLS-1$
        try {
            new JDBCURL(URL);
            fail("Illegal argument should have failed."); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test public void testNoAtSignInURL() {
        String URL = "jdbc:teiid:bqt!mm://localhost:12345;version=1;user=xyz;password=***;logLevel=1"; //$NON-NLS-1$
        try {
            new JDBCURL(URL);
            // No @ sign is llowed as part of embedded driver now,
            // but this form of URL rejected in the acceptURL
            //fail("Illegal argument should have failed.");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test public void testMoreThanOneAtSign() {
        String URL = "jdbc:teiid:bqt@mm://localhost:12345;version=1;user=xy@;password=***;logLevel=1"; //$NON-NLS-1$
        try {
            // this allowed as customer properties can have @ in their properties
            new JDBCURL(URL);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test public void testNoEqualsInProperty() {
        String URL = "jdbc:teiid:bqt@mm://localhost:12345;version=1;user=xyz;password***;logLevel=1"; //$NON-NLS-1$
        try {
            new JDBCURL(URL);
            fail("Illegal argument should have failed."); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test public void testMoreThanOneEqualsInProperty() {
        String URL = "jdbc:teiid:bqt@mm://localhost:12345;version=1;user=xyz;password==***;logLevel=1"; //$NON-NLS-1$
        try {
            new JDBCURL(URL);
            fail("Illegal argument should have failed."); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // expected
        }
        URL = "jdbc:teiid:bqt@mm://localhost:12345;version=1;user=xyz;password=***=;logLevel=1"; //$NON-NLS-1$
        try {
            new JDBCURL(URL);
            fail("Illegal argument should have failed."); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // expected
        }
        URL = "jdbc:teiid:bqt@mm://localhost:12345;version=1;user=xyz;=password=***;logLevel=1"; //$NON-NLS-1$
        try {
            new JDBCURL(URL);
            fail("Illegal argument should have failed."); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test public void testNoKeyInProperty() {
        String URL = "jdbc:teiid:bqt@mm://localhost:12345;version=1;user=xyz;=***;logLevel=1"; //$NON-NLS-1$
        try {
            new JDBCURL(URL);
            fail("Illegal argument should have failed."); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test public void testConstructor() {
        JDBCURL url = new JDBCURL("myVDB", "mm://myhost:12345",null); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("jdbc:teiid:myVDB@mm://myhost:12345", url.getJDBCURL()); //$NON-NLS-1$

        Properties props = new Properties();
        props.setProperty(BaseDataSource.USER_NAME, "myuser"); //$NON-NLS-1$
        props.setProperty(BaseDataSource.PASSWORD, "mypassword"); //$NON-NLS-1$
        props.put("ClieNTtOKeN", new Integer(1)); //$NON-NLS-1$
        url = new JDBCURL("myVDB", "mm://myhost:12345", props); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("jdbc:teiid:myVDB@mm://myhost:12345;password=mypassword;user=myuser", url.getJDBCURL()); //$NON-NLS-1$
    }

    @Test public void testConstructor_Exception() {
        try {
            new JDBCURL(null, "myhost", null); //$NON-NLS-1$
            fail("Should have failed."); //$NON-NLS-1$
        } catch (Exception e) {

        }
        try {
            new JDBCURL("  ", "myhost", null); //$NON-NLS-1$ //$NON-NLS-2$
            fail("Should have failed."); //$NON-NLS-1$
        } catch (Exception e) {

        }

        try {
            // in embedded situation there is no connection url
            new JDBCURL("myVDB", "  ", null); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (Exception e) {

        }
    }

    @Test public void testNormalize() {
        Properties props = new Properties();
        props.setProperty("UsEr", "myuser"); //$NON-NLS-1$ //$NON-NLS-2$
        props.setProperty("pAssWOrD", "mypassword"); //$NON-NLS-1$ //$NON-NLS-2$
        props.put("ClieNTtOKeN", new Integer(1)); //$NON-NLS-1$
        JDBCURL.normalizeProperties(props);
        assertEquals("myuser", props.getProperty(BaseDataSource.USER_NAME)); //$NON-NLS-1$
        assertEquals("mypassword", props.getProperty(BaseDataSource.PASSWORD)); //$NON-NLS-1$
    }

    @Test public final void testEncodedPropertyProperties() throws Exception {
        String password = "=@#^&*()+!%$^%@#_-)_~{}||\\`':;,./<>?password has = & %"; //$NON-NLS-1$
        Properties props = new Properties();
        props.setProperty("UsEr", "foo"); //$NON-NLS-1$ //$NON-NLS-2$
        props.setProperty("PASswoRd", password); //$NON-NLS-1$
        JDBCURL.normalizeProperties(props);

        assertEquals(password, props.getProperty("password"));  //$NON-NLS-1$
        assertEquals("foo", props.getProperty("user"));  //$NON-NLS-1$//$NON-NLS-2$
    }

    @Test public final void testEncodedPropertyInURL() throws Exception {
        String password = "=@#^&*()+!%$^%@#_-)_~{}||\\`':;,./<>?password has = & %"; //$NON-NLS-1$
        String encPassword = URLEncoder.encode(password, "UTF-8"); //$NON-NLS-1$
        JDBCURL url = new JDBCURL("jdbc:teiid:QT_sqls2kds@mm://slwxp136:43100;PASswoRd="+encPassword); //$NON-NLS-1$
        Properties p = url.getProperties();
        assertEquals(password, p.getProperty("password"));  //$NON-NLS-1$
    }


    @Test public void testGetServerURL_NoProperties() {
        String result = new JDBCURL("jdbc:teiid:designtimecatalog@mm://slwxp172:44401;user=ddifranco;password=mm").getConnectionURL(); //$NON-NLS-1$
        assertEquals("mm://slwxp172:44401", result);         //$NON-NLS-1$
    }

    @Test public void testGetServerURL_Properties() {
        String result = new JDBCURL("jdbc:teiid:designtimecatalog@mm://slwxp172:44401;user=ddifranco;password=mm").getConnectionURL(); //$NON-NLS-1$
        assertEquals("mm://slwxp172:44401", result);         //$NON-NLS-1$
    }

    /**
     * Test getServerURL with a valid URL and password that contains at least
     * one ASCII character in the range of 32 to 126 excluding the ; and = sign.
     *
     * @since 5.0.2
     */
    @Test public void testGetServerURL_PasswordProperties() throws Exception {
        String result = null;
        String srcURL = "jdbc:teiid:designtimecatalog@mm://slwxp172:44401;user=ddifranco;password="; //$NON-NLS-1$
        String password = null;
        String tgtURL = "mm://slwxp172:44401"; //$NON-NLS-1$


        for ( char ch = 32; ch <= 126; ch++ ) {
            //exclude URL reserved characters
            if ( ch != ';' && ch != '=' && ch != '%') {
                password = ch+"mm"; //$NON-NLS-1$
                result = new JDBCURL(srcURL+URLEncoder.encode(password, "UTF-8")).getConnectionURL(); //$NON-NLS-1$
                assertEquals("Failed to obtain correct ServerURL when using password "+password,tgtURL, result);         //$NON-NLS-1$
            }
        }

    }

    @Test public void testGetServerURL_2Servers() {
        String result = new JDBCURL("jdbc:teiid:designtimecatalog@mm://slwxp172:44401,slabc123:12345;user=ddifranco;password=mm").getConnectionURL(); //$NON-NLS-1$
        assertEquals("mm://slwxp172:44401,slabc123:12345", result);         //$NON-NLS-1$
    }

    @Test public void testBuildEmbeedURL() {
        JDBCURL url = new JDBCURL("vdb", "/home/foo/deploy.properties", new Properties()); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("jdbc:teiid:vdb@/home/foo/deploy.properties", url.getJDBCURL()); //$NON-NLS-1$

        Properties p = new Properties();
        p.setProperty("user", "test"); //$NON-NLS-1$ //$NON-NLS-2$
        p.setProperty("password", "pass"); //$NON-NLS-1$ //$NON-NLS-2$
        p.setProperty("autoFailover", "true"); //$NON-NLS-1$ //$NON-NLS-2$
        p.setProperty("any", "thing"); //$NON-NLS-1$ //$NON-NLS-2$

        url = new JDBCURL("vdb", "/home/foo/deploy.properties", p); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue(url.getJDBCURL().startsWith("jdbc:teiid:vdb@/home/foo/deploy.properties;")); //$NON-NLS-1$
        assertTrue(url.getJDBCURL().indexOf("any=thing")!=-1); //$NON-NLS-1$
        assertTrue(url.getJDBCURL().indexOf("password=pass")!=-1); //$NON-NLS-1$
        assertTrue(url.getJDBCURL().indexOf("autoFailover=true")!=-1); //$NON-NLS-1$

    }

    @Test public void testUnicodeName() {
        String result = new JDBCURL("jdbc:teiid:%E4%BD%A0%E5%A5%BD").getVDBName(); //$NON-NLS-1$
        assertEquals("你好", result);         //$NON-NLS-1$
        result = new JDBCURL("jdbc:teiid:你好").getVDBName(); //$NON-NLS-1$
        assertEquals("你好", result);         //$NON-NLS-1$
    }

    @Test public void testEncoding() {
        JDBCURL url = new JDBCURL("jdbc:teiid:a%40b@mm://%50;%55=a"); //$NON-NLS-1$
        assertEquals("a@b", url.getVDBName());         //$NON-NLS-1$
        assertEquals("mm://P", url.getConnectionURL());         //$NON-NLS-1$
        assertEquals("U", url.getProperties().entrySet().iterator().next().getKey());         //$NON-NLS-1$
    }

}
