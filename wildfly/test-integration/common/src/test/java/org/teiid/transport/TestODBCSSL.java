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

package org.teiid.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.junit.After;
import org.junit.Test;
import org.postgresql.Driver;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.TestMMDatabaseMetaData;
import org.teiid.runtime.DoNothingSecurityHelper;
import org.teiid.security.Credentials;
import org.teiid.services.SessionServiceImpl;
import org.teiid.transport.TestODBCSocketTransport.FakeOdbcServer;
import org.teiid.transport.TestODBCSocketTransport.Mode;

@SuppressWarnings("nls")
public class TestODBCSSL {

    FakeOdbcServer odbcServer = new FakeOdbcServer();

    @After public void tearDown() {
        odbcServer.stop();
    }

    @Test public void testSelectSsl() throws Exception {
        odbcServer.start(Mode.ENABLED);
        Driver d = new Driver();
        Properties p = new Properties();
        p.setProperty("user", "testuser");
        p.setProperty("password", "testpassword");
        p.setProperty("ssl", "true");
        p.setProperty("sslfactory", "org.postgresql.ssl.NonValidatingFactory");
        p.setProperty("sslhostnameverifier", TestODBCSSL.AllowAllHostnameVerifier.class.getName());
        Connection conn = d.connect("jdbc:postgresql://"+odbcServer.addr.getHostName()+":" +odbcServer.odbcTransport.getPort()+"/parts", p);
        Statement s = conn.createStatement();
        assertTrue(s.execute("select * from sys.tables order by name"));
        TestMMDatabaseMetaData.compareResultSet("TestODBCSocketTransport/testSelect", s.getResultSet());

        p.setProperty("sslmode", "disable");
        try {
            conn = d.connect("jdbc:postgresql://"+odbcServer.addr.getHostName()+":" +odbcServer.odbcTransport.getPort()+"/parts", p);
            fail("should require ssl");
        } catch (SQLException e) {

        }
    }

    @Test(expected=SQLException.class) public void testLogin() throws Exception {
        odbcServer.start(Mode.LOGIN);
        Driver d = new Driver();
        Properties p = new Properties();
        p.setProperty("user", "testuser");
        p.setProperty("password", "testpassword");
        d.connect("jdbc:postgresql://"+odbcServer.addr.getHostName()+":" +odbcServer.odbcTransport.getPort()+"/parts", p);
    }

    @Test(expected=SQLException.class) public void testNonSSL() throws Exception {
        odbcServer.start(Mode.DISABLED);
        Driver d = new Driver();
        Properties p = new Properties();
        p.setProperty("user", "testuser");
        p.setProperty("password", "testpassword");
        p.setProperty("ssl", "true");
        p.setProperty("sslfactory", "org.postgresql.ssl.NonValidatingFactory");
        d.connect("jdbc:postgresql://"+odbcServer.addr.getHostName()+":" +odbcServer.odbcTransport.getPort()+"/parts", p);
    }

    @Test public void testWantAuth() throws Exception {
        odbcServer.start(Mode.WANT);
        Driver d = new Driver();
        Properties p = new Properties();
        p.setProperty("user", "testuser");
        p.setProperty("password", "testpassword");
        p.setProperty("ssl", "true");
        p.setProperty("sslfactory", "org.postgresql.ssl.NonValidatingFactory");
        p.setProperty("sslhostnameverifier", TestODBCSSL.AllowAllHostnameVerifier.class.getName());
        //server - should work
        Connection conn = d.connect("jdbc:postgresql://"+odbcServer.addr.getHostName()+":" +odbcServer.odbcTransport.getPort()+"/parts", p);
        Statement s = conn.createStatement();
        assertTrue(s.execute("select * from sys.tables order by name"));
        TestMMDatabaseMetaData.compareResultSet("TestODBCSocketTransport/testSelect", s.getResultSet());

        //no ssl - should fail
        p.setProperty("sslmode", "disable");
        try {
            conn = d.connect("jdbc:postgresql://"+odbcServer.addr.getHostName()+":" +odbcServer.odbcTransport.getPort()+"/parts", p);
            fail("should require ssl");
        } catch (SQLException e) {

        }

        //mutual auth
        p.setProperty("sslfactory", "org.postgresql.ssl.jdbc4.LibPQFactory");
        p.setProperty("sslcert", UnitTestUtil.getTestDataPath() + "/selfsigned.crt");
        p.setProperty("sslkey", UnitTestUtil.getTestDataPath() + "/selfsigned.pk8");
        //sslrootcert ??
        p.setProperty("sslmode", "require");
        conn = d.connect("jdbc:postgresql://"+odbcServer.addr.getHostName()+":" +odbcServer.odbcTransport.getPort()+"/parts", p);
    }

    @Test public void testSSLAuth() throws Exception {
        odbcServer.start(Mode.SSL_AUTH);
        Driver d = new Driver();
        Properties p = new Properties();
        p.setProperty("user", "testuser");
        p.setProperty("password", "testpassword");
        p.setProperty("ssl", "true");
        p.setProperty("sslfactory", "org.postgresql.ssl.NonValidatingFactory");

        //server / 1-way - should not work
        try {
            d.connect("jdbc:postgresql://"+odbcServer.addr.getHostName()+":" +odbcServer.odbcTransport.getPort()+"/parts", p);
            fail("should require 2-way ssl");
        } catch (SQLException e) {

        }

        SessionServiceImpl ss = odbcServer.server.getSessionService();
        ss.setSecurityHelper(new DoNothingSecurityHelper() {
            Subject s = new Subject();

            @Override
            public Subject getSubjectInContext(Object context) {
                return (Subject)context;
            }

            @Override
            public Object getSecurityContext(String securityDomain) {
                return s;
            }

            @Override
            public Object authenticate(String securityDomain,
                    String baseUserName, Credentials credentials,
                    String applicationName) throws LoginException {
                X509Certificate cert = (X509Certificate)credentials.getCredentials();
                s.getPrincipals().add(cert.getSubjectDN());
                return s;
            }

        });


        //mutual auth
        p.setProperty("sslfactory", "org.postgresql.ssl.jdbc4.LibPQFactory");
        p.setProperty("sslcert", UnitTestUtil.getTestDataPath() + "/selfsigned.crt");
        p.setProperty("sslkey", UnitTestUtil.getTestDataPath() + "/selfsigned.pk8");
        p.setProperty("sslrootcert", UnitTestUtil.getTestDataPath() + "/selfsigned.crt");
        p.setProperty("ssl", "true");
        p.setProperty("sslhostnameverifier", TestODBCSSL.AllowAllHostnameVerifier.class.getName());
        Connection conn = d.connect("jdbc:postgresql://"+odbcServer.addr.getHostName()+":" +odbcServer.odbcTransport.getPort()+"/parts", p);
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select user()");
        rs.next();
        assertEquals("CN=Steve Hawkins, OU=org, O=jboss, L=Winston-Salem, ST=North Carolina, C=US@teiid-security", rs.getString(1));
    }

    public static class AllowAllHostnameVerifier implements HostnameVerifier {
        @Override
        public boolean verify(String hostname, SSLSession session) {
            return true;
        }
    }

}
