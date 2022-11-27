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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.infinispan.transaction.tm.DummyTransactionManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.CommandContext;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.security.Credentials;
import org.teiid.security.GSSResult;
import org.teiid.security.SecurityHelper;
import org.teiid.transport.SocketConfiguration;
import org.teiid.transport.WireProtocol;

@SuppressWarnings("nls")
public class TestInternalConnection {

    public static class ThreadLocalSecurityHelper implements SecurityHelper {

        private static ThreadLocal<Subject> threadLocalContext = new ThreadLocal<Subject>();

        @Override
        public Object associateSecurityContext(Object context) {
            Object previous = threadLocalContext.get();
            threadLocalContext.set((Subject)context);
            return previous;
        }

        @Override
        public Object getSecurityContext(String securityDomain) {
            return threadLocalContext.get();
        }

        @Override
        public void clearSecurityContext() {
            threadLocalContext.remove();
        }

        @Override
        public Object authenticate(String securityDomain,
                String baseUserName, Credentials credentials,
                String applicationName) throws LoginException {
            return new Subject();
        }

        @Override
        public Subject getSubjectInContext(Object context) {
            return (Subject)context;
        }

        @Override
        public GSSResult negotiateGssLogin(String securityDomain,
                byte[] serviceTicket) throws LoginException {
            return null;
        }

    }

    private static final String vdb = "<vdb name=\"test\" version=\"1\"><model name=\"test\" type=\"VIRTUAL\"><metadata type=\"DDL\"><![CDATA["
            + "CREATE VIEW helloworld as SELECT 'HELLO WORLD';"
            + "CREATE function func (val integer) returns string options (JAVA_CLASS '"+TestInternalConnection.class.getName()+"',  JAVA_METHOD 'doSomething');]]> </metadata></model></vdb>";
    EmbeddedServer es;
    static boolean useTxn = false;

    @Before public void setup() {
        es = new EmbeddedServer();
    }

    @After public void teardown() {
        es.stop();
        useTxn = false;
    }

    public static String doSomething(CommandContext cc, Integer val) throws SQLException {
        TeiidConnection tc = cc.getConnection();
        try {
            Statement s = tc.createStatement();
            if (useTxn) {
                s.execute("set autoCommitTxn on");
            }
            ResultSet rs = s.executeQuery("select user(), expr1 from helloworld");
            rs.next();
            return rs.getString(1) + rs.getString(2) + val;
        } finally {
            tc.close();
        }
    }

    @Test public void testInternalRemote() throws Exception {
        SocketConfiguration s = new SocketConfiguration();
        InetSocketAddress addr = new InetSocketAddress(0);
        s.setBindAddress(addr.getHostName());
        s.setPortNumber(addr.getPort());
        s.setProtocol(WireProtocol.teiid);
        EmbeddedConfiguration config = new EmbeddedConfiguration();
        config.addTransport(s);
        config.setSecurityHelper(new ThreadLocalSecurityHelper());
        es.start(config);
        es.deployVDB(new ByteArrayInputStream(vdb.getBytes()));
        Connection conn = null;
        try {
            TeiidDriver driver = new TeiidDriver();
            Properties p = new Properties();
            p.setProperty("user", "me");
            conn = driver.connect("jdbc:teiid:test@mm://"+addr.getHostName()+":"+es.getPort(0), p);
            ResultSet rs = conn.createStatement().executeQuery("select func(1)");
            rs.next();
            assertEquals("me@teiid-securityHELLO WORLD1", rs.getString(1));
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test public void testInternalLocal() throws Exception {
        EmbeddedConfiguration config = new EmbeddedConfiguration();
        config.setSecurityHelper(new ThreadLocalSecurityHelper());
        es.start(config);
        es.deployVDB(new ByteArrayInputStream(vdb.getBytes()));
        Connection conn = null;
        try {
            TeiidDriver driver = es.getDriver();
            conn = driver.connect("jdbc:teiid:test", null);
            //execute multiple to check for an id conflict
            ResultSet rs = conn.createStatement().executeQuery("select func(2) union all select func(3)");
            rs.next();
            assertEquals("anonymous@teiid-securityHELLO WORLD2", rs.getString(1));
            rs.next();
            assertEquals("anonymous@teiid-securityHELLO WORLD3", rs.getString(1));
            ResultSetMetaData metadata = rs.getMetaData();
            assertNotNull(metadata.getColumnName(1));
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test public void testInternalLocalNestedTransactions() throws Exception {
        useTxn = true;
        EmbeddedConfiguration config = new EmbeddedConfiguration();
        config.setSecurityHelper(new ThreadLocalSecurityHelper());
        config.setTransactionManager(new DummyTransactionManager());
        es.start(config);
        es.deployVDB(new ByteArrayInputStream(vdb.getBytes()));
        Connection conn = null;
        TeiidDriver driver = es.getDriver();
        conn = driver.connect("jdbc:teiid:test;autoCommitTxn=on", null);
        try {

            PreparedStatement ps = conn.prepareStatement("select func(?)");
            ps.setInt(1, 1);
            ps.execute();
        } finally {
            if (conn != null) {
                conn.close();
            }
        }

        conn = driver.connect("jdbc:teiid:test;autoCommitTxn=on", null);
        try {
            conn.setAutoCommit(false);
            PreparedStatement ps = conn.prepareStatement("select func(?)");
            ps.setInt(1, 1);
            ps.execute();
            conn.commit();
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    /**
     * Within the scope of the function, all design time metadata should be visible
     */
    @Test public void testInternalLocalHidden() throws Exception {
        EmbeddedConfiguration config = new EmbeddedConfiguration();
        config.setSecurityHelper(new ThreadLocalSecurityHelper());
        es.start(config);
        String hiddenVdb = "<vdb name=\"test\" version=\"1\">"
                + "<model name=\"hidden\" type=\"VIRTUAL\" visible=\"false\"><metadata type=\"DDL\"><![CDATA["
                + "CREATE VIEW helloworld as SELECT 'HELLO WORLD';"
                + "]]></metadata></model>"
                + "<model name=\"test\" type=\"VIRTUAL\"><metadata type=\"DDL\"><![CDATA["
                + "CREATE function func (val integer) returns string options (JAVA_CLASS '"+TestInternalConnection.class.getName()+"',  JAVA_METHOD 'doSomething');]]> </metadata></model>"
                + "</vdb>";
        es.deployVDB(new ByteArrayInputStream(hiddenVdb.getBytes()));
        Connection conn = null;
        try {
            TeiidDriver driver = es.getDriver();
            conn = driver.connect("jdbc:teiid:test", null);
            //execute multiple to check for an id conflict
            ResultSet rs = conn.createStatement().executeQuery("select func(2)");
            rs.next();
            assertEquals("anonymous@teiid-securityHELLO WORLD2", rs.getString(1));
            ResultSetMetaData metadata = rs.getMetaData();
            assertNotNull(metadata.getColumnName(1));
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

}
