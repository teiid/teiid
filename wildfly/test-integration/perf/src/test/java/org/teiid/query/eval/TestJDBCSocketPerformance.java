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

package org.teiid.query.eval;

import static org.junit.Assert.*;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.language.QueryExpression;
import org.teiid.query.processor.TestTextTable;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.HardCodedExecutionFactory;
import org.teiid.transport.SSLConfiguration;
import org.teiid.transport.SocketConfiguration;
import org.teiid.transport.SocketListener;

@SuppressWarnings("nls")
public class TestJDBCSocketPerformance {

    static InetSocketAddress addr;
    static SocketListener jdbcTransport;
    static FakeServer server;

    @BeforeClass public static void oneTimeSetup() throws Exception {
        SocketConfiguration config = new SocketConfiguration();
        config.setSSLConfiguration(new SSLConfiguration());
        addr = new InetSocketAddress(0);
        config.setBindAddress(addr.getHostName());
        config.setPortNumber(0);

        EmbeddedConfiguration dqpConfig = new EmbeddedConfiguration();
        dqpConfig.setMaxActivePlans(2);
        server = new FakeServer(false);
        server.start(dqpConfig);
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("x");
        mmd.setModelType(Type.PHYSICAL);
        mmd.addSourceMapping("x", "hc", null);
        mmd.setSchemaSourceType("ddl");
        StringBuffer ddl = new StringBuffer("create foreign table x (col0 string");
        for (int i = 1; i < 10; i++) {
            ddl.append(",").append(" col").append(i).append(" string");
        }
        ddl.append(");");
        mmd.setSchemaText(ddl.toString());
        server.addTranslator("hc", new HardCodedExecutionFactory() {
            @Override
            protected List<? extends List<?>> getData(QueryExpression command) {
                List<List<String>> result = new ArrayList<List<String>>();
                int size = command.getProjectedQuery().getDerivedColumns().size();
                for (int i = 0; i < 64; i++) {
                    List<String> row = new ArrayList<String>(size);
                    for (int j = 0; j < size; j++) {
                        row.add("abcdefghi" + j);
                    }
                    result.add(row);
                }
                return result;
            }
        });
        server.deployVDB("x", mmd);

        jdbcTransport = new SocketListener(addr, config, server.getClientServiceRegistry(), BufferManagerFactory.getStandaloneBufferManager());
    }

    @AfterClass public static void oneTimeTearDown() throws Exception {
        if (jdbcTransport != null) {
            jdbcTransport.stop();
        }
        server.stop();
    }

    @Test public void testLargeSelects() throws Exception {
        Properties p = new Properties();
        p.setProperty("user", "testuser");
        p.setProperty("password", "testpassword");
        Connection conn = TeiidDriver.getInstance().connect("jdbc:teiid:x@mm://"+addr.getHostName()+":" +jdbcTransport.getPort(), p);
        long start = System.currentTimeMillis();
        for (int j = 0; j < 10; j++) {
            Statement s = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            assertTrue(s.execute("select * from x as x1, x as x2, x as x3"));
            ResultSet rs = s.getResultSet();
            int i = 0;
            while (rs.next()) {
                i++;
            }
            s.close();
        }
        System.out.println((System.currentTimeMillis() - start));
    }

    @Test public void testSmallSelects() throws Exception {
        Properties p = new Properties();
        p.setProperty("user", "testuser");
        p.setProperty("password", "testpassword");
        Connection conn = TeiidDriver.getInstance().connect("jdbc:teiid:x@mm://"+addr.getHostName()+":" +jdbcTransport.getPort(), p);
        long start = System.currentTimeMillis();
        for (int j = 0; j < 1000; j++) {
            Statement s = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            assertTrue(s.execute("select 1"));
            ResultSet rs = s.getResultSet();
            int i = 0;
            while (rs.next()) {
                i++;
            }
            s.close();
        }
        System.out.println((System.currentTimeMillis() - start));
    }

    //TODO: this isn't a socket test per se, but does show a performance bump with multi-threaded texttable execution
    @Test public void testTextTable() throws Exception {
        Properties p = new Properties();
        p.setProperty("user", "testuser");
        p.setProperty("password", "testpassword");
        Connection conn = TeiidDriver.getInstance().connect("jdbc:teiid:x@mm://"+addr.getHostName()+":" +jdbcTransport.getPort(), p);
        for (int j = 0; j < 10; j++) {
            PreparedStatement ps = conn.prepareStatement("select * from (select * from texttable(cast (? as clob) columns x string width 100000 no row delimiter) as x limit 1) as x, texttable(cast (? as clob) columns x string width 100 no row delimiter) as y");

            ps.setClob(1, TestTextTable.clobFromFile("test.xml").getReference());
            ps.setClob(2, TestTextTable.clobFromFile("test.xml").getReference());

            ResultSet rs = ps.executeQuery();
            int i = 0;
            while (rs.next()) {
                i++;
            }
            assertEquals(58454, i);
            ps.close();
        }
    }

}
