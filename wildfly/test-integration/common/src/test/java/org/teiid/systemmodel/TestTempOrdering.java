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

package org.teiid.systemmodel;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.FakeServer;
import org.teiid.runtime.EmbeddedConfiguration;

@SuppressWarnings("nls")
public class TestTempOrdering {

    private static final String VDB = "PartsSupplier"; //$NON-NLS-1$

    private static FakeServer server;

    @BeforeClass public static void setup() throws Exception {
        server = new FakeServer(false);
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        Properties p = new Properties();
        p.setProperty("org.teiid.defaultNullOrder", "HIGH");
        ec.setProperties(p);
        server.start(ec, false);
        server.deployVDB(VDB, UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");
    }

    @AfterClass public static void teardown() throws Exception {
        server.stop();
    }

    @Test public void testNullOrder() throws Exception {
        Connection c = server.createConnection("jdbc:teiid:PartsSupplier");
        Statement s = c.createStatement();
        s.execute("insert into #temp (a) values (null),(1)");

        //will be high based upon the system property
        ResultSet rs = s.executeQuery("select * from #temp order by a");
        rs.next();
        assertNotNull(rs.getObject(1));

        rs = s.executeQuery("select * from #temp order by a nulls first");
        rs.next();
        assertNull(rs.getObject(1));
    }
}
