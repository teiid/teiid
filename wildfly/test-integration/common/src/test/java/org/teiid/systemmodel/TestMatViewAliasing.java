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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.core.types.DataTypeManager;
import org.teiid.deployers.VDBRepository;
import org.teiid.jdbc.FakeServer;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Table;

@SuppressWarnings("nls")
public class TestMatViewAliasing {

    private static final String MATVIEWS = "matviews";
    private Connection conn;
    private FakeServer server;

    @Before public void setUp() throws Exception {
        server = new FakeServer(true);

        VDBRepository vdbRepository = new VDBRepository();
        MetadataFactory mf = new MetadataFactory(null, 1, "foo", vdbRepository.getRuntimeTypeMap(), new Properties(), null);
        mf.getSchema().setPhysical(false);
        Table mat = mf.addTable("mat");
        mat.setVirtual(true);
        mat.setMaterialized(true);
        mat.setSelectTransformation("/*+ cache(ttl:0) */ select 1 as x, 'y' as Name");

        mf.addColumn("x", DataTypeManager.DefaultDataTypes.INTEGER, mat);
        mf.addColumn("Name", DataTypeManager.DefaultDataTypes.STRING, mat);

        MetadataStore ms = mf.asMetadataStore();

        server.deployVDB(MATVIEWS, ms);
        conn = server.createConnection("jdbc:teiid:"+MATVIEWS);
    }

    @After public void tearDown() throws Exception {
        server.stop();
        conn.close();
    }

    @Test public void testSystemMatViewsWithImplicitLoad() throws Exception {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select * from MatViews order by name");
        assertTrue(rs.next());
        assertEquals("NEEDS_LOADING", rs.getString("loadstate"));
        assertEquals(false, rs.getBoolean("valid"));

        rs = s.executeQuery("select * from mat order by x");
        assertTrue(rs.next());
        rs = s.executeQuery("select * from MatViews where name = 'mat'");
        assertTrue(rs.next());
        assertEquals("LOADED", rs.getString("loadstate"));

        rs = s.executeQuery("select * from mat as a, mat as b where cast(a.x as string) = b.name order by a.x");
        assertFalse(rs.next());
    }

}
