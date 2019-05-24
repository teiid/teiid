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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.FakeServer;

@SuppressWarnings("nls")
public class TestPGMetadata extends AbstractMMQueryTestCase {
    static FakeServer server = null;

    @BeforeClass
    public static void setup() {
        server = new FakeServer(true);
    }

    @AfterClass
    public static void teardown() {
            server.stop();
    }

    private static VDBMetaData buildVDB(String name) {
        VDBMetaData vdb = new VDBMetaData();
        vdb.setName(name);
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("x");
        mmd.addSourceMetadata("DDL", "create view v as select 1");
        mmd.setModelType(Type.VIRTUAL);
        vdb.addModel(mmd);
        return vdb;
    }

    @Test
    public void test_PG_MetadataOFF() throws Exception {
        VDBMetaData vdb = buildVDB("x");
        vdb.addProperty("include-pg-metadata", "false");
        server.deployVDB(vdb);
        this.internalConnection = server.createConnection("jdbc:teiid:x"); //$NON-NLS-1$ //$NON-NLS-2$
        try {
            execute("select * FROM pg_am"); //$NON-NLS-1$
            Assert.fail("there should be no PG metadata");
        } catch (Exception e) {
        }
    }
    @Test
    public void test_PG_Metadata_ON() throws Exception {
        VDBMetaData vdb = buildVDB("y");
        vdb.addProperty("include-pg-metadata", "true");
        server.deployVDB(vdb);
        this.internalConnection = server.createConnection("jdbc:teiid:y"); //$NON-NLS-1$ //$NON-NLS-2$
        execute("select * FROM pg_am"); //$NON-NLS-1$
    }
    @Test
    public void test_PG_Metadata_DEFAULT() throws Exception {
        VDBMetaData vdb = buildVDB("z");
        server.deployVDB(vdb);
        this.internalConnection = server.createConnection("jdbc:teiid:z"); //$NON-NLS-1$ //$NON-NLS-2$
        execute("select * FROM pg_am"); //$NON-NLS-1$
    }

    @Test public void testTypes() throws Exception {
        VDBMetaData vdb = buildVDB("t");
        server.deployVDB(vdb);
        this.internalConnection = server.createConnection("jdbc:teiid:t"); //$NON-NLS-1$ //$NON-NLS-2$
        execute("select format_type((select oid from pg_type where typname = '_int2'), 0)"); //$NON-NLS-1$
        assertResults(new String[] {"expr1[string]", "smallint[]"});

        execute("select format_type((select oid from pg_type where typname = 'float4'), 0)"); //$NON-NLS-1$
        assertResults(new String[] {"expr1[string]", "real"});

        execute("select format_type((select oid from pg_type where typname = 'numeric'), 100)"); //$NON-NLS-1$
        assertResults(new String[] {"expr1[string]", "numeric(0,96)"});

        execute("select format_type((select oid from pg_type where typname = 'bpchar'), 100)"); //$NON-NLS-1$
        assertResults(new String[] {"expr1[string]", "character(96)"});
    }

}
