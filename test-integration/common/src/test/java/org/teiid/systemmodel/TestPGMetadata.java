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
    }
    
}
