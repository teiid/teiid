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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.TestMMDatabaseMetaData;

@SuppressWarnings("nls")
public class TestODBCProceduresSchema extends AbstractMMQueryTestCase {
    private static final String VDB = "bqt"; //$NON-NLS-1$
    private static FakeServer server;

    public TestODBCProceduresSchema() {
        // this is needed because the result files are generated
        // with another tool which uses tab as delimiter
        super.DELIMITER = "\t"; //$NON-NLS-1$
    }

    @BeforeClass public static void oneTimeSetup() throws Exception {
        server = new FakeServer(true);
        server.deployVDB(VDB, UnitTestUtil.getTestDataPath() + "/bqt.vdb");
    }

    @AfterClass public static void oneTimeTeardown() throws Exception {
        server.stop();
    }

    @Before public void setUp() throws Exception {
        this.internalConnection = server.createConnection("jdbc:teiid:" + VDB); //$NON-NLS-1$ //$NON-NLS-2$
       }

    @Test public void test_Pg_Proc_alltypes() throws Exception {
        execute("select oid, proname, proretset,prorettype, pronargs, proargtypes, proargnames, proargmodes, proallargtypes, pronamespace FROM pg_proc where proname='bigProcedure'"); //$NON-NLS-1$
        TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
    }

    @Test public void test_Pg_Proc_void() throws Exception {
        execute("select oid, proname, proretset,prorettype, pronargs, proargtypes, proargnames, proargmodes, proallargtypes, pronamespace FROM pg_proc where proname='VoidProcedure'"); //$NON-NLS-1$
        TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
    }

    @Test public void test_Pg_Proc_with_return() throws Exception {
        execute("select oid, proname, proretset,prorettype, pronargs, proargtypes, proargnames, proargmodes, proallargtypes, pronamespace FROM pg_proc where proname='ProcedureWithReturn'"); //$NON-NLS-1$
        TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
    }

    @Test public void test_Pg_Proc_with_return_table() throws Exception {
        execute("select oid, proname, proretset,prorettype, pronargs, proargtypes, proargnames, proargmodes, proallargtypes, pronamespace FROM pg_proc where proname='ProcedureReturnTable'"); //$NON-NLS-1$
        TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
    }
}
