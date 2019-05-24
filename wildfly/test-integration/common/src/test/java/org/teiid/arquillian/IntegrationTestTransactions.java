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

package org.teiid.arquillian;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.util.Random;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.jboss.arquillian.junit.Arquillian;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.jboss.AdminFactory;
import org.teiid.client.xa.XidImpl;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.TeiidDataSource;
import org.teiid.jdbc.TeiidDriver;

@RunWith(Arquillian.class)
@SuppressWarnings("nls")
public class IntegrationTestTransactions extends AbstractMMQueryTestCase {

    private static Admin admin;

    @BeforeClass
    public static void setup() throws Exception {
        admin = AdminFactory.getInstance().createAdmin("localhost", AdminUtil.MANAGEMENT_PORT,	"admin", "admin".toCharArray());
        admin.deploy("txn-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("txn-vdb.xml")));
        assertTrue(AdminUtil.waitForVDBLoad(admin, "txn", 1));
    }

    @AfterClass
    public static void teardown() throws AdminException {
        AdminUtil.cleanUp(admin);
        admin.close();
    }

    @Test
    public void testViewDefinition() throws Exception {
        this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:txn@mm://localhost:31000;user=user;password=user", null);
        execute("create local temporary table temp (x integer)"); //$NON-NLS-1$
        execute("call proc()");
        execute("start transaction"); //$NON-NLS-1$
        execute("call proc()");
        execute("insert into temp (x) values (1)"); //$NON-NLS-1$
        execute("select * from temp");
        assertRowCount(1);
        execute("rollback");
        execute("select * from temp");
        assertRowCount(0);

        execute("select rand(1)"); //TODO - I think our rand function doesn't make sense
        this.internalConnection.setAutoCommit(false);

        execute("/*+ cache */ select rand()");
        internalResultSet.next();
        double d = internalResultSet.getDouble(1);

        execute("select rand(2)");
        execute("/*+ cache */ select rand()");
        internalResultSet.next();
        double d1 = internalResultSet.getDouble(1);
        assertEquals("Expected same in the txn", d, d1, 0);

        this.internalConnection.rollback();
        this.internalConnection.setAutoCommit(true);

        execute("select rand(3)");
        execute("/*+ cache */ select rand()");
        internalResultSet.next();
        double d2 = internalResultSet.getDouble(1);
        assertTrue("Expected different after rollback", d != d2);

        execute("select rand(4)");
        execute("/*+ cache */ select rand()");
        internalResultSet.next();
        d = internalResultSet.getDouble(1);
        assertEquals("Expected same after autoCommit", d, d2, 0);
    }

    @Test public void testXa() throws Exception {
        this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:txn@mm://localhost:31000;user=user;password=user", null);
        TeiidDataSource tds = new TeiidDataSource();
        tds.setDatabaseName("txn");
        tds.setServerName("localhost");
        tds.setPortNumber(31000);
        XAConnection xa = tds.getXAConnection("user", "user");
        XAResource xar = xa.getXAResource();
        xar.setTransactionTimeout(100);
        byte[] gid = new byte[10];
        byte[] bid = new byte[10];
        Random r = new Random();
        r.nextBytes(gid);
        r.nextBytes(bid);
        XidImpl xidImpl = new XidImpl(0, gid, bid);

        //sequence should succeed
        xar.start(xidImpl, XAResource.TMNOFLAGS);
        xar.end(xidImpl, XAResource.TMFAIL);
        xar.rollback(xidImpl);

        try {
            xar.start(new XidImpl(), XAResource.TMNOFLAGS);
            fail("invalid xid should fail");
        } catch (XAException e) {

        }
    }

}
