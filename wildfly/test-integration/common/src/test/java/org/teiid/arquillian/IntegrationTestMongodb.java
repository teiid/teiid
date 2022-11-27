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

package org.teiid.arquillian;

import static org.junit.Assert.*;

import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.Properties;

import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.jboss.AdminFactory;
import org.teiid.core.util.ReaderInputStream;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.TeiidDriver;

@Ignore
@RunWith(Arquillian.class)
@SuppressWarnings("nls")
public class IntegrationTestMongodb extends AbstractMMQueryTestCase {

    private Admin admin;

    @Before
    public void setup() throws Exception {
        admin = AdminFactory.getInstance().createAdmin("localhost", AdminUtil.MANAGEMENT_PORT, "admin", "admin".toCharArray());
    }

    @After
    public void teardown() throws AdminException {
        AdminUtil.cleanUp(admin);
        admin.close();
    }
    @Test
    public void testOdata() throws Exception {
        Properties props = new Properties();
        props.setProperty("SecurityType", "NONE");
        props.setProperty("Database", "myproject");
        props.setProperty("RemoteServerList", "localhost:27017");
        props.setProperty("min-pool-size", "1");
        props.setProperty("transaction-support", "LocalTransaction");
        props.setProperty("class-name", "org.teiid.resource.adapter.mongodb.MongoDBManagedConnectionFactory");

        AdminUtil.createDataSource(admin, "mongodbDS", "mongodb", props);

        String vdb = "<vdb name=\"mongodb\" version=\"1\">"
                + "<model name=\"mongodb\" type=\"PHYSICAL\">"
                + "<source name=\"mongodb\" translator-name=\"mongodb\" connection-jndi-name=\"java:/mongodbDS\"/>"
                + "         <metadata type=\"DDL\"><![CDATA[\n" +
                "                CREATE FOREIGN TABLE G1 (\n" +
                "    e1 integer NOT NULL,\n" +
                "    e2 integer NOT NULL,\n" +
                "    e3 integer,\n" +
                "    PRIMARY KEY (e1)\n" +
                ") OPTIONS(UPDATABLE 'TRUE');" +
                "        ]]> </metadata>\n"
                + "</model></vdb>";

        admin.deploy("mongodb-vdb.xml", new ReaderInputStream(new StringReader(vdb), Charset.forName("UTF-8")));

        assertTrue(AdminUtil.waitForVDBLoad(admin, "mongodb", 1));

        this.internalConnection = TeiidDriver.getInstance()
                .connect("jdbc:teiid:mongodb@mm://localhost:31000;user=user;password=user", null);

        this.internalConnection.setAutoCommit(false);

        execute("delete from G1");
        execute("insert into G1 (e1, e2, e3) values (1, 1, 1)");
        execute("insert into G1 (e1, e2, e3) values (2, 2, 2)");
        execute("insert into G1 (e1, e2, e3) values (3, 3, 3)");

        execute("select * from G1");
        assertRowCount(3);

        this.internalConnection.commit();

        execute("select * from G1");
        assertRowCount(3);

        execute("insert into G1 (e1, e2, e3) values (4, 4, 4)");
        execute("insert into G1 (e1, e2, e3) values (5, 5, 5)");

        execute("select * from G1");
        assertRowCount(5);

        this.internalConnection.rollback();

        execute("select * from G1");
        assertRowCount(3);
    }
}
