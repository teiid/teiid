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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.StringReader;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.jboss.AdminFactory;
import org.teiid.core.util.ReaderInputStream;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.TeiidDriver;

@RunWith(Arquillian.class)
@SuppressWarnings("nls")
public class IntegrationTestParquet extends AbstractMMQueryTestCase {

    private static Admin admin;

    @BeforeClass
    public static void setup() throws Exception {
        admin = AdminFactory.getInstance().createAdmin("localhost", AdminUtil.MANAGEMENT_PORT, "admin", "admin".toCharArray());

        Properties props = new Properties();
        props.setProperty("ParentDirectory", UnitTestUtil.getTestDataFile("/arquillian/").getAbsolutePath());
        props.setProperty("class-name", "org.teiid.resource.adapter.file.FileManagedConnectionFactory");

        AdminUtil.createDataSource(admin, "fileDS", "file", props);

        String vdb = "<vdb name=\"parquet\" version=\"1\">"
                + "<model name=\"parquet\" type=\"PHYSICAL\">"
                + "<source name=\"parquet\" translator-name=\"parquet\" connection-jndi-name=\"java:/fileDS\"/>"
                + "  <metadata type=\"DDL\"><![CDATA[\n" +
                  "    CREATE FOREIGN TABLE Table1 (\n" +
                        "   contacts integer[] options (searchable 'unsearchable'),\n" +
                        "   id long ,\n" +
                        "   last string ,\n" +
                        "   name string ,\n" +
                        "   CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                        ") OPTIONS (\"teiid_parquet:LOCATION\" 'parquet')"+
                "        ]]> </metadata>\n"
                + "</model>"
                + "</vdb>";

        admin.deploy("parquet-vdb.xml", new ReaderInputStream(new StringReader(vdb), Charset.forName("UTF-8")));

        assertTrue(AdminUtil.waitForVDBLoad(admin, "parquet", 1));
    }

    @AfterClass
    public static void teardown() throws AdminException {
        AdminUtil.cleanUp(admin);
        admin.close();
    }

    @Before
    public void before() throws SQLException {
        this.internalConnection = TeiidDriver.getInstance()
                .connect("jdbc:teiid:parquet@mm://localhost:31000;user=user;password=user", null);
    }

    @After
    public void after() throws SQLException {
        if (this.internalConnection != null) {
            this.internalConnection.close();
        }
    }

    @Test
    public void testSearch() throws Exception {
        this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:parquet@mm://localhost:31000;user=user;password=user", null);
        Statement s = this.internalConnection.createStatement();
        ResultSet rs = s.executeQuery("select count(*) from table1");
        rs.next();
        assertEquals(2, rs.getInt(1));
    }

}
