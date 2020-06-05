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

import java.io.ByteArrayInputStream;

import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.jboss.AdminFactory;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.TeiidDriver;

@RunWith(Arquillian.class)
@SuppressWarnings("nls")
public class IntegrationTestJsonPath extends AbstractMMQueryTestCase {

    private Admin admin;

    @Before
    public void setup() throws Exception {
        admin = AdminFactory.getInstance().createAdmin("localhost",
                AdminUtil.MANAGEMENT_PORT, "admin", "admin".toCharArray());
    }

    @After
    public void teardown() throws AdminException {
        AdminUtil.cleanUp(admin);
        admin.close();
    }

    @Test
    public void testJsonPathValue() throws Exception {
        String ddl = "create database foo version '1';"
                + "use database foo version '1';"
                + "create server NONE type 'NONE' foreign data wrapper loopback;"
                + "create schema test server NONE;"
                + "set schema test;"
                + "CREATE FOREIGN TABLE G1 (e1 integer PRIMARY KEY, e2 varchar(25), e3 double)";

        this.admin.deploy("foo-vdb.ddl", new ByteArrayInputStream(ddl.getBytes()), false);
        this.internalConnection = TeiidDriver.getInstance()
                .connect("jdbc:teiid:foo@mm://localhost:31000;user=user;password=user", null);

        execute("SELECT jsonpathvalue('{}', '$.*')"); //$NON-NLS-1$
        assertRowCount(1);
    }

}
