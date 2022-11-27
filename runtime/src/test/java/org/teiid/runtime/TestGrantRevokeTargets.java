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

package org.teiid.runtime;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.adminapi.AdminProcessingException;

@SuppressWarnings("nls")
public class TestGrantRevokeTargets {

    EmbeddedServer es;

    @Before public void setup() {
        es = new EmbeddedServer();
    }

    @After public void teardown() {
        if (es != null) {
            es.stop();
        }
    }

    @Test public void testGrantTargetTable() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        es.start(ec);

        String ddl1 = "CREATE DATABASE x VERSION '1';"
                + "USE DATABASE x VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2;"
                + "CREATE ROLE r with any authenticated;\n"
                + "SET SCHEMA test2;"
                + "CREATE VIEW x as select 1 as col;"
                + "GRANT select on table test2.x to r;";

        es.getAdmin().deploy("x-vdb.ddl", new ByteArrayInputStream(ddl1.getBytes("UTF-8")));

        String ddl2 = "CREATE DATABASE x2 VERSION '1';"
                + "USE DATABASE x2 VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2;"
                + "CREATE ROLE r with any authenticated;\n"
                + "SET SCHEMA test2;"
                + "CREATE VIEW x as select 1 as col;"
                + "GRANT select on table test2.y to r;";

        try {
            es.getAdmin().deploy("x2-vdb.ddl", new ByteArrayInputStream(ddl2.getBytes("UTF-8")));
            fail();
        } catch (AdminProcessingException e) {

        }
    }

    @Test public void testGrantTargetSchema() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        es.start(ec);

        String ddl1 = "CREATE DATABASE x VERSION '1';"
                + "USE DATABASE x VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2;"
                + "CREATE ROLE r with any authenticated;\n"
                + "SET SCHEMA test2;"
                + "CREATE VIEW x as select 1 as col;"
                + "GRANT select on schema test2 to r;";

        es.getAdmin().deploy("x-vdb.ddl", new ByteArrayInputStream(ddl1.getBytes("UTF-8")));

        String ddl2 = "CREATE DATABASE x2 VERSION '1';"
                + "USE DATABASE x2 VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2;"
                + "CREATE ROLE r with any authenticated;\n"
                + "SET SCHEMA test2;"
                + "CREATE VIEW x as select 1 as col;"
                + "GRANT select on schema test3 to r;";

        try {
            es.getAdmin().deploy("x2-vdb.ddl", new ByteArrayInputStream(ddl2.getBytes("UTF-8")));
            fail();
        } catch (AdminProcessingException e) {

        }
    }

    @Test public void testGrantTargetColumn() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        es.start(ec);

        String ddl1 = "CREATE DATABASE x VERSION '1';"
                + "USE DATABASE x VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2;"
                + "CREATE ROLE r with any authenticated;\n"
                + "SET SCHEMA test2;"
                + "CREATE VIEW x as select 1 as col;"
                + "GRANT select on column test2.x.col to r;";

        es.getAdmin().deploy("x-vdb.ddl", new ByteArrayInputStream(ddl1.getBytes("UTF-8")));

        String ddl2 = "CREATE DATABASE x2 VERSION '1';"
                + "USE DATABASE x2 VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2;"
                + "CREATE ROLE r with any authenticated;\n"
                + "SET SCHEMA test2;"
                + "CREATE VIEW x as select 1 as col;"
                + "GRANT select on column test2.x.col2 to r;";

        try {
            es.getAdmin().deploy("x2-vdb.ddl", new ByteArrayInputStream(ddl2.getBytes("UTF-8")));
            fail();
        } catch (AdminProcessingException e) {

        }
    }

    @Test public void testGrantTargetProcedure() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        es.start(ec);

        String ddl1 = "CREATE DATABASE x VERSION '1';"
                + "USE DATABASE x VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2;"
                + "CREATE ROLE r with any authenticated;\n"
                + "SET SCHEMA test2;"
                + "CREATE VIRTUAL PROCEDURE x () as select 1;"
                + "GRANT select on procedure test2.x to r;";

        es.getAdmin().deploy("x-vdb.ddl", new ByteArrayInputStream(ddl1.getBytes("UTF-8")));

        String ddl2 = "CREATE DATABASE x2 VERSION '1';"
                + "USE DATABASE x2 VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2;"
                + "CREATE ROLE r with any authenticated;\n"
                + "SET SCHEMA test2;"
                + "CREATE VIRTUAL PROCEDURE x () as select 1;"
                + "GRANT select on procedure test2.y to r;";

        try {
            es.getAdmin().deploy("x2-vdb.ddl", new ByteArrayInputStream(ddl2.getBytes("UTF-8")));
            fail();
        } catch (AdminProcessingException e) {

        }
    }

    @Test public void testGrantTargetFunction() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        es.start(ec);

        String ddl1 = "CREATE DATABASE x VERSION '1';"
                + "USE DATABASE x VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2;"
                + "CREATE ROLE r with any authenticated;\n"
                + "SET SCHEMA test2;"
                + "CREATE VIRTUAL function x () returns integer as return 1;"
                + "GRANT select on function test2.x to r;";

        es.getAdmin().deploy("x-vdb.ddl", new ByteArrayInputStream(ddl1.getBytes("UTF-8")));

        String ddl2 = "CREATE DATABASE x2 VERSION '1';"
                + "USE DATABASE x2 VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2;"
                + "CREATE ROLE r with any authenticated;\n"
                + "SET SCHEMA test2;"
                + "CREATE VIRTUAL function x () returns integer as return 1;"
                + "GRANT select on function test2.y to r;";

        try {
            es.getAdmin().deploy("x2-vdb.ddl", new ByteArrayInputStream(ddl2.getBytes("UTF-8")));
            fail();
        } catch (AdminProcessingException e) {

        }
    }

}
