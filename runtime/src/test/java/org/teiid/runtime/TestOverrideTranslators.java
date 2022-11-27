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

import java.io.ByteArrayInputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@SuppressWarnings("nls")
public class TestOverrideTranslators {
    EmbeddedServer es;

    @Before public void setup() {
        es = new EmbeddedServer() {
            @Override
            protected boolean allowOverrideTranslators() {
                return true;
            }
        };
    }

    @After public void teardown() {
        if (es != null) {
            es.stop();
        }
    }

    @Ignore("the handling for this is in teiid spring boot")
    @Test public void testOverrideTranslator() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        es.start(ec);
        es.addTranslator(TestEmbeddedServer.DummyExecutionFactory.class);
        String ddl = "CREATE DATABASE db;\n" +
                "USE DATABASE db;\n" +
                "CREATE FOREIGN DATA WRAPPER myloopback handler dummy OPTIONS (supportsOrderBy true);\n" +
                "CREATE SERVER salesforce FOREIGN DATA WRAPPER myloopback;\n" +
                "CREATE SCHEMA sf SERVER salesforce;\n" +
                "IMPORT FROM SERVER salesforce INTO sf OPTIONS(\"importer.useFullSchemaName\" 'false');";

        es.getAdmin().deploy("x-vdb.ddl", new ByteArrayInputStream(ddl.getBytes("UTF-8")));
    }

    @Test public void testImplicitTranslatorReference() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        es.start(ec);
        es.addTranslator(TestEmbeddedServer.DummyExecutionFactory.class);
        String ddl = "CREATE DATABASE db;\n" +
                "USE DATABASE db;\n" +
                "CREATE SERVER salesforce FOREIGN DATA WRAPPER dummy;\n" +
                "CREATE SCHEMA sf SERVER salesforce;\n" +
                "IMPORT FROM SERVER salesforce INTO sf OPTIONS(\"importer.useFullSchemaName\" 'false');";

        es.getAdmin().deploy("x-vdb.ddl", new ByteArrayInputStream(ddl.getBytes("UTF-8")));
    }

}
