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

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.translator.BaseDelegatingExecutionFactory;

@SuppressWarnings("nls")
public class TestEmbeddedServerCaching {
    EmbeddedServer es;

    @Before public void setup() {
        es = new EmbeddedServer();
    }

    @After public void teardown() {
        if (es != null) {
            es.stop();
        }
    }


    @Test public void testDelegatingCaching() throws Exception {
        es.start(new EmbeddedConfiguration());
        HardCodedExecutionFactory hcef = new HardCodedExecutionFactory();
        hcef.addData("SELECT pm1.g1.e1 FROM pm1.g1", Arrays.asList(Arrays.asList("a")));
        es.addTranslator("hc1", hcef);
        es.addTranslator(BaseDelegatingExecutionFactory.class);
        Map<String, String> properties = new HashMap<>();
        properties.put("delegateName", "hc1");
        properties.put("cachePattern", ".*");
        properties.put("cacheTtl", "5000");
        es.addTranslator("x", "delegator", properties);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("my-schema");
        mmd.addSourceMapping("x", "x", null);
        mmd.addSourceMetadata("ddl", "create foreign table \"pm1.g1\" (e1 string)");

        es.deployVDB("test", mmd);

        Connection c = es.getDriver().connect("jdbc:teiid:test", null);

        Statement s = c.createStatement();
        s.executeQuery("select * from pm1.g1");

        s.executeQuery("select * from pm1.g1");

        assertEquals(1, hcef.getCommands().size());
    }

}
