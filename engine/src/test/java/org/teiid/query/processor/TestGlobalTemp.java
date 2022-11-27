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

package org.teiid.query.processor;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestGlobalTemp {

    @Test public void testGlobalTempUse() throws Exception {
        TempTableTestHarness harness = new TempTableTestHarness();
        TransformationMetadata metadata = RealMetadataFactory.fromDDL("create global temporary table temp (x serial, s string) options (updatable true);" +
                "", "x", "y");
        HardcodedDataManager dm = new HardcodedDataManager();
        harness.setUp(metadata, dm);

        harness.execute("select * from temp", new List<?>[0]);
        harness.execute("insert into temp (s) values ('a')", new List<?>[] {Arrays.asList(1)});
        harness.execute("select * from temp", new List<?>[] {Arrays.asList(1, "a")});
        try {
            harness.execute("drop table temp", new List<?>[0]);
            fail();
        } catch (QueryValidatorException e) {
        }
    }

    @Test public void testInsertCreation() throws Exception {
        TempTableTestHarness harness = new TempTableTestHarness();
        TransformationMetadata metadata = RealMetadataFactory.fromDDL("create global temporary table temp (x serial, s string) options (updatable true);" +
                "", "x", "y");
        HardcodedDataManager dm = new HardcodedDataManager();
        harness.setUp(metadata, dm);

        harness.execute("insert into temp (s) values ('a')", new List<?>[] {Arrays.asList(1)});
        harness.execute("select * from temp", new List<?>[] {Arrays.asList(1, "a")});
    }

    @Test public void testPkInitialUse() throws Exception {
        TempTableTestHarness harness = new TempTableTestHarness();
        TransformationMetadata metadata = RealMetadataFactory.fromDDL("create global temporary table temp (x serial, s string primary key) options (updatable true);" +
                "", "x", "y");
        HardcodedDataManager dm = new HardcodedDataManager();
        harness.setUp(metadata, dm);

        harness.execute("select * from temp", new List<?>[] {});
    }

    @Test public void testInsertMultipleValues() throws Exception {
        TempTableTestHarness harness = new TempTableTestHarness();
        TransformationMetadata metadata = RealMetadataFactory.fromDDL("CREATE GLOBAL TEMPORARY TABLE tabglob (id integer PRIMARY KEY, name string) OPTIONS (UPDATABLE 'TRUE');" +
                "", "x", "y");
        HardcodedDataManager dm = new HardcodedDataManager();
        harness.setUp(metadata, dm);

        harness.execute("INSERT INTO tabglob (id, name) VALUES (1, 'name1'), (2, 'name2')", new List<?>[] {Arrays.asList(2)});
        harness.execute("select count(*) from tabglob", new List<?>[] {Arrays.asList(2)});
    }

    @Test public void testTempInFunctionAndProcedure() throws Exception {
        String ddl = "CREATE GLOBAL TEMPORARY TABLE teiidtemp(val integer) OPTIONS (UPDATABLE 'TRUE');\n" +
                " \n" +
                "            CREATE VIRTUAL FUNCTION f1() RETURNS string AS\n" +
                "            BEGIN\n" +
                "                INSERT INTO teiidtemp(val) VALUES (1);\n" +
                "                DECLARE string v1 = SELECT 'default'||COUNT(val) FROM teiidtemp;\n" +
                "                RETURN v1;\n" +
                "            END;\n" +
                "        CREATE VIRTUAL PROCEDURE p1() RETURNS (v1 string) AS\n" +
                "            BEGIN\n" +
                "                INSERT INTO teiidtemp(val) VALUES (1);\n" +
                "                SELECT 'default'||COUNT(val) FROM teiidtemp;\n" +
                "            END;";

        TempTableTestHarness harness = new TempTableTestHarness();
        TransformationMetadata metadata = RealMetadataFactory.fromDDL(ddl, "x", "y");
        HardcodedDataManager dm = new HardcodedDataManager();
        harness.setUp(metadata, dm);
        harness.execute("INSERT INTO teiidtemp(val) VALUES (1)", new List<?>[] {Arrays.asList(1)});
        harness.execute("SELECT f1()", new List<?>[] {Arrays.asList("default2")});
        harness.execute("SELECT a.v1 FROM (CALL p1()) a", new List<?>[] {Arrays.asList("default3")});
    }

}
