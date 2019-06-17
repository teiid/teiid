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
import static org.teiid.query.processor.TestProcessor.*;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import javax.sql.rowset.serial.SerialBlob;

import org.junit.Test;
import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.resolver.TestResolver;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestJSONProcessing {

    @Test public void testJSONParseBlob() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        String sql = "select jsonParse(cast(? as blob), false) x"; //$NON-NLS-1$
        String json = "{\"name\":123}";

        List<?>[] expected = new List[] {
                Arrays.asList(json),
        };

        processPreparedStatement(sql, expected, dataManager, new DefaultCapabilitiesFinder(), RealMetadataFactory.example1Cached(), Arrays.asList(new BlobType(new SerialBlob(json.getBytes(Charset.forName("UTF-16BE"))))));
    }

    @Test public void testJSONArray() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        String sql = "select jsonArray(1, null, true, {d '2007-01-01'}, jsonParse('{\"name\":123}', true), unescape('\\t\\n?'))"; //$NON-NLS-1$

        List<?>[] expected = new List[] {
                Arrays.asList("[1,null,true,\"2007-01-01\",{\"name\":123},\"\\t\\n?\"]"),
        };

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testJSONArray_Agg() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        String sql = "select jsonArray_agg(e1 order by e1 desc) from pm1.g1"; //$NON-NLS-1$

        List<?>[] expected = new List[] {
                Arrays.asList("[\"d\",\"a\",\"\\\"b\"]"),
        };

        dataManager.addData("SELECT pm1.g1.e1 FROM pm1.g1", new List<?>[] {Arrays.asList("a"), Arrays.asList("\"b"), Arrays.asList("d")});
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        assertEquals(DataTypeManager.DefaultDataClasses.JSON, ((Expression)plan.getOutputElements().get(0)).getType());
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testJSONObject() throws Exception {
        String sql = "select jsonObject(e1, e2, 1) from pm1.g1 order by e1 limit 1"; //$NON-NLS-1$

        List<?>[] expected = new List[] {
                Arrays.asList("{\"e1\":null,\"e2\":1,\"expr3\":1}"),
        };
        FakeDataManager fdm = new FakeDataManager();
        sampleData1(fdm);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        helpProcess(plan, fdm, expected);
    }

    @Test public void testJSONObjectWithNestedJson() throws Exception {
        String sql = "select jsonObject(jsonObject(e1), e2, jsonarray(e3, 2), 1) from pm1.g1 order by e1 limit 1"; //$NON-NLS-1$

        List<?>[] expected = new List[] {
                Arrays.asList("{\"expr1\":{\"e1\":null},\"e2\":1,\"expr3\":[false,2],\"expr4\":1}"),
        };
        FakeDataManager fdm = new FakeDataManager();
        sampleData1(fdm);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        helpProcess(plan, fdm, expected);
    }

    @Test public void testJSONObjectWithNestedJson1() throws Exception {
        String sql = "select jsonObject(jsonObject(e2, 1, jsonObject((select jsonArray_agg(e2) from pm1.g2 where e1 = pm1.g1.e1), e1))) from pm1.g1 order by e1 nulls last limit 1"; //$NON-NLS-1$

        List<?>[] expected = new List[] {
                Arrays.asList("{\"expr1\":{\"e2\":0,\"expr2\":1,\"expr3\":{\"expr1\":[0,3,0],\"e1\":\"a\"}}}"),
        };
        FakeDataManager fdm = new FakeDataManager();
        fdm.setBlockOnce();
        sampleData1(fdm);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

          helpProcess(plan, fdm, expected);
    }

    @Test public void testJSONCasts() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create view json as select jsonParse('{}', true) as col", "x", "y");

        String sql = "select cast(jsonObject(col) as string), cast(cast(jsonParse(col, true) as clob) as string) from json"; //$NON-NLS-1$

        //note in the first results contains the nested json, not a nested string
        List<?>[] expected = new List[] {
                Arrays.asList("{\"col\":{}}", "{}"),
        };
        HardcodedDataManager hdm = new HardcodedDataManager();
        ProcessorPlan plan = helpGetPlan(sql, tm);
        helpProcess(plan, hdm, expected);

        //small breaking potential - if we explicitly expect clob results
        //tm = RealMetadataFactory.fromDDL("create view json (col clob) as select jsonParse('{}', true) as col", "x", "y");
    }

    @Test public void testJSONResolving() throws Exception {
        String sql = "select jsonArray(1, null, true, {d '2007-01-01'}), jsonParse('{\\\"name\\\":123}', true), jsonObject(1 as expr)"; //$NON-NLS-1$

        Command command = TestResolver.helpResolve(sql, RealMetadataFactory.example1Cached());
        command.getProjectedSymbols().stream().forEach(e -> assertEquals(
                DataTypeManager.DefaultDataClasses.JSON, e.getType()));
    }

    @Test public void testCastStringToJson() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        String sql = "select cast('{\"name\":123}' as json)"; //$NON-NLS-1$
        String json = "{\"name\":123}";

        List<?>[] expected = new List[] {
                Arrays.asList(json),
        };

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        helpProcess(plan, dataManager, expected);
    }

    @Test(expected=ExpressionEvaluationException.class) public void testCastStringToJsonFails() throws Exception {
        String sql = "select convert('{\"name\":?}', json)"; //$NON-NLS-1$

        TestProcessor.helpGetPlan(
                TestOptimizer.helpGetCommand(sql,
                        RealMetadataFactory.exampleBQTCached()),
                RealMetadataFactory.exampleBQTCached(),
                new DefaultCapabilitiesFinder(),
                TestProcessor.createCommandContext());
    }

}
