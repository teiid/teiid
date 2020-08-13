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

package org.teiid.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.JsonType;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.rewriter.TestQueryRewriter;
import org.teiid.query.unittest.RealMetadataFactory;

import com.jayway.jsonpath.PathNotFoundException;

@SuppressWarnings("nls")
public class TestJsonPath {

    private static String EXAMPLE;
    static {
        try {
            EXAMPLE = ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("example.json"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test public void testJsonPathValueNonascii() throws Exception {
        String result = JsonPathFunctionMethods.jsonPathValue(new ClobImpl(EXAMPLE), "$.owner", false);
        assertEquals("Günter Grass", result);
    }

    @Test public void testJsonPathValueDefiniteArray() throws Exception {
        String result = JsonPathFunctionMethods.jsonPathValue(new ClobImpl(EXAMPLE), "$.store.book", false);
        assertEquals("[{\"category\":\"reference\",\"author\":\"Nigel Rees\",\"title\":\"Sayings of the Century\",\"price\":8.95},{\"category\":\"fiction\",\"author\":\"Evelyn Waugh\",\"title\":\"Sword of Honour\",\"price\":12.99},{\"category\":\"fiction\",\"author\":\"Herman Melville\",\"title\":\"Moby Dick\",\"isbn\":\"0-553-21311-3\",\"price\":8.99},{\"category\":\"fiction\",\"author\":\"J. R. R. Tolkien\",\"title\":\"The Lord of the Rings\",\"isbn\":\"0-395-19395-8\",\"price\":22.99}]", result);
    }

    @Test public void testJsonPathValueDefiniteSingle() throws Exception {
        String result = JsonPathFunctionMethods.jsonPathValue(new ClobImpl(EXAMPLE), "$.store.book[1]", false);
        assertEquals("{\"category\":\"fiction\",\"author\":\"Evelyn Waugh\",\"title\":\"Sword of Honour\",\"price\":12.99}", result);
    }

    @Test public void testJsonPathValueIndefiniteArray() throws Exception {
        String result = JsonPathFunctionMethods.jsonPathValue(new ClobImpl(EXAMPLE), "$..author", false);
        assertEquals("Nigel Rees", result);
    }

    @Test public void testJsonPathValueIndefiniteNonExistent() throws Exception {
        String result = JsonPathFunctionMethods.jsonPathValue(new ClobImpl(EXAMPLE), "$..mountains", false);
        assertNull(result);
    }

    @Test(expected=PathNotFoundException.class) public void testJsonPathValueDefiniteNonExistent() throws Exception {
        JsonPathFunctionMethods.jsonPathValue(new ClobImpl(EXAMPLE), "$.mountains", false);
    }

    @Test public void testJsonPathValueDefiniteNonExistent1() throws Exception {
        String result = JsonPathFunctionMethods.jsonPathValue(new ClobImpl(EXAMPLE), "$.mountains", true);
        assertNull(result);
    }

    @Test public void testJsonQuery() throws Exception {
        JsonType result = JsonPathFunctionMethods.jsonQuery(new ClobImpl(EXAMPLE), "$..author", false);
        assertEquals("[\"Nigel Rees\",\"Evelyn Waugh\",\"Herman Melville\",\"J. R. R. Tolkien\"]", ClobType.getString(result));
    }

    @Test public void testJsonQueryNullLeaf() throws Exception {
        JsonType result = JsonPathFunctionMethods.jsonQuery(new ClobImpl(EXAMPLE), "$..author", true);
        assertEquals("[null,null,\"Nigel Rees\",\"Evelyn Waugh\",\"Herman Melville\",\"J. R. R. Tolkien\",null]", ClobType.getString(result));
    }

    @Test public void testJsonToArray() throws Exception {
        Object[] result = JsonPathFunctionMethods.jsonToArray(new ClobImpl(EXAMPLE), "$.store.book", true, "@.author", "@.type");
        assertEquals("[[Nigel Rees, null], [Evelyn Waugh, null], [Herman Melville, null], [J. R. R. Tolkien, null]]", Arrays.deepToString(result));
    }

    @Test(expected=FunctionExecutionException.class) public void testJsonToArrayInvalidPath() throws Exception {
        JsonPathFunctionMethods.jsonToArray(new ClobImpl(EXAMPLE), "$.store.book", true, ".author", "@.type");
    }

    @Test public void testFullSelect() {
        ProcessorPlan plan = TestProcessor.helpGetPlan("select * from arraytable(rows jsontoarray('"+EXAMPLE+"', '$..book[0:2]', false, '@.author', '@.title', '@.price') columns author string, title string, price decimal) as x", RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());

        List<?>[] expected = new List[] {
                Arrays.asList("Nigel Rees","Sayings of the Century", BigDecimal.valueOf(8.95)),
                Arrays.asList("Evelyn Waugh","Sword of Honour", BigDecimal.valueOf(12.99))
        };

        TestProcessor.helpProcess(plan, new HardcodedDataManager(), expected);
    }

    @Test public void testJsonTableRewrite() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("SELECT * from jsontable('{\"1\":{\"2\":3}}', '$[''1'']', true columns \"2\" integer) as x", //$NON-NLS-1$
                                "SELECT x.\"2\" FROM ARRAYTABLE(ROWS jsontoarray('{\"1\":{\"2\":3}}', '$[''1'']', TRUE, '@[''2'']') COLUMNS \"2\" integer) AS x", RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testJsonTableRewrite1() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("SELECT * from jsontable('{\"1\":{\"2\":3}}', '$..*' columns a integer path '@.x', b string) as x", //$NON-NLS-1$
                                "SELECT x.a, x.b FROM ARRAYTABLE(ROWS jsontoarray('{\"1\":{\"2\":3}}', '$..*', FALSE, '@.x', '@[''b'']') COLUMNS a integer, b string) AS x", RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testJsonTableProcessing() throws Exception {
        ProcessorPlan plan = TestProcessor.helpGetPlan("select * from jsontable('"+EXAMPLE+"', '$..book.*' columns author string, title string, price decimal) as x", RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());

        List<?>[] expected = new List[] {
                Arrays.asList("Nigel Rees","Sayings of the Century", BigDecimal.valueOf(8.95)),
                Arrays.asList("Evelyn Waugh","Sword of Honour", BigDecimal.valueOf(12.99)),
                Arrays.asList("Herman Melville","Moby Dick", BigDecimal.valueOf(8.99)),
                Arrays.asList("J. R. R. Tolkien","The Lord of the Rings", BigDecimal.valueOf(22.99))
        };

        TestProcessor.helpProcess(plan, new HardcodedDataManager(), expected);
    }

    @Test public void testJsonTableProcessingMissing() throws Exception {
        ProcessorPlan plan = TestProcessor.helpGetPlan("select * from jsontable('"+EXAMPLE+"', '$..book[1:3]', true columns id for ordinality, isbn string, title string, price decimal) as x", RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());

        List<?>[] expected = new List[] {
                Arrays.asList(1, null,"Sword of Honour", BigDecimal.valueOf(12.99)),
                Arrays.asList(2, "0-553-21311-3","Moby Dick", BigDecimal.valueOf(8.99)),
        };

        TestProcessor.helpProcess(plan, new HardcodedDataManager(), expected);
    }

    @Test public void testJsonTableProcessingCorrelated() throws Exception {
        ProcessorPlan plan = TestProcessor.helpGetPlan("select x.* from pm1.g1, jsontable(e1, '$..book[1:3]', true columns id for ordinality, isbn string, title string, price decimal) as x", RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());

        List<?>[] expected = new List[] {
                Arrays.asList(1, null,"Sword of Honour", BigDecimal.valueOf(12.99)),
                Arrays.asList(2, "0-553-21311-3","Moby Dick", BigDecimal.valueOf(8.99)),
        };

        HardcodedDataManager hdm = new HardcodedDataManager();
        //the null row will be ignored
        hdm.addData("SELECT g_0.e1 FROM pm1.g1 AS g_0", Arrays.asList(EXAMPLE), Collections.singletonList(null));

        TestProcessor.helpProcess(plan, hdm, expected);
    }

    @Test public void testRootObject() throws Exception {
        String sql = "SELECT j.id, j.name, j.status FROM JSONTABLE(cast('{\n" +
                "  \"id\": 5,\n" +
                "  \"name\": \"carly\",\n" +
                "  \"status\": \"sold\"\n" +
                "}' as json), '$', false COLUMNS id integer, name string, status string) as j";

        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());

        List<?>[] expected = new List[] {
                Arrays.asList(5, "carly", "sold"),
        };

        HardcodedDataManager hdm = new HardcodedDataManager();

        TestProcessor.helpProcess(plan, hdm, expected);
    }

}
