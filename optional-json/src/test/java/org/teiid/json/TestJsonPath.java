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

import static org.junit.Assert.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
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
}
