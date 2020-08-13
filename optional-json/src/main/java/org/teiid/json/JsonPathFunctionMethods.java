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

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.BundleUtil;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.JsonType;
import org.teiid.core.util.ReaderInputStream;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.query.function.TeiidFunction;
import org.teiid.query.function.TeiidFunctions;
import org.teiid.query.function.metadata.FunctionCategoryConstants;
import org.teiid.translator.SourceSystemFunctions;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONAwareEx;
import net.minidev.json.JSONObject;

@TeiidFunctions(category=FunctionCategoryConstants.JSON)
public class JsonPathFunctionMethods {

    private static String ORDINAL = "ordinal"; //$NON-NLS-1$

    private static BundleUtil UTIL = BundleUtil.getBundleUtil(JsonPathFunctionMethods.class);

    @TeiidFunction(name = SourceSystemFunctions.JSONPATHVALUE, pushdown = PushDown.CAN_PUSHDOWN, nullOnNull = true)
    public static String jsonPathValue(Clob clob, String jsonPath, boolean nullLeaf)
            throws IOException, SQLException {
        JsonPath path = JsonPath.compile(jsonPath);
        Object result = jsonPathRead(clob.getCharacterStream(), path, nullLeaf);

        if (path.isDefinite()) {
            //single value
            return toString(result);
        }

        if (result instanceof JSONArray) {
            JSONArray list = (JSONArray)result;
            if (!list.isEmpty()) {
                return toString(list.get(0));
            }
            return null;
        }

        return (String)result;
    }

    private static String toString(Object result) {
        if (result == null) {
            return null;
        }
        if (result instanceof JSONAwareEx) {
            return ((JSONAwareEx)result).toJSONString();
        }
        if (result instanceof Map) {
            return JSONObject.toJSONString((Map)result);
        }
        return result.toString();
    }

    @TeiidFunction(name = SourceSystemFunctions.JSONPATHVALUE, pushdown = PushDown.CAN_PUSHDOWN, nullOnNull = true)
    public static String jsonPathValue(Clob clob, String jsonPath)
            throws IOException, SQLException {
        return jsonPathValue(clob, jsonPath, false);
    }

    @TeiidFunction(name = SourceSystemFunctions.JSONQUERY, pushdown = PushDown.CAN_PUSHDOWN, nullOnNull = true)
    public static JsonType jsonQuery(Clob clob, String jsonPath, boolean nullLeaf)
            throws IOException, SQLException {
        JsonPath path = JsonPath.compile(jsonPath);
        Object result = jsonPathRead(clob.getCharacterStream(), path, nullLeaf);
        if (result == null) {
            return null;
        }
        //TODO: could need intermediate storage for large json
        return new JsonType(new ClobImpl(toString(result)));
    }

    @TeiidFunction(name = SourceSystemFunctions.JSONQUERY, pushdown = PushDown.CAN_PUSHDOWN, nullOnNull = true)
    public static JsonType jsonQuery(Clob clob, String jsonPath)
            throws IOException, SQLException {
        return jsonQuery(clob, jsonPath, false);
    }

    @TeiidFunction(name = SourceSystemFunctions.JSONTOARRAY, pushdown = PushDown.CAN_PUSHDOWN, nullOnNull = true)
    public static Object[] jsonToArray(Clob clob, String jsonPath, boolean nullLeaf, String... colpaths)
            throws IOException, SQLException, FunctionExecutionException {

        JsonPath[] paths = new JsonPath[colpaths.length];

        for (int i = 0; i < colpaths.length; i++) {
            String path = colpaths[i];
            if (path == null || (!path.trim().startsWith("@") && !path.equalsIgnoreCase(ORDINAL))) { //$NON-NLS-1$
                throw new FunctionExecutionException(UTIL.gs("invalid_path")); //$NON-NLS-1$
            }
            if (!path.equalsIgnoreCase(ORDINAL)) {
                paths[i] = JsonPath.compile(path);
            }
        }

        JsonPath path = JsonPath.compile(jsonPath);
        Object result = jsonPathRead(clob.getCharacterStream(), path, nullLeaf);
        if (result == null) {
            return null;
        }

        Configuration conf = Configuration.defaultConfiguration();
        if (nullLeaf) {
            conf = conf.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
        }

        List<Object[]> rows = new ArrayList<>();

        if (result instanceof JSONArray) {
            JSONArray list = (JSONArray)result;

            int row = 1;

            for (int i = 0; i < list.size(); i++) {
                Object value = list.get(i);

                if (value == null) {
                    continue;
                }

                rows.add(buildRow(value, conf, paths, row++));
            }
        } else {
            rows.add(buildRow(result, conf, paths, 1));
        }

        return rows.toArray(new Object[rows.size()]);
    }

    private static Object[] buildRow(Object value, Configuration conf, JsonPath[] paths, int row) {
        DocumentContext dc = JsonPath.parse(value, conf);

        Object[] values = null;
        if (paths.length==0) {
            values = new Object[] {getTeiidValue(value)};
        } else {
            values = new Object[paths.length];
            for (int j = 0; j < paths.length; j++) {
                if (paths[j] == null) {
                    values[j] = row;
                } else {
                    Object colValue = dc.read(paths[j]);
                    values[j] = getTeiidValue(colValue);
                }
            }
        }

        return values;
    }

    private static Object getTeiidValue(Object colValue) {
        if (colValue instanceof JSONAwareEx) {
            return new JsonType(new ClobImpl(toString(colValue)));
        }
        return colValue;
    }

    static Object jsonPathRead(Reader jsonReader, JsonPath jsonPath, boolean nullLeaf) throws IOException {
        Configuration conf = Configuration.defaultConfiguration();
        if (nullLeaf) {
            conf = conf.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
        }

        try {
            return jsonPath.read(new ReaderInputStream(jsonReader, Charset.forName("UTF-8")), conf); //$NON-NLS-1$
        } finally {
            jsonReader.close();
        }
    }

}
