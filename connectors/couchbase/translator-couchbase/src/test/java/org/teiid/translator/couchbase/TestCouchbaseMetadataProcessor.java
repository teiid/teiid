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
package org.teiid.translator.couchbase;

import static org.junit.Assert.*;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.*;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.*;
import static org.teiid.translator.couchbase.CouchbaseProperties.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Ignore;
import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.translator.couchbase.CouchbaseMetadataProcessor.Dimension;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.document.json.JsonValue;

@SuppressWarnings({"nls",})
public class TestCouchbaseMetadataProcessor {

    static final String KEYSPACE = "test";
    static final String KEYSPACE_SOURCE = "`test`";

    @Test
    public void testCustomerOrder() throws Exception {
        CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        Table table = createTable(mf, KEYSPACE, KEYSPACE);
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, formCustomer(), mf, table, KEYSPACE, false, new Dimension());
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, formOder(), mf, table, KEYSPACE, false, new Dimension());
        helpTest("customerOrder.expected", mf);
    }

    @Test
    public void testCustomerOrderMultiple() throws Exception {
        CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        Table table = createTable(mf, KEYSPACE, KEYSPACE);
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, formCustomer(), mf, table, KEYSPACE, false, new Dimension());
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, formOder(), mf, table, KEYSPACE, false, new Dimension());
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, formCustomer(), mf, table, KEYSPACE, false, new Dimension());
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, formOder(), mf, table, KEYSPACE, false, new Dimension());
        helpTest("customerOrder.expected", mf);
    }

    @Test
    public void testCustomerOrderWithTypedName() throws Exception {
        CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();
        mp.setTypeNameList("`test`:`type`,`beer-sample`:`type`,` travel-sample`:`type`");
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        Table customer = createTable(mf, KEYSPACE, "Customer");
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, formCustomer(), mf, customer, customer.getName(), false, new Dimension());
        Table order = createTable(mf, KEYSPACE, "Oder");
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, formOder(), mf, order, order.getName(), false, new Dimension());
        helpTest("customerOrderTypedName.expected", mf);
    }

    @Test
    public void testCustomerWithDuplicatedTypedName() throws Exception {
        CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();
        mp.setTypeNameList("`test`:`type`,`test2`:`type`");
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        String typedName = "Customer";
        String keyspace1 = "test";
        String keyspace2 = "test2";
        Table table1 = createTable(mf, keyspace1, typedName);
        mp.scanRow("test", KEYSPACE_SOURCE, formCustomer(), mf, table1, table1.getName(), false, new Dimension());
        Table table2 = createTable(mf, keyspace2, typedName);
        mp.scanRow("test2", "`test2`", formCustomer(), mf, table2, table2.getName(), false, new Dimension());
        helpTest("customerDuplicatedTypedName.expected", mf);
    }

    @Test
    public void testMoreTypedNameInOneKeyspace() throws Exception {

        CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();
        mp.setTypeNameList("`test`:`type`,`test`:`name`,`test`:`category`");
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        Table ea = createTable(mf, KEYSPACE, "name", "ExampleA");
        mp.scanRow("test", KEYSPACE_SOURCE, JsonObject.create().put("name", "ExampleA"), mf, ea, ea.getName(), false, new Dimension());
        Table eb = createTable(mf, KEYSPACE, "name", "ExampleB");
        mp.scanRow("test", KEYSPACE_SOURCE, JsonObject.create().put("name", "ExampleB"), mf, eb, eb.getName(), false, new Dimension());
        Table sa = createTable(mf, KEYSPACE, "type", "SampleA");
        mp.scanRow("test", KEYSPACE_SOURCE, JsonObject.create().put("type", "SampleA"), mf, sa, sa.getName(), false, new Dimension());
        Table sb = createTable(mf, KEYSPACE, "type", "SampleB");
        mp.scanRow("test", KEYSPACE_SOURCE, JsonObject.create().put("type", "SampleB"), mf, sb, sb.getName(), false, new Dimension());
        Table qa = createTable(mf, KEYSPACE, "category", "QuickstartA");
        mp.scanRow("test", KEYSPACE_SOURCE, JsonObject.create().put("category", "QuickstartA"), mf, qa, qa.getName(), false, new Dimension());
        Table qb = createTable(mf, KEYSPACE, "type", "QuickstartB");
        mp.scanRow("test", KEYSPACE_SOURCE, JsonObject.create().put("type", "QuickstartB").put("name", "SampleC").put("category", "ExampleC"), mf, qb, qb.getName(), false, new Dimension());
        helpTest("moreTypedNameInOneKeyspace.expected", mf);
    }

    @Test
    public void testNullValue() throws Exception {
        CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        Table table = createTable(mf, KEYSPACE, KEYSPACE);
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, formNullValueJson(), mf, table, KEYSPACE, false, new Dimension());
        helpTest("nullValue.expected", mf);
    }


    @Test
    public void testDataType() throws Exception {

        /* 10 potential types: String, Integer, Long, Double, Boolean, BigInteger, BigDecimal, JsonObject, JsonArray, null */

        CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        Table table = createTable(mf, KEYSPACE, KEYSPACE);
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, formDataTypeJson(), mf, table, KEYSPACE, false, new Dimension());
        helpTest("dataTypeJson.expected", mf);
    }

    @Test
    public void testNestedJson() throws Exception {

        CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        Table table = createTable(mf, KEYSPACE, KEYSPACE);
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, nestedJson(), mf, table, KEYSPACE, false, new Dimension());
        helpTest("nestedJson.expected", mf);
    }

    @Test
    public void testNestedJsonWithTypedName() throws Exception {

        CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        Table table = createTable(mf, KEYSPACE, "Sample");
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, nestedJson(), mf, table, "Sample", false, new Dimension());
        helpTest("nestedJsonTypedName.expected", mf);
    }

    @Test
    public void testNestedArray() throws Exception {

        CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        Table table = createTable(mf, KEYSPACE, KEYSPACE);
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, nestedArray(), mf, table, KEYSPACE, false, new Dimension());
        helpTest("nestedArray.expected", mf);
    }

    @Test
    public void testComplexJson() throws Exception {

        CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        Table table = createTable(mf, KEYSPACE, KEYSPACE);
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, complexJson(), mf, table, KEYSPACE, false, new Dimension());
        helpTest("complexJson.expected", mf);
    }

    @Test
    public void testJsonNestedArray() throws Exception {

        CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        Table table = createTable(mf, KEYSPACE, KEYSPACE);
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, complexJsonNestedArray(), mf, table, KEYSPACE, false, new Dimension());
        helpTest("complexJsonNestedArray.expected", mf);
    }

    @Ignore("not resolved so far")
    @Test
    public void testMetadataCaseSensitive() throws Exception {

        CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        JsonObject json = JsonObject.create()
                .put("name", "value")
                .put("Name", "value")
                .put("nAmE", "value");
        Table table = createTable(mf, KEYSPACE, KEYSPACE);
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, json, mf, table, KEYSPACE, false, new Dimension());
        helpTest("TODO.expected", mf);
    }

    @Test
    public void testProcedures() throws Exception {

        CouchbaseMetadataProcessor metadataProcessor = new CouchbaseMetadataProcessor();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        metadataProcessor.addProcedures(mf, null);
        helpTest("procedures.expected", mf);
    }

    static Table createTable(MetadataFactory mf, String keyspace, String tableName) {
        return createTable(mf, keyspace, "type", tableName);
    }

    static Table createTable(MetadataFactory mf, String keyspace, String typedKey, String tableName) {
        if (mf.getSchema().getTable(tableName) != null && !tableName.equals(keyspace)) {
            tableName = keyspace + UNDERSCORE + tableName;
        }
        Table table = mf.addTable(tableName);
        table.setNameInSource(WAVE + keyspace + WAVE);
        table.setSupportsUpdate(true);
        table.setProperty(IS_ARRAY_TABLE, FALSE_VALUE);
        if(!tableName.equals(keyspace)){
            table.setProperty(NAMED_TYPE_PAIR, buildNamedTypePair("`" + typedKey + "`", tableName));
        }
        mf.addColumn(DOCUMENTID, STRING, table);
        mf.addPrimaryKey("PK0", Arrays.asList(DOCUMENTID), table); //$NON-NLS-1$
        return table;
    }

    private static String buildNamedTypePair(String columnIdentifierName, String typedValue) {
        StringBuilder sb = new StringBuilder();
        sb.append(columnIdentifierName).append(Tokens.COLON).append(Tokens.QUOTE).append(typedValue).append(Tokens.QUOTE);
        return sb.toString();
    }

    static JsonObject formCustomer() {
        return JsonObject.create()
                .put("Name", "John Doe")
                .put("ID", "Customer_101")
                .put("type", "Customer")
                .put("SavedAddresses", JsonArray.from("123 Main St.", "456 1st Ave"));
    }

    static JsonObject formOder() {
        return JsonObject.create()
                .put("Name", "Air Ticket")
                .put("type", "Oder")
                .put("CustomerID", "Customer_101")
                .put("CreditCard", JsonObject.create().put("Type", "Visa").put("CardNumber", "4111 1111 1111 111").put("Expiry", "12/12").put("CVN", 123))
                .put("Items", JsonArray.from(JsonObject.create().put("ItemID", 89123).put("Quantity", 1), JsonObject.create().put("ItemID", 92312).put("Quantity", 5)));
    }

    static JsonValue formNullValueJson() {
        return JsonObject.create()
                .put("Name", "null value test")
                .putNull("attr_null")
                .put("attr_obj", JsonObject.create().putNull("attr_null"))
                .put("attr_array", JsonArray.create().addNull());
    }

    static JsonObject formDataTypeJson() {
        return JsonObject.create()
                .put("Name", "data type test")
                .put("attr_string", "This is String value")
                .put("attr_integer", Integer.MAX_VALUE)
                .put("attr_long", Long.MAX_VALUE)
                .put("attr_double", Double.MAX_VALUE)
                .put("attr_boolean", Boolean.TRUE)
                .put("attr_bigInteger", new BigInteger("fffffffffffff", 16))
                .put("attr_bigDecimal", new BigDecimal("1115.37"))
                .put("attr_jsonObject", JsonObject.create().put("key", "value"))
                .put("attr_jsonArray", formDataTypeArray())
                .putNull("attr_null");
    }

    static JsonArray formDataTypeArray() {
        return JsonArray.create()
                .add("This is String value")
                .add(Integer.MAX_VALUE)
                .add(Long.MAX_VALUE)
                .add(Double.MAX_VALUE)
                .add(Boolean.TRUE)
                .add(new BigInteger("fffffffffffff", 16))
                .add(new BigDecimal("1115.37"))
                .add(JsonObject.create().put("key", "value"))
                .add(JsonArray.create().add("array"))
                .addNull();

    }


    static JsonObject nestedJson() {
        return JsonObject.create()
                .put("Name", "Nested Json")
                .put("nestedJson", JsonObject.create()
                        .put("Dimension", 1)
                        .put("nestedJson", JsonObject.create()
                                .put("Dimension", 2)
                                .put("nestedJson", JsonObject.create()
                                        .put("Dimension", 3)
                                        .put("nestedJson", "value"))));
    }

    static JsonObject nestedArray() {
        return JsonObject.create()
                .put("Name", "Nested Array")
                .put("nestedArray", JsonArray.create()
                        .add("dimension 1")
                        .add(JsonArray.create()
                                .add("dimension 2")
                                .add(JsonArray.create()
                                        .add("dimension 3")
                                        .add(JsonArray.create()
                                                .add("dimension 4")))));
    }

    static JsonObject complexJson() {
        return JsonObject.create()
                .put("Name", "Complex Json")
                .put("attr_jsonObject", JsonObject.create()
                        .put("Name", "Nested Json")
                        .put("attr_jsonArray", JsonArray.create()
                                .add("Nested array")
                                .add(JsonObject.create().put("Name", "Nested Json"))))
                .put("attr_jsonArray", JsonArray.create()
                        .add("Nested array")
                        .add(JsonObject.create().put("Name", "Nested Json"))
                        .add(JsonObject.create().put("Name", "Nested Json").put("Dimension", 1)));
    }

    static JsonValue complexJsonNestedArray() {
        return JsonObject.create()
                .put("Name", "Complex Json")
                .put("attr_jsonObject", JsonObject.create()
                        .put("Name", "Nested Json")
                        .put("attr_jsonArray", JsonArray.create()
                                .add("Nested array")
                                .add(JsonObject.create().put("Name", "Nested Json"))));
    }

    private static final boolean PRINT_TO_CONSOLE = false;
    private static final boolean REPLACE_EXPECTED = false;

    private void helpTest(String name, MetadataFactory mf) throws Exception {
        String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        if(PRINT_TO_CONSOLE) {
            System.out.println(metadataDDL);
        }
        Path path = Paths.get(UnitTestUtil.getTestDataPath(), name);
        if(REPLACE_EXPECTED) {
            Files.write(path, metadataDDL.getBytes("UTF-8"), StandardOpenOption.TRUNCATE_EXISTING);
        }
        String expected = new String(Files.readAllBytes(path));
        assertEquals(expected, metadataDDL);
    }

    @Test
    public void testTypeListParse() {

        Map<String, String> typeNameMap = new HashMap<>();
        String typeNameList = "`product`:`type`,`store`:`type`,`customer`:`jsonType`,`sales`:`type`";
        Pattern typeNamePattern = Pattern.compile(CouchbaseProperties.TPYENAME_MATCHER_PATTERN);
        Matcher typeGroupMatch = typeNamePattern.matcher(typeNameList);
        while (typeGroupMatch.find()) {
            typeNameMap.put(typeGroupMatch.group(1), typeGroupMatch.group(2));
        }
        assertTrue(typeNameMap.values().contains("`type`"));
        assertEquals("`type`", typeNameMap.get("`product`"));
    }

    @Test
    public void testTypeListParse_2() {

        Map<String, List<String>> typeNameMap = new HashMap<>();
        String typeNameList = "`test`:`type`,`test`:`name`,`test`:`category`,`default`:`type`";
        Pattern typeNamePattern = Pattern.compile(CouchbaseProperties.TPYENAME_MATCHER_PATTERN);
        Matcher typeGroupMatch = typeNamePattern.matcher(typeNameList);
        while (typeGroupMatch.find()) {
            String key = typeGroupMatch.group(1);
            String value = typeGroupMatch.group(2);
            if(typeNameMap.get(key) == null) {
                typeNameMap.put(key, new ArrayList<>(3));
            }
            typeNameMap.get(key).add(value);
        }
        assertEquals(typeNameMap.get("`test`").size(), 3);
        assertEquals(typeNameMap.get("`default`").size(), 1);
        assertTrue(typeNameMap.keySet().contains("`test`"));

    }
}
