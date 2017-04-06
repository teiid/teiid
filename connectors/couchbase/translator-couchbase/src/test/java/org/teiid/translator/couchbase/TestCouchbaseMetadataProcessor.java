/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.translator.couchbase;

import static org.junit.Assert.*;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.STRING;
import static org.teiid.translator.couchbase.CouchbaseProperties.COLON;
import static org.teiid.translator.couchbase.CouchbaseProperties.DOCUMENTID;
import static org.teiid.translator.couchbase.CouchbaseProperties.FALSE_VALUE;
import static org.teiid.translator.couchbase.CouchbaseProperties.QUOTE;
import static org.teiid.translator.couchbase.CouchbaseProperties.UNDERSCORE;
import static org.teiid.translator.couchbase.CouchbaseProperties.WAVE;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.IS_ARRAY_TABLE;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.NAMED_TYPE_PAIR;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.resource.ResourceException;

import org.junit.Ignore;
import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
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
    public void testCustomerOrder() throws ResourceException {
        CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();  
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        Table table = createTable(mf, KEYSPACE, KEYSPACE);
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, formCustomer(), mf, table, KEYSPACE, false, new Dimension());
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, formOder(), mf, table, KEYSPACE, false, new Dimension());
        helpTest("customerOrder.expected", mf);
    }
    
    @Test
    public void testCustomerOrderMultiple() throws ResourceException {
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
    public void testCustomerOrderWithTypedName() throws ResourceException {
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
    public void testCustomerWithDuplicatedTypedName() throws ResourceException {
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
    public void testNullValue() throws ResourceException {
        CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();  
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        Table table = createTable(mf, KEYSPACE, KEYSPACE);
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, formNullValueJson(), mf, table, KEYSPACE, false, new Dimension());
        helpTest("nullValue.expected", mf);
    }
    

    @Test
    public void testDataType() throws ResourceException {
        
        /* 10 potential types: String, Integer, Long, Double, Boolean, BigInteger, BigDecimal, JsonObject, JsonArray, null */
        
        CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();  
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        Table table = createTable(mf, KEYSPACE, KEYSPACE);
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, formDataTypeJson(), mf, table, KEYSPACE, false, new Dimension());
        helpTest("dataTypeJson.expected", mf);
    }
    
    @Test
    public void testNestedJson() throws ResourceException {
        
        CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();  
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        Table table = createTable(mf, KEYSPACE, KEYSPACE);
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, nestedJson(), mf, table, KEYSPACE, false, new Dimension());
        helpTest("nestedJson.expected", mf);
    }
    
    @Test
    public void testNestedJsonWithTypedName() throws ResourceException {
        
        CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();  
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        Table table = createTable(mf, KEYSPACE, "Sample");
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, nestedJson(), mf, table, "Sample", false, new Dimension());
        helpTest("nestedJsonTypedName.expected", mf);
    }
    
    @Test
    public void testNestedArray() throws ResourceException {
        
        CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();  
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        Table table = createTable(mf, KEYSPACE, KEYSPACE);
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, nestedArray(), mf, table, KEYSPACE, false, new Dimension());
        helpTest("nestedArray.expected", mf);
    }

    @Test
    public void testComplexJson() throws ResourceException {
        
        CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();  
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        Table table = createTable(mf, KEYSPACE, KEYSPACE);
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, complexJson(), mf, table, KEYSPACE, false, new Dimension());
        helpTest("complexJson.expected", mf);
    }
    
    @Test
    public void testJsonNestedArray() throws ResourceException {
        
        CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();  
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        Table table = createTable(mf, KEYSPACE, KEYSPACE);
        mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, complexJsonNestedArray(), mf, table, KEYSPACE, false, new Dimension());
        helpTest("complexJsonNestedArray.expected", mf);
    }

    @Ignore("not resolved so far")
    @Test
    public void testMetadataCaseSensitive() throws ResourceException {
        
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
    public void testProcedures() throws ResourceException {
        
        CouchbaseMetadataProcessor metadataProcessor = new CouchbaseMetadataProcessor();  
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        metadataProcessor.addProcedures(mf, null);
        helpTest("procedures.expected", mf);
    }
    
    static Table createTable(MetadataFactory mf, String keyspace, String tableName) {
        if (mf.getSchema().getTable(tableName) != null && !tableName.equals(keyspace)) { 
            tableName = keyspace + UNDERSCORE + tableName;
        }
        Table table = mf.addTable(tableName);
        table.setNameInSource(WAVE + keyspace + WAVE);
        table.setSupportsUpdate(true);
        table.setProperty(IS_ARRAY_TABLE, FALSE_VALUE);
        if(!tableName.equals(keyspace)){
            table.setProperty(NAMED_TYPE_PAIR, buildNamedTypePair("`type`", tableName));
        }
        mf.addColumn(DOCUMENTID, STRING, table);
        mf.addPrimaryKey("PK0", Arrays.asList(DOCUMENTID), table); //$NON-NLS-1$
        return table;
    }
    
    private static String buildNamedTypePair(String columnIdentifierName, String typedValue) {
        StringBuilder sb = new StringBuilder();
        sb.append(columnIdentifierName).append(COLON).append(QUOTE).append(typedValue).append(QUOTE); 
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
    
    private void helpTest(String name, MetadataFactory mf) throws ResourceException {
        try {
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
        } catch (IOException e) {
            throw new ResourceException(e);
        }
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
}
