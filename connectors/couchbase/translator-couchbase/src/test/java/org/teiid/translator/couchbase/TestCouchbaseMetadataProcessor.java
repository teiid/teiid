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

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import javax.resource.ResourceException;

import org.junit.Ignore;
import org.junit.Test;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.SystemMetadata;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

@SuppressWarnings("nls")
public class TestCouchbaseMetadataProcessor {
 
    @Test
    public void testMetadata() throws ResourceException {
        
        CouchbaseMetadataProcessor metadataProcessor = new CouchbaseMetadataProcessor();  
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        // Two documents under keyspace
        String keyspace = "testKeyspace";
        metadataProcessor.addTable(mf, keyspace, formCustomer(), null);
        metadataProcessor.addTable(mf, keyspace, formOder(), null);
        helpTest("customerOrder.expected", mf);
    }
    
    @Test
    public void testMetadataMultiple() throws ResourceException {
        
        CouchbaseMetadataProcessor metadataProcessor = new CouchbaseMetadataProcessor();  
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        // multiple documents with same structure under keyspace
        String keyspace = "testKeyspace";
        metadataProcessor.addTable(mf, keyspace, formCustomer(), null);
        metadataProcessor.addTable(mf, keyspace, formCustomer(), null);
        metadataProcessor.addTable(mf, keyspace, formOder(), null);
        metadataProcessor.addTable(mf, keyspace, formOder(), null);
        helpTest("customerOrder.expected", mf);
    }
    
    @Test
    public void testMetadataComplex() throws ResourceException {
        
        CouchbaseMetadataProcessor metadataProcessor = new CouchbaseMetadataProcessor();  
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        String keyspace = "A";
        JsonObject json = JsonObject.create().put("A", JsonObject.create().put("A", JsonObject.create().put("A", "value")));
        metadataProcessor.addTable(mf, keyspace, json, null);
        helpTest("complexJson.expected", mf);
    }
    
    @SuppressWarnings("static-access")
    @Test
    public void testMetadataComplexArray() throws ResourceException {
        CouchbaseMetadataProcessor metadataProcessor = new CouchbaseMetadataProcessor();  
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        String keyspace = "A";
        JsonObject json = JsonObject.create().put("A", JsonArray.create().from("A",  JsonArray.create().from("A", JsonArray.create().add("A"))));
        metadataProcessor.addTable(mf, keyspace, json, null);
        helpTest("complexJsonArray.expected", mf);
    }
    
    @Test
    public void testMetadataDataTypeSimpleJson() throws ResourceException {
        
        CouchbaseMetadataProcessor metadataProcessor = new CouchbaseMetadataProcessor();  
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        String keyspace = "testKeyspace";
        metadataProcessor.addTable(mf, keyspace, formSimpleJson(), null);
        helpTest("simpleJson.expected", mf);
    }
    
    @Test
    public void testMetadataDataTypeCompleteJson() throws ResourceException {
        
        CouchbaseMetadataProcessor metadataProcessor = new CouchbaseMetadataProcessor();  
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        String keyspace = "testKeyspace";
        metadataProcessor.addTable(mf, keyspace, formJson(), null);
        helpTest("completeJson.expected", mf);
    }
    
    @Ignore("not resolved so far")
    @Test
    public void testMetadataCaseSensitive() throws ResourceException {
        
        CouchbaseMetadataProcessor metadataProcessor = new CouchbaseMetadataProcessor();  
        MetadataFactory mf = new MetadataFactory("vdb", 1, "couchbase", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        String keyspace = "testKeyspace";
        JsonObject json = JsonObject.create()
                .put("name", "value")
                .put("Name", "value")
                .put("nAmE", "value");
        metadataProcessor.addTable(mf, keyspace, json, null);
        helpTest("TODO.expected", mf);
    }
    
    private JsonObject formCustomer() {
        return JsonObject.create()
                .put("Type", "Customer")
                .put("Name", "John Doe")
                .put("SavedAddresses", JsonArray.from("123 Main St.", "456 1st Ave"));
    }
    
    private JsonObject formOder() {
        return JsonObject.create()
                .put("Type", "Oder")
                .put("CustomerID", "Customer_12345")
                .put("CreditCard", JsonObject.create().put("Type", "Visa").put("CardNumber", "4111 1111 1111 111").put("Expiry", "12/12").put("CVN", 123))
                .put("Items", JsonArray.from(JsonObject.create().put("ItemID", 89123).put("Quantity", 1), JsonObject.create().put("ItemID", 92312).put("Quantity", 5)));
    }
    
    private JsonArray formSimpleJsonArray() {
        return JsonArray.create()
                .add(true)
                .add(0.0d)
                .add(1)
                .add(10000L)
                .add(new Byte((byte)1))
                .add(new Short((short) 20))
                .add(new Integer(1))
                .add(new Long(10000L))
                .add(new Float(50.123))
                .add(new Double(60.123))
                .add("This is String value")
                .addNull();
    }
    
    private JsonObject formJson() {
        return formSimpleJson()
                .put("attr_jsonObject", formSimpleJson())
                .put("attr_jsonArray", formSimpleJsonArray());
    }

    private JsonObject formSimpleJson() {
        return JsonObject.create()
                .put("attr_boolean", true)
                .put("attr_double", 0.0d)
                .put("attr_int", 1)
                .put("attr_long", 10000L)
                .put("attr_number_byte", new Byte((byte)1))
                .put("attr_number_short", new Short((short) 20))
                .put("attr_number_integer", new Integer(1))
                .put("attr_number_long", new Long(10000L))
                .put("attr_number_float", new Float(50.123))
                .put("attr_number_double", new Double(60.123))
                .put("attr_string", "This is String value")
                .putNull("attr_null");
    }
    
    private static final boolean PRINT_TO_CONSOLE = true;
    
    private void helpTest(String name, MetadataFactory mf) throws ResourceException {
        try {
            String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
            if(PRINT_TO_CONSOLE) {
                System.out.println(metadataDDL);
            }
            String expected = new String(Files.readAllBytes(Paths.get(TestCouchbaseMetadataProcessor.class.getClassLoader().getResource(name).toURI())));
            assertEquals(expected, metadataDDL);
        } catch (IOException | URISyntaxException e) {
            throw new ResourceException(e);
        }
    }
}
