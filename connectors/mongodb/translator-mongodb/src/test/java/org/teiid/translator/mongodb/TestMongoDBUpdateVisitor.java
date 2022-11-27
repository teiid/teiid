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
package org.teiid.translator.mongodb;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Command;
import org.teiid.language.Delete;
import org.teiid.language.Insert;
import org.teiid.language.Update;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.TranslatorException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;

@SuppressWarnings("nls")
public class TestMongoDBUpdateVisitor {

    private MongoDBExecutionFactory translator;
    private TranslationUtility utility;
    private LinkedHashMap<String, DBObject> docs;

    @Before
    public void setUp() throws Exception {
        this.translator = new MongoDBExecutionFactory();
        this.translator.start();

        TransformationMetadata metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("northwind.ddl")), "sakila", "northwind");
        this.utility = new TranslationUtility(metadata);

    }

    private MergeDetails buildKey(String name, String parentTable, String embeddedTable, String id) {
        MergeDetails key = new MergeDetails(null);
        key.setName(name);
        key.setParentTable(parentTable);
        key.setEmbeddedTable(embeddedTable);
        key.setId("id", id);
        return key;
    }

    private void helpExecute(String query, String collection, String expected, String match, MergeDetails pushKey, List<MergeDetails> pullKeys) throws Exception {
        Command cmd = this.utility.parseCommand(query);
        MongoDBUpdateVisitor visitor = new MongoDBUpdateVisitor(this.translator, this.utility.createRuntimeMetadata(), Mockito.mock(DB.class));
        visitor.visitNode(cmd);
        if (!visitor.exceptions.isEmpty()) {
            throw visitor.exceptions.get(0);
        }

        assertEquals(collection, visitor.mongoDoc.getTargetTable().getName());

        if (cmd instanceof Insert) {
            assertEquals("wrong insert", expected, visitor.getInsert(this.docs).toString());
        }
        else if (cmd instanceof Update) {
            assertEquals("wrong update", expected, visitor.getUpdate(this.docs).toString());
        }
        else if (cmd instanceof Delete) {
        }

        if (visitor.match != null) {
            assertEquals("match wrong", match, visitor.match.toString());
        }

        MongoDocument doc = visitor.mongoDoc;
        if (doc.isMerged()) {
            assertEquals("Wrong PushKey", pushKey.toString(), visitor.mongoDoc.getMergeKey().toString());
        }

        if (!visitor.mongoDoc.getEmbeddedReferences().isEmpty()) {
            assertEquals("Wrong PullKeys", visitor.mongoDoc.getEmbeddedReferences().toString(), pullKeys.toString());
        }
        this.docs = null;
    }

    @Test
    public void testInsert() throws Exception {
        helpExecute("insert into users (id, user_id, age, status) values (1, 'johndoe', 34, 'A')", "users",
                "{ \"_id\" : 1, \"user_id\" : \"johndoe\", \"age\" : 34, \"status\" : \"A\" }",
                null, null, null);
    }

    @Test
    public void testInsertWithFKWithEmbeddable() throws Exception {
        this.docs = new LinkedHashMap<String, DBObject>();
        this.docs.put("Categories", new BasicDBObject("categoryK", "categoryV"));
        this.docs.put("Suppliers", new BasicDBObject("SuppliersK", "SuppliersV"));

        ArrayList<MergeDetails> pull = new ArrayList<MergeDetails>();
        pull.add(buildKey("Categories", "Products", "Categories", "24"));
        pull.add(buildKey("Suppliers", "Products", "Suppliers", "34"));

        helpExecute("insert into Products (ProductID, ProductName, SupplierID, CategoryID, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued) " +
                "values (1, 'hammer', 34, 24, 12, 12.50, 3, 4, 2, 1)",
                "Products",
                "{ \"_id\" : 1, \"ProductName\" : \"hammer\", \"SupplierID\" : 34, \"CategoryID\" : 24, \"QuantityPerUnit\" : \"12\", \"UnitPrice\" : 12.5, \"UnitsInStock\" : 3, \"UnitsOnOrder\" : 4, \"ReorderLevel\" : 2, \"Discontinued\" : 1, \"Categories\" : { \"categoryK\" : \"categoryV\" }, \"Suppliers\" : { \"SuppliersK\" : \"SuppliersV\" } }",
                null, null,    pull);
    }


    @Test
    public void testMergeInsert() throws Exception {
        helpExecute("insert into OrderDetails (odID, ProductID, UnitPrice, Quantity, Discount) " +
                "values (2, 3, 1.50, 12, 1.0)",
                "Orders",
                "{ \"_id\" : { \"odID\" : 2, \"ProductID\" : 3 }, \"UnitPrice\" : 1.5, \"Quantity\" : 12, \"Discount\" : 1.0 }",
                null, buildKey("FK1", "Orders", "OrderDetails", "2"), null);
    }

    @Test
    public void testUpdate() throws Exception {
        helpExecute("update users set age = 48",  "users", "{ \"age\" : 48 }", null, null, null);
    }

    @Test
    public void testUpdateFK() throws Exception {
        helpExecute(
                "update users set user_id = 'billybob'",
                "users",
                "{ \"user_id\" : \"billybob\" }",
                null, null, null);
    }

    @Test
    public void testUpdateWithWhere() throws Exception {
        helpExecute(
                "update users set user_id = 'billybob' WHERE age > 50",
                "users",
                "{ \"user_id\" : \"billybob\" }",
                "{ \"age\" : { \"$gt\" : 50 } }", null, null);
    }

    @Test
    public void testDeleteWithWhere() throws Exception {
        helpExecute("delete from users WHERE age > 50", "users", null,
                "{ \"age\" : { \"$gt\" : 50 } }", null, null);
    }

    @Test
    public void testUpdateEmbedddedInSimpleUpdate() throws Exception {
        helpExecute("UPDATE OrderDetails SET UnitPrice = 14.50", "Orders",
                "{ \"OrderDetails.$.UnitPrice\" : 14.5 }", null,
                buildKey("FK1", "Orders", "OrderDetails", null), null);
    }

    @Test(expected=TranslatorException.class)
    public void testUpdateMergeReferenceUpdate() throws Exception {
        helpExecute("UPDATE OrderDetails SET ProductID = 4", "Orders",
                "{ \"ProductID\" : 4 }",
                null, buildKey("FK1", "Orders", "OrderDetails", null),
                null);
    }

    @Test(expected=TranslatorException.class)
    public void testUpdateMergeParentUpdate() throws Exception {
        this.docs = new LinkedHashMap<String, DBObject>();
        this.docs.put("Products", new BasicDBObject("key", "value"));
        helpExecute("UPDATE OrderDetails SET odID = 4",  "Orders",
                "{ \"Products.ProductID\" : { \"$ref\" : \"Products\" , \"$id\" : 4 } , \"Products\" : { \"key\" : \"value\" } }",
                null, buildKey("FK1", "Orders", "OrderDetails", null), null);
    }

    @Test
    public void testUpdateParentTableWithEmbeddable() throws Exception {
        this.docs = new LinkedHashMap<String, DBObject>();
        this.docs.put("Categories", new BasicDBObject("categoryK", "categoryV"));

        ArrayList<MergeDetails> pull = new ArrayList<MergeDetails>();
        pull.add(buildKey("Categories", "Products", "Categories", "4"));
        pull.add(buildKey("Suppliers", "Products", "Suppliers", null));

        helpExecute("UPDATE Products SET CategoryID = 4",  "Products",
                "{ \"CategoryID\" : 4, \"Categories\" : { \"categoryK\" : \"categoryV\" } }",
                null,null,pull);
    }


    @Test
    public void testUpdateEmbeddableTable() throws Exception {
        this.docs = new LinkedHashMap<String, DBObject>();

        helpExecute("UPDATE Categories SET Description = 'change' WHERE CategoryID = 1",  "Categories",
                "{ \"Description\" : \"change\" }",
                "{ \"_id\" : 1 }",
                null,
                null);
    }

    @Test
    public void testCompositeKeyInsert() throws Exception {
        helpExecute("insert into G1 (e1, e2, e3) values (1,2,3)", "G1",
                "{ \"_id\" : { \"e1\" : 1, \"e2\" : 2 }, \"e3\" : 3 }",
                null, null, null);
    }

    @Test
    @Ignore
    public void testCompositeKeyUpdate() throws Exception {
        helpExecute("update G1 set e2 = 48",  "G1", "{ \"_id.e2\" : 48 }", null, null, null);
    }

    @Test
    public void testCompositeKeyDeleteWithWhere() throws Exception {
        helpExecute("delete from G1 WHERE e1 > 50", "G1", null,
                "{ \"_id.e1\" : { \"$gt\" : 50 } }", null, null);
    }

    @Test
    public void testCompositeFKKeyInsert() throws Exception {
        helpExecute("insert into G2 (e1, e2, e3) values (1,2,3)", "G2",
                "{ \"e1\" : 1, \"e2\" : 2, \"e3\" : 3 }",
                null, null, null);
    }

    @Test
    public void testCompositeFKUpdate() throws Exception {
        helpExecute("update G2 set e1=47, e2 = 48",  "G2", "{ \"e1\" : 47, \"e2\" : 48 }", null, null, null);
    }

    @Test
    public void testCompositeFKUpdateNonKey() throws Exception {
        helpExecute("update G2 set e3=0 where e2 = 48",  "G2", "{ \"e3\" : 0 }", "{ \"e2\" : 48 }", null, null);
    }

}
