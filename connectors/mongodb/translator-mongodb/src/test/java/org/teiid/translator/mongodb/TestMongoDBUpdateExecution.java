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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.teiid.CommandContext;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Command;
import org.teiid.mongodb.MongoDBConnection;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

import com.mongodb.*;

@SuppressWarnings("nls")
public class TestMongoDBUpdateExecution {
    private MongoDBExecutionFactory translator;
    private TranslationUtility utility;

    @Before
    public void setUp() throws Exception {
        this.translator = new MongoDBExecutionFactory();
        this.translator.start();

        TransformationMetadata metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("northwind.ddl")), "sakila", "northwind");
        this.utility = new TranslationUtility(metadata);
    }

    @Test
    public void testSimpleInsertNoAssosiations() throws Exception {
        String query = "insert into Customers (CustomerID,CompanyName,ContactName,"
                + "ContactTitle,Address,City,Region,"
                + "PostalCode,Country,Phone,Fax) VALUES ('11', 'jboss', 'teiid', 'Mr.Lizard', "
                + "'jboss.org', 'internet', 'all', '1111', 'US', '555-1212', '555-1212')";

        DBCollection dbCollection = helpUpdate(query, new String[]{"Customers"}, null, null);

        BasicDBObject result = new BasicDBObject();
        result.append("_id", "11");
        result.append("CompanyName", "jboss");
        result.append("ContactName" , "teiid");
        result.append("ContactTitle","Mr.Lizard");
        result.append("Address","jboss.org");
        result.append("City","internet");
        result.append("Region","all");
        result.append("PostalCode","1111");
        result.append( "Country","US");
        result.append("Phone","555-1212");
        result.append("Fax","555-1212");
        Mockito.verify(dbCollection).insert(result, WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection, Mockito.never()).update(new BasicDBObject(), result, false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test
    public void testEmbeddableInsert() throws Exception {
        String query = "INSERT INTO Categories (CategoryID, CategoryName, Description, Picture) " +
                "VALUES (12, 'mongo', 'mongo with sql', null)";

        DBCollection dbCollection = helpUpdate(query, new String[]{"Categories"}, null, null);
        BasicDBObject result = new BasicDBObject();
        result.append("_id", 12);
        result.append("CategoryName", "mongo");
        result.append("Description" , "mongo with sql");
        result.append("Picture",null);
        Mockito.verify(dbCollection).insert(result, WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection, Mockito.never()).update(new BasicDBObject(), result, false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test
    public void testMergeInsert() throws Exception {
        // tests one-to-many situation
        String query = "INSERT INTO OrderDetails (odID, ProductID, UnitPrice, Quantity, Discount) " +
                "VALUES (14, 15, 34.50, 10, 12)";

        BasicDBObject match = new BasicDBObject("_id", 14);
        DBCollection dbCollection = helpUpdate(query, new String[]{"Orders"}, match, null);

        BasicDBObject details = new BasicDBObject();
        BasicDBObject pk = new BasicDBObject();
        pk.append("odID", 14);
        pk.append("ProductID", 15);

        details.append("UnitPrice", 34.50);
        details.append("Quantity", 10);
        details.append("Discount", 12.0);
        details.append("_id", pk);

        details = new BasicDBObject("OrderDetails", details);
        Mockito.verify(dbCollection, Mockito.never()).insert(details, WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, new BasicDBObject("$push", details), false, true, WriteConcern.ACKNOWLEDGED);
    }


    @Test // one-2-one mapping update merge case;
    public void testMergeInsertOne2One() throws Exception {
        String query = "INSERT INTO address (cust_id, street, zip) VALUES (100, '123 Street', '90210')";

        BasicDBObject match = new BasicDBObject("_id", 100);
        DBCollection dbCollection = helpUpdate(query, new String[]{"customer"}, match, null);

        BasicDBObject details = new BasicDBObject();

        details.append("street", "123 Street");
        details.append("zip", "90210");

        details = new BasicDBObject("address", details);
        Mockito.verify(dbCollection, Mockito.never()).insert(details, WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, new BasicDBObject("$set", details), false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test // one-2-many mapping update merge case;
    public void testMergeInsertOne2Many() throws Exception {
        String query = "INSERT INTO Notes (CustomerId, PostDate, Comment) VALUES (100, null, 'Hello')";

        BasicDBObject match = new BasicDBObject("_id", 100);
        DBCollection dbCollection = helpUpdate(query, new String[]{"customer"}, match, null);

        BasicDBObject details = new BasicDBObject();
        details.append("PostDate", null);
        details.append("Comment", "Hello");

        details = new BasicDBObject("Notes", details);
        Mockito.verify(dbCollection, Mockito.never()).insert(details, WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, new BasicDBObject("$push", details), false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test // one-2-one mapping update merge case;
    public void testMergeDeleteOne2OneonPK() throws Exception {
        // tests one-to-many situation
        String query = "DELETE FROM address WHERE cust_id = 100";

        ArrayList<DBObject> results = new ArrayList<DBObject>();
        BasicDBObject address = new BasicDBObject();
        address.append("street", "123 Street").append("zip", "90210").append("_id", new DBRef(null, "customer", 100));
        results.add(new BasicDBObject("_id", 100).append("address", address));

        DBObject match = new BasicDBObject("_id", 100);

        DBCollection dbCollection = helpUpdate(query, new String[]{"customer"}, match, results);

        BasicDBObject details = new BasicDBObject("address", "");
        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, new BasicDBObject("$unset", details), false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test // one-2-one mapping update merge case;
    public void testMergeDeleteOne2OneonNonPK() throws Exception {
        // tests one-to-many situation
        String query = "DELETE FROM address WHERE street = 'Highway 100'";

        ArrayList<DBObject> results = new ArrayList<DBObject>();
        BasicDBObject address = new BasicDBObject();
        address.append("street", "123 Street").append("zip", "90210").append("_id", new DBRef(null, "customer", 100));
        results.add(new BasicDBObject("_id", 100).append("address", address));

        DBObject match = QueryBuilder.start("address.street").is("Highway 100").get();
        DBCollection dbCollection = helpUpdate(query, new String[]{"customer"}, match, results);

        BasicDBObject details = new BasicDBObject("address", "");
        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, new BasicDBObject("$unset", details), false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test // one-2-one mapping update merge case;
    public void testMergeDeleteOne2OneAllRows() throws Exception {
        // tests one-to-many situation
        String query = "DELETE FROM address";

        ArrayList<DBObject> results = new ArrayList<DBObject>();
        BasicDBObject address = new BasicDBObject();
        address.append("street", "123 Street").append("zip", "90210").append("_id", new DBRef(null, "customer", 100));
        results.add(new BasicDBObject("_id", 100).append("address", address));

        DBObject match = QueryBuilder.start("address").exists(true).get();
        DBCollection dbCollection = helpUpdate(query, new String[]{"customer"}, match, results);

        BasicDBObject details = new BasicDBObject("address", "");
        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, new BasicDBObject("$unset", details), false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test(expected=TranslatorException.class) // one-2-one mapping update merge case;
    public void testMergeDeleteOne2ManyWithORClause() throws Exception {
        // tests one-to-many situation
        String query = "DELETE FROM Notes WHERE CustomerId = 100 OR Comment IN ('Hello', 'Hola')";

        ArrayList<DBObject> results = new ArrayList<DBObject>();
        BasicDBList row = new BasicDBList();
        row.add(new BasicDBObject("Comment", "Hello").append("PostDate", "{ts '2014-07-15 09:06:04'}"));
        row.add(new BasicDBObject("Comment", "Hola").append("PostDate", "{ts '2013-07-15 09:06:04'}"));
        results.add(new BasicDBObject("_id", 100).append("Notes", row));

        BasicDBList commentIN = new BasicDBList();
        commentIN.add("Hello");
        commentIN.add("Hola");
        DBObject match = new BasicDBObject("_id", 100);
        DBCollection dbCollection = helpUpdate(query, new String[]{"customer"}, match, results);

        BasicDBObject details = new BasicDBObject("Notes", QueryBuilder.start("Comment").in(commentIN).get());
        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, new BasicDBObject("$pull", details), false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test // one-2-one mapping update merge case;
    public void testMergeDeleteOne2ManyWithIN() throws Exception {
        // tests one-to-many situation
        String query = "DELETE FROM Notes WHERE Comment IN ('Hello', 'Hola')";

        ArrayList<DBObject> results = new ArrayList<DBObject>();
        BasicDBList row = new BasicDBList();
        row.add(new BasicDBObject("Comment", "Hello").append("PostDate", "{ts '2014-07-15 09:06:04'}"));
        row.add(new BasicDBObject("Comment", "Hola").append("PostDate", "{ts '2013-07-15 09:06:04'}"));
        results.add(new BasicDBObject("_id", 100).append("Notes", row));

        BasicDBList commentIN = new BasicDBList();
        commentIN.add("Hello");
        commentIN.add("Hola");
        DBObject match = QueryBuilder.start("Notes").exists(true).get();
        DBCollection dbCollection = helpUpdate(query, new String[]{"customer"}, match, results);

        BasicDBObject details = new BasicDBObject("Notes", QueryBuilder.start("Comment").in(commentIN).get());
        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, new BasicDBObject("$pull", details), false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test // one-2-one mapping update merge case;
    public void testMergeDeleteOne2ManyNonFK() throws Exception {
        // tests one-to-many situation
        String query = "DELETE FROM Notes WHERE Comment = 'Hello'";

        ArrayList<DBObject> results = new ArrayList<DBObject>();
        BasicDBList row = new BasicDBList();
        row.add(new BasicDBObject("Comment", "Hello").append("PostDate", "{ts '2014-07-15 09:06:04'}"));
        row.add(new BasicDBObject("Comment", "Hola").append("PostDate", "{ts '2013-07-15 09:06:04'}"));
        results.add(new BasicDBObject("_id", 100).append("Notes", row));

        DBObject match = QueryBuilder.start("Notes").exists(true).get();
        DBCollection dbCollection = helpUpdate(query, new String[]{"customer"}, match, results);

        BasicDBObject details = new BasicDBObject("Notes", new BasicDBObject("Comment", "Hello"));
        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, new BasicDBObject("$pull", details), false, true, WriteConcern.ACKNOWLEDGED);
    }


    private DBCollection helpUpdate(String query, String[] expectedCollection, DBObject match_result, ArrayList<DBObject> results) throws TranslatorException {
        Command cmd = this.utility.parseCommand(query);
        ExecutionContext context = Mockito.mock(ExecutionContext.class);
        CommandContext cc = Mockito.mock(CommandContext.class);
        Mockito.stub(context.getCommandContext()).toReturn(cc);
        MongoDBConnection connection = Mockito.mock(MongoDBConnection.class);
        DB db = Mockito.mock(DB.class);
        DBCollection dbCollection = Mockito.mock(DBCollection.class);
        for(String collection:expectedCollection) {
            Mockito.stub(db.getCollection(collection)).toReturn(dbCollection);
        }
        Mockito.stub(db.collectionExists(Mockito.anyString())).toReturn(true);
        Mockito.stub(connection.getDatabase()).toReturn(db);

        Mockito.stub(db.getCollectionFromString(Mockito.anyString())).toReturn(dbCollection);
        Mockito.stub(dbCollection.findOne(Mockito.any(BasicDBObject.class))).toReturn(match_result);
        WriteResult result = Mockito.mock(WriteResult.class);
        Mockito.stub(result.getN()).toReturn(1);
        Mockito.stub(dbCollection.insert(Mockito.any(BasicDBObject.class), Mockito.any(WriteConcern.class)))
            .toReturn(result);
        Mockito.stub(dbCollection.update(Mockito.any(BasicDBObject.class),
                Mockito.any(BasicDBObject.class),
                Mockito.eq(false),
                Mockito.eq(true),
                Mockito.any(WriteConcern.class))).toReturn(result);

        if (results != null) {
            Cursor out = new ResultsCursor(results);
            for (DBObject obj:results) {
                Mockito.stub(dbCollection.aggregate(Mockito.anyList(), Mockito.any(AggregationOptions.class))).toReturn(out);
                Mockito.stub(dbCollection.aggregate(Mockito.anyList(), Mockito.any(AggregationOptions.class))).toReturn(out);
            }
        }

        UpdateExecution execution = this.translator.createUpdateExecution(cmd, context, this.utility.createRuntimeMetadata(), connection);
        execution.execute();
        return dbCollection;
    }

    private static class ResultsCursor implements Cursor {
        private Iterator<DBObject> rows;

        ResultsCursor (ArrayList<DBObject> rows){
            this.rows = rows.iterator();
        }

        @Override
        public void remove() {
            this.rows.remove();
        }

        @Override
        public DBObject next() {
            return rows.next();
        }

        @Override
        public boolean hasNext() {
            return this.rows.hasNext();
        }

        @Override
        public ServerAddress getServerAddress() {
            return null;
        }

        @Override
        public long getCursorId() {
            return 0;
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void testUpdateEmbeddable() throws Exception {
        // check to make sure about multiple updates
        String query = "UPDATE Categories SET CategoryName='JBOSS' WHERE CategoryID = 11";

        BasicDBObject match = new BasicDBObject("_id", 11);
        BasicDBObject details = new BasicDBObject();
        details.append("CategoryName", "JBOSS");
        details = new BasicDBObject("$set", details);

        ArrayList<DBObject> results = new ArrayList<DBObject>();
        results.add(new BasicDBObject("_id", 11).append("key", "value"));

        DBCollection dbCollection = helpUpdate(query, new String[]{"Categories", "Products"}, match, results);

        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, details, false, true, WriteConcern.ACKNOWLEDGED);

        Mockito.verify(dbCollection).update(
                new BasicDBObject("CategoryID", 11), new BasicDBObject("$set", new BasicDBObject("Categories", new BasicDBObject("key","value"))),
                false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Ignore // the composite key with nesting is not going to work when both keys are not in the parent.
    @Test // one to many
    public void testDeleteMergeOneToManyOnPK() throws Exception {
        String query = "DELETE FROM OrderDetails WHERE ProductID = 14 and odID = 1";

        DBCollection dbCollection = helpUpdate(query, new String[]{"Orders"}, new BasicDBObject("oid", 1), null);
        DBObject match = QueryBuilder.start().and(new BasicDBObject("_id.ProductID", 14), new BasicDBObject("_id.odID", 1)).get();
        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection, Mockito.times(1)).update(
                new BasicDBObject("_id", 1), new BasicDBObject("$pull", new BasicDBObject("OrderDetails", match)),
                false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Ignore // the composite key with nesting is not going to work when both keys are not in the parent.
    @Test // one to many
    public void testDeleteMergeOneToManyOnPKANDColumn() throws Exception {
        String query = "DELETE FROM OrderDetails WHERE ProductID = 14 and UnitPrice > 23.89";

        DBCollection dbCollection = helpUpdate(query, new String[]{"Orders"}, QueryBuilder.start().exists("OrderDetails").get(), null);

        QueryBuilder qb = QueryBuilder.start("OrderDetails").exists(true);

        QueryBuilder pullQuery = new QueryBuilder();
        pullQuery.and(new BasicDBObject("_id.ProductID", 14), QueryBuilder.start("UnitPrice").greaterThan(23.89).get());

        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection, Mockito.times(1)).update(
                qb.get(), new BasicDBObject("$pull", new BasicDBObject("OrderDetails", pullQuery.get())),
                false, true, WriteConcern.ACKNOWLEDGED);
    }


    @Test // one to many
    @Ignore
    public void testMergeOne2ManyUpdateComplexKey() throws Exception {
        String query = "UPDATE OrderDetails SET UnitPrice = 12.50 WHERE ProductID = 14 and odID = 1";

        BasicDBObject pk= new BasicDBObject("ProductID", 14);
        BasicDBObject pk2= new BasicDBObject("ProductID", 15);

        BasicDBList result = new BasicDBList();
        result.add(new BasicDBObject().append("_id", pk).append("UnitPrice", 99.00).append("Quantity", 11));
        result.add(new BasicDBObject().append("_id", pk2).append("UnitPrice", 0.99).append("Quantity", 12));

        BasicDBObject row = new BasicDBObject("OrderDetails", result).append("_id", 1);

        ArrayList<DBObject> results = new ArrayList<DBObject>();
        results.add(row);

        DBCollection dbCollection = helpUpdate(query, new String[]{"Orders"}, new BasicDBObject("oid", 1), results );
        // { "$set" : { "OrderDetails" : [ { "_id" : 1 , "ProductID" : 14 , "odID" : 1 , "UnitPrice" : 12.5}]}},
        BasicDBList expected = new BasicDBList();
        expected.add(new BasicDBObject("_id", pk).append("UnitPrice", 12.50).append("Quantity", 11));
        expected.add(new BasicDBObject().append("_id", pk2).append("UnitPrice", 0.99).append("Quantity", 12));

        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection, Mockito.times(1)).update(
                new BasicDBObject("_id", 1), new BasicDBObject("$set", new BasicDBObject("OrderDetails", expected)),
                false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test
    public void tesNestedEmbeddingInsert() throws Exception {
        String query = "insert into T1 (e1, e2, e3) VALUES (1, 2, 3)";

        BasicDBObject match = new BasicDBObject();
        match.append("_id", 2);

        DBCollection dbCollection = helpUpdate(query, new String[]{"T1", "T2"}, match, null);

        BasicDBObject row = new BasicDBObject();
        row.append("e1", 1);
        row.append("e3", 3);
        row.append("_id", 2);
        row.append("T2", match);

        Mockito.verify(dbCollection).insert(row, WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection, Mockito.never()).update(match, row, false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test
    @Ignore
    public void tesNestedEmbeddingUpdate() throws Exception {
        String query = "UPDATE T3 SET e2 = 2, e3 = 3 WHERE e1 = 1";

        BasicDBObject t3_match = new BasicDBObject();
        t3_match.append("_id", 1);

        ArrayList<DBObject> results = new ArrayList<DBObject>();
        results.add(new BasicDBObject("_id", 1).append("key", "value"));

        DBCollection dbCollection = helpUpdate(query, new String[]{"T3", "T2", "T1"}, t3_match, results);

        BasicDBObject t3_row = new BasicDBObject();
        t3_row.append("e2", 2);
        t3_row.append("e3", 3);


        BasicDBObject t2_match = new BasicDBObject();
        t2_match.append("e1", 1);

        BasicDBObject t1row = new BasicDBObject("T2", results.get(0));
        BasicDBObject t2row = new BasicDBObject("T3", results.get(0));

        BasicDBObject t1_match = new BasicDBObject();
        t1_match.append("e1", 1);

        Mockito.verify(dbCollection, Mockito.never()).insert(t3_row, WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(t3_match, new BasicDBObject("$set", t3_row), false, true, WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(t2_match, new BasicDBObject("$set", t2row), false, true, WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(t1_match, new BasicDBObject("$set", t1row), false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test
    @Ignore
    public void tesNestedEmbeddingUpdateInMiddle() throws Exception {
        String query = "UPDATE T2 SET e2 = 2, e3 = 3 WHERE e1 = 1";

        BasicDBObject match = new BasicDBObject();
        match.append("_id", 1);

        ArrayList<DBObject> results = new ArrayList<DBObject>();
        results.add(new BasicDBObject("_id", 1).append("key", "value"));

        DBCollection dbCollection = helpUpdate(query, new String[]{"T2", "T1", "T3"}, match, results);

        BasicDBObject row = new BasicDBObject();
        row.append("e2", 2);
        row.append("e3", 3);

        BasicDBObject t2_match = new BasicDBObject();
        t2_match.append("e1.$id", 1);

        BasicDBObject t2row = new BasicDBObject("T2", results.get(0));

        Mockito.verify(dbCollection, Mockito.never()).insert(row, WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, new BasicDBObject("$set", row), false, true, WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(t2_match, new BasicDBObject("$set", t2row), false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test
    public void testNestedMergeInsert() throws Exception {
        String query = "INSERT INTO customer (customer_id, name) VALUES (1, 'jboss')";

        BasicDBObject customer_result = new BasicDBObject();
        customer_result.append("name", "jboss");
        customer_result.append("_id", 1);

        BasicDBObject match = new BasicDBObject();
        match.append("_id", 1);

        DBCollection dbCollection = helpUpdate(query, new String[]{"customer"}, null, null);
        Mockito.verify(dbCollection).insert(customer_result, WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection, Mockito.never()).update(new BasicDBObject(), match, false, true, WriteConcern.ACKNOWLEDGED);

        // { "$push" : { "rental" : { "amount" : "3.99" , "customer_id" : { "$ref" : "customer" , "$id" : 1} , "_id" : 1}}},
        BasicDBObject rental = new BasicDBObject();
        rental.append("amount", 3.99);
        //rental.append("customer_id", new DBRef(null, "customer", 1));
        rental.append("_id", 2);
        BasicDBObject rentalresult = new BasicDBObject("rental", rental);


        query = "INSERT INTO rental (rental_id, amount, customer_id) VALUES (2, 3.99, 1)";
        dbCollection = helpUpdate(query, new String[]{"customer"}, customer_result, null);
        Mockito.verify(dbCollection, Mockito.never()).insert(customer_result, WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, new BasicDBObject("$push", rentalresult), false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test
    public void testArrayInsert() throws Exception {
        String query = "insert into ArrayTest(id,column1) VALUES (1, ('jboss', 'teiid', 'Mr.Lizard'))";

        DBCollection dbCollection = helpUpdate(query, new String[]{"ArrayTest"}, null, null);

        BasicDBList col1 = new BasicDBList();
        col1.add("jboss");
        col1.add("teiid");
        col1.add("Mr.Lizard");

        BasicDBObject result = new BasicDBObject();
        result.append("id", 1);
        result.append("column1", col1);
        Mockito.verify(dbCollection).insert(result, WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection, Mockito.never()).update(new BasicDBObject(), result, false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test
    public void testArrayUpdate() throws Exception {
        String query = "UPDATE ArrayTest SET column1 = ('jboss', 'teiid', 'Mr.Lizard') WHERE id = 1";

        BasicDBObject match = new BasicDBObject("id", 1);
        DBCollection dbCollection = helpUpdate(query, new String[]{"ArrayTest"}, match, null);

        BasicDBList col1 = new BasicDBList();
        col1.add("jboss");
        col1.add("teiid");
        col1.add("Mr.Lizard");

        BasicDBObject details = new BasicDBObject();
        details.append("column1", col1);

        details = new BasicDBObject("$set", details);

        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, details, false, true, WriteConcern.ACKNOWLEDGED);
    }

    /////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////
    // NEW Tests based on N1, N2, N3, N4, N5, N6, N7
    // N1 -> N2 -> N3
    // N1 -> N2 -> N4[]
    // N1 -> N5[] -> N6
    // N1 -> N5[] -> N7[]
    /////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////

    @Test // one to many
    public void testMergeDelete_one_2_many_all() throws Exception {
        String query = "DELETE FROM N5";

        BasicDBObject N5ROW1 = new BasicDBObject().append("e1", 3).append("e3", 3);
        BasicDBObject N5ROW2 = new BasicDBObject().append("e1", 3).append("e3", 3);
        BasicDBList N5Rows = new BasicDBList();
        N5Rows.add(N5ROW1);
        N5Rows.add(N5ROW2);

        ArrayList<DBObject> N1ROWS = new ArrayList<DBObject>();
        N1ROWS.add(new BasicDBObject("_id", 1).append("N5", N5Rows));

        DBObject match = QueryBuilder.start("N5").exists(true).get();
        DBCollection dbCollection = helpUpdate(query, new String[] { "N1" }, match, N1ROWS);

        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, new BasicDBObject("$pull", new BasicDBObject("N5", new BasicDBObject())),
                false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test
    public void testMergeDelete_one_2_many_usingFK() throws Exception {
        // tests one-to-many situation
        String query = "DELETE FROM N5 WHERE e2 = 100";

        ArrayList<DBObject> results = new ArrayList<DBObject>();
        BasicDBList row = new BasicDBList();
        row.add(new BasicDBObject("e1", 1).append("e3", 5));
        row.add(new BasicDBObject("e1", 2).append("e3", 5));
        results.add(new BasicDBObject("_id", 100).append("N5", row));

        BasicDBObject match = new BasicDBObject("_id", 100);
        DBCollection dbCollection = helpUpdate(query, new String[]{"N1"}, match, results);

        BasicDBObject details = new BasicDBObject("N5", new BasicDBList());
        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, new BasicDBObject("$set", details), false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test
    public void testMergeDelete_one_2_many_usingFK2() throws Exception {
        // tests one-to-many situation
        String query = "DELETE FROM N5 WHERE e2 = 100 and e1=1";

        ArrayList<DBObject> results = new ArrayList<DBObject>();
        BasicDBList row = new BasicDBList();
        row.add(new BasicDBObject("_id", 1).append("e3", 5));
        row.add(new BasicDBObject("_id", 2).append("e3", 5));
        results.add(new BasicDBObject("_id", 100).append("N5", row));

        BasicDBObject match = new BasicDBObject("_id", 100);
        DBCollection dbCollection = helpUpdate(query, new String[]{"N1"}, match, results);

        BasicDBList expected = new BasicDBList();
        expected.add(new BasicDBObject("_id", 2).append("e3", 5));
        BasicDBObject details = new BasicDBObject("N5", expected);
        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, new BasicDBObject("$set", details), false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test
    public void testNestedMergeDelete_one_2_many_many() throws Exception {
        // tests one-to-many situation
        String query = "DELETE FROM N7 WHERE e2 = 5 and e1=7";


        ArrayList<DBObject> N1ROWS = new ArrayList<DBObject>();
        BasicDBList N5ROW = new BasicDBList();
        N5ROW.add(5);
        N5ROW.add(51);

        BasicDBList N5ROW2 = new BasicDBList();
        N5ROW2.add(50);
        N5ROW2.add(5);

        BasicDBList N5_N7ROW = new BasicDBList();
        N5_N7ROW.add(new BasicDBObject("_id", 7).append("e3", 7));

        BasicDBList N5_N7ROW_ARRAY1 = new BasicDBList();
        N5_N7ROW_ARRAY1.add(N5_N7ROW);

        BasicDBList N5_N7ROW2 = new BasicDBList();
        N5_N7ROW2.add(new BasicDBObject("_id", 5).append("e3", 7));
        N5_N7ROW2.add(new BasicDBObject("_id", 7).append("e3", 7));

        BasicDBList N5_N7ROW_ARRAY2 = new BasicDBList();
        N5_N7ROW_ARRAY2.add(new BasicDBList());
        N5_N7ROW_ARRAY2.add(N5_N7ROW2);

        N1ROWS.add(new BasicDBObject("_id", 1).append("N5", N5ROW).append("N5_N7", N5_N7ROW_ARRAY1));
        N1ROWS.add(new BasicDBObject("_id", 2).append("N5", N5ROW2).append("N5_N7", N5_N7ROW_ARRAY2));

        BasicDBObject match = new BasicDBObject("_id", 1);
        DBCollection dbCollection = helpUpdate(query, new String[]{"N1"}, match, N1ROWS);

        ArgumentCaptor<BasicDBObject> matchCaptor = ArgumentCaptor.forClass(BasicDBObject.class);
        ArgumentCaptor<BasicDBObject> updateCaptor = ArgumentCaptor.forClass(BasicDBObject.class);

        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection, Mockito.times(2)).update(matchCaptor.capture(), updateCaptor.capture(), Mockito.eq(false),
                Mockito.eq(true), Mockito.eq(WriteConcern.ACKNOWLEDGED));

        assertEquals(new BasicDBObject("_id", 1), matchCaptor.getAllValues().get(0));
        assertEquals(new BasicDBObject("_id", 2), matchCaptor.getAllValues().get(1));

        BasicDBList EXPECTED_N5_N7ROW2 = new BasicDBList();
        EXPECTED_N5_N7ROW2.add(new BasicDBObject("_id", 5).append("e3", 7));

        BasicDBList EXPECTED_N5_N7ROW2_ARRAY = new BasicDBList();
        EXPECTED_N5_N7ROW2_ARRAY.add(EXPECTED_N5_N7ROW2);


        assertEquals(new BasicDBObject("$set", new BasicDBObject("N5.0.N7", new BasicDBList())),
                updateCaptor.getAllValues().get(0));

        assertEquals(new BasicDBObject("$set", new BasicDBObject("N5.1.N7", EXPECTED_N5_N7ROW2)),
                updateCaptor.getAllValues().get(1));
    }

    @Test
    public void testMergeDelete_one_2_one_2_many() throws Exception {
        // tests one-to-many situation
        String query = "DELETE FROM N4 WHERE e2 = 100 and e1=1";

        ArrayList<DBObject> results = new ArrayList<DBObject>();
        BasicDBList row = new BasicDBList();
        row.add(new BasicDBObject("_id", 1).append("e3", 5));
        row.add(new BasicDBObject("_id", 2).append("e3", 5));
        results.add(new BasicDBObject("_id", 100).append("N2_N4", row));

        BasicDBObject match = new BasicDBObject("_id", 100);
        DBCollection dbCollection = helpUpdate(query, new String[]{"N1"}, match, results);

        BasicDBList expected = new BasicDBList();
        expected.add(new BasicDBObject("_id", 2).append("e3", 5));
        BasicDBObject details = new BasicDBObject("N2.N4", expected);
        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, new BasicDBObject("$set", details), false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test
    public void testMergeDelete_one_2_one_many_onFK() throws Exception {
        // tests one-to-many situation
        String query = "DELETE FROM N4 WHERE e2 = 100";

        ArrayList<DBObject> results = new ArrayList<DBObject>();
        BasicDBList row = new BasicDBList();
        row.add(new BasicDBObject("e1", 1).append("e3", 5));
        row.add(new BasicDBObject("e1", 2).append("e3", 5));
        results.add(new BasicDBObject("_id", 100).append("N2_N4", row));

        BasicDBObject match = new BasicDBObject("_id", 100);
        DBCollection dbCollection = helpUpdate(query, new String[]{"N1"}, match, results);

        BasicDBObject details = new BasicDBObject("N2.N4", new BasicDBList());
        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, new BasicDBObject("$set", details), false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test
    public void testUpdate() throws Exception {
        String query = "UPDATE N1 SET e2 = 2, e3 = 3 WHERE e1 = 1";

        BasicDBObject match = new BasicDBObject("_id", 1);
        DBCollection dbCollection = helpUpdate(query, new String[]{"N1"}, match, null);

        BasicDBObject details = new BasicDBObject();
        details.append("e2", 2);
        details.append("e3", 3);

        details = new BasicDBObject("$set", details);

        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, details, false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test
    public void testMergeUpdate_one_2_one() throws Exception {
        String query = "UPDATE N2 SET e2 = 2 WHERE e2 = 3";

        BasicDBObject N2ROW = new BasicDBObject().append("e2", 3).append("e3", 2);

        ArrayList<DBObject> N1ROWS = new ArrayList<DBObject>();
        N1ROWS.add(new BasicDBObject("_id", 1).append("N2", N2ROW));

        BasicDBObject match = new BasicDBObject("_id", 1);
        DBCollection dbCollection = helpUpdate(query, new String[] { "N1" }, match, N1ROWS);

        BasicDBObject N2UPDATE = new BasicDBObject();
        N2UPDATE.append("N2", new BasicDBObject().append("e2", 2).append("e3", 2));

        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, new BasicDBObject("$set", N2UPDATE), false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test // one-2-many mapping update merge case;
    public void testMergeUpdate_one_2_many() throws Exception {
        // tests one-to-many situation
        String query = "UPDATE N5 SET e3 = 5 WHERE e1 = 1";

        // { "$project" : { "N5" : "$N5" , "_id" : "$_id"}}

        ArrayList<DBObject> N1ROWS = new ArrayList<DBObject>();
        BasicDBList N5ROW = new BasicDBList();
        N5ROW.add(new BasicDBObject("_id", 1).append("e2", 1).append("e3", 1));
        N5ROW.add(new BasicDBObject("_id", 2).append("e2", 1).append("e3", 2));
        N1ROWS.add(new BasicDBObject("_id", 1).append("e2", 1).append("e3", 1).append("N5", N5ROW));

        BasicDBObject match = new BasicDBObject("_id", 1);
        DBCollection dbCollection = helpUpdate(query, new String[]{"N1"}, match, N1ROWS);

        BasicDBList expected = new BasicDBList();
        expected.add(new BasicDBObject("_id", 1).append("e2", 1).append("e3", 5));
        expected.add(new BasicDBObject("_id", 2).append("e2", 1).append("e3", 2));

        BasicDBObject update = new BasicDBObject("N5", expected);
        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, new BasicDBObject("$set", update), false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test
    public void testNestedMergeInsert_one_2_one() throws Exception {
        DBObject n1_match = QueryBuilder.start().and( QueryBuilder.start("N2").exists(true).get(), new BasicDBObject("_id", 1)).get();

        BasicDBObject match = new BasicDBObject();
        match.append("_id", 1);

        BasicDBObject n3 = new BasicDBObject();
        n3.append("e2", 3);
        n3.append("e3", 3);

        BasicDBObject result = new BasicDBObject("N2.N3", n3);

        String query = "INSERT INTO N3 (e1, e2, e3) VALUES (1,3,3)";
        DBCollection dbCollection = helpUpdate(query, new String[]{"N1"}, match, null);
        Mockito.verify(dbCollection, Mockito.never()).insert(n1_match, WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(n1_match, new BasicDBObject("$set", result), false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test
    public void testNestedMergeUpdate_one_2_one_2_one() throws Exception {
        String query = "UPDATE N3 SET e2 = 2 WHERE e1 = 1 and e2 = 3";

        //{ "$project" : { "N2_N3" : "$N2.N3" , "N2" : "$N2._id" , "_id" : "$_id"}}

        BasicDBObject N3ROW = new BasicDBObject().append("e2", 3).append("e3", 3);

        ArrayList<DBObject> N1ROWS = new ArrayList<DBObject>();
        N1ROWS.add(new BasicDBObject("_id", 1).append("N2_N3", N3ROW));

        BasicDBObject match = new BasicDBObject("_id", 1);
        DBCollection dbCollection = helpUpdate(query, new String[] { "N1" }, match, N1ROWS);

        BasicDBObject N3UPDATE = new BasicDBObject();
        N3UPDATE.append("N2.N3", new BasicDBObject().append("e2", 2).append("e3", 3));

        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, new BasicDBObject("$set", N3UPDATE), false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test
    public void testNestedMergeDelete_one_2_one_2_one() throws Exception {
        String query = "DELETE FROM N3 WHERE e1 = 1 and e2 = 3";

        //{ "$project" : { "N2_N3" : "$N2.N3" , "N2" : "$N2._id" , "_id" : "$_id"}}

        BasicDBObject N3ROW = new BasicDBObject().append("e2", 3).append("e3", 3);

        ArrayList<DBObject> N1ROWS = new ArrayList<DBObject>();
        N1ROWS.add(new BasicDBObject("_id", 1).append("N2_N3", N3ROW));

        DBObject match = QueryBuilder.start().and(new BasicDBObject("_id", 1), new BasicDBObject("N2.N3.e2", 3)).get();
        DBCollection dbCollection = helpUpdate(query, new String[] { "N1" }, match, N1ROWS);

        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, new BasicDBObject("$unset", new BasicDBObject("N2.N3", "")), false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test
    public void testNestedMergeUpdate_one_2_one_2_many() throws Exception {
        String query = "UPDATE N4 SET e3 = 5 WHERE e1 = 1";

        BasicDBList N4ROW = new BasicDBList();
        N4ROW.add(new BasicDBObject("_id", 1).append("e3", 1));
        N4ROW.add(new BasicDBObject("_id", 2).append("e3", 2));

        ArrayList<DBObject> N1ROWS = new ArrayList<DBObject>();
        N1ROWS.add(new BasicDBObject("_id", 1).append("N2_N4", N4ROW));

        BasicDBObject match = new BasicDBObject("_id", 1);
        DBCollection dbCollection = helpUpdate(query, new String[] { "N1" }, match, N1ROWS);

        BasicDBList expected = new BasicDBList();
        expected.add(new BasicDBObject("_id", 1).append("e3", 5));
        expected.add(new BasicDBObject("_id", 2).append("e3", 2));

        BasicDBObject N4UPDATE = new BasicDBObject("N2.N4", expected);

        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, new BasicDBObject("$set", N4UPDATE), false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test
    public void testNestedMergeUpdate_one_2_many_2_one() throws Exception {
        String query = "UPDATE N6 SET e3 = 5 WHERE e1 = 5";

        // { "$project" : { "N5_N6" : "$N5.N6" , "N5" : "$N5._id" , "_id" : "$_id"}}
        // { "_id" : 1, "N5" : [ 5, 51 ], "N5_N6" : [ { "e2" : 6, "e3" : 6 }, { "e2" : 0, "e3" : 6 } ] }
        // N5.e1 = N6.e1
        // N5.e2 = N1.e1
        ArrayList<DBObject> N1ROWS = new ArrayList<DBObject>();
        BasicDBList N5ROW = new BasicDBList();
        N5ROW.add(5);
        N5ROW.add(51);

        BasicDBList N5ROW2 = new BasicDBList();
        N5ROW2.add(50);
        N5ROW2.add(5);

        BasicDBList N5_N6ROW = new BasicDBList();
        N5_N6ROW.add(new BasicDBObject("e2", 6).append("e3", 6));
        N5_N6ROW.add(new BasicDBObject("e2", 0).append("e3", 6));

        BasicDBList N5_N6ROW2 = new BasicDBList();
        N5_N6ROW2.add(new BasicDBObject("e2", 1).append("e3", 1));
        N5_N6ROW2.add(new BasicDBObject("e2", 2).append("e3", 2));

        N1ROWS.add(new BasicDBObject("_id", 1).append("N5", N5ROW).append("N5_N6", N5_N6ROW));
        N1ROWS.add(new BasicDBObject("_id", 2).append("N5", N5ROW2).append("N5_N6", N5_N6ROW2));

        BasicDBObject match = new BasicDBObject("_id", 1);
        DBCollection dbCollection = helpUpdate(query, new String[]{"N1"}, match, N1ROWS);

        // one from each of the N5_N6 array is updated.
        BasicDBObject expected1 = new BasicDBObject("e2", 6).append("e3", 5);
        BasicDBObject expected2 = new BasicDBObject("e2", 2).append("e3", 5);

        ArgumentCaptor<BasicDBObject> matchCaptor = ArgumentCaptor.forClass(BasicDBObject.class);
        ArgumentCaptor<BasicDBObject> updateCaptor = ArgumentCaptor.forClass(BasicDBObject.class);

        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection, Mockito.times(2)).update(matchCaptor.capture(), updateCaptor.capture(), Mockito.eq(false),
                Mockito.eq(true), Mockito.eq(WriteConcern.ACKNOWLEDGED));

        assertEquals(new BasicDBObject("_id", 1), matchCaptor.getAllValues().get(0));
        assertEquals(new BasicDBObject("_id", 2), matchCaptor.getAllValues().get(1));

        assertEquals(new BasicDBObject("$set", new BasicDBObject("N5.0.N6", expected1)),
                updateCaptor.getAllValues().get(0));
        assertEquals(new BasicDBObject("$set", new BasicDBObject("N5.1.N6", expected2)),
                updateCaptor.getAllValues().get(1));
    }

    @Test
    public void testNestedMergeUpdate_one_2_many_2_many() throws Exception {
        String query = "UPDATE N7 SET e3 = 5 WHERE e1 = 5";

        // { "$project" : { "N5_N7" : "$N5.N7" , "N5" : "$N5._id" , "_id" : "$_id"}}
        // { "_id" : 1, "N5" : [ 5, 51 ], "N5_N7" : [ [ { "e3" : 7, "_id" : 7 } ]] ] }
        // { "_id" : 1, "N5" : [ 50, 5 ], "N5_N7" : [ [ { "e3" : 7, "_id" : 7 } ], [ { "e3" : 7, "_id" : 5 }, { "e3" : 7, "_id" : 7 } ] ] }
        // N5.e1 = N7.e2
        // N5.e2 = N1.e1
        ArrayList<DBObject> N1ROWS = new ArrayList<DBObject>();
        BasicDBList N5ROW = new BasicDBList();
        N5ROW.add(5);
        N5ROW.add(51);

        BasicDBList N5ROW2 = new BasicDBList();
        N5ROW2.add(50);
        N5ROW2.add(5);

        BasicDBList N5_N7ROW = new BasicDBList();
        N5_N7ROW.add(new BasicDBObject("_id", 7).append("e3", 7));

        BasicDBList N5_N7ROW_ARRAY1 = new BasicDBList();
        N5_N7ROW_ARRAY1.add(N5_N7ROW);

        BasicDBList N5_N7ROW2 = new BasicDBList();
        N5_N7ROW2.add(new BasicDBObject("_id", 5).append("e3", 7));
        N5_N7ROW2.add(new BasicDBObject("_id", 7).append("e3", 7));

        BasicDBList N5_N7ROW_ARRAY2 = new BasicDBList();
        N5_N7ROW_ARRAY2.add(new BasicDBList());
        N5_N7ROW_ARRAY2.add(N5_N7ROW2);

        N1ROWS.add(new BasicDBObject("_id", 1).append("N5", N5ROW).append("N5_N7", N5_N7ROW_ARRAY1));
        N1ROWS.add(new BasicDBObject("_id", 2).append("N5", N5ROW2).append("N5_N7", N5_N7ROW_ARRAY2));

        BasicDBObject match = new BasicDBObject("_id", 1);
        DBCollection dbCollection = helpUpdate(query, new String[]{"N1"}, match, N1ROWS);

        ArgumentCaptor<BasicDBObject> matchCaptor = ArgumentCaptor.forClass(BasicDBObject.class);
        ArgumentCaptor<BasicDBObject> updateCaptor = ArgumentCaptor.forClass(BasicDBObject.class);

        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection, Mockito.times(1)).update(matchCaptor.capture(), updateCaptor.capture(), Mockito.eq(false),
                Mockito.eq(true), Mockito.eq(WriteConcern.ACKNOWLEDGED));

        assertEquals(new BasicDBObject("_id", 2), matchCaptor.getAllValues().get(0));

        BasicDBList EXPECTED_N5_N7ROW2 = new BasicDBList();
        EXPECTED_N5_N7ROW2.add(new BasicDBObject("_id", 5).append("e3", 5));
        EXPECTED_N5_N7ROW2.add(new BasicDBObject("_id", 7).append("e3", 7));

        BasicDBList EXPECTED_N5_N7ROW2_ARRAY = new BasicDBList();
        EXPECTED_N5_N7ROW2_ARRAY.add(EXPECTED_N5_N7ROW2);


        assertEquals(new BasicDBObject("$set", new BasicDBObject("N5.1.N7", EXPECTED_N5_N7ROW2)),
                updateCaptor.getAllValues().get(0));
    }


    @Test
    public void testNestedMergeInsert_One_2_One_Many() throws Exception {
        DBObject n1_match = QueryBuilder.start().and(QueryBuilder.start("N2").exists(true).get(),
                new BasicDBObject("_id", 2)).get();

        BasicDBObject match = new BasicDBObject();
        match.append("_id", 3);

        BasicDBObject n4 = new BasicDBObject();
        n4.append("e3", 3);
        n4.append("_id",1);

        BasicDBObject result = new BasicDBObject("N2.N4", n4);

        String query = "INSERT INTO N4 (e1, e2, e3) VALUES (1,2,3)";
        DBCollection dbCollection = helpUpdate(query, new String[]{"N1"}, match, null);
        Mockito.verify(dbCollection, Mockito.never()).insert(n1_match, WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(n1_match, new BasicDBObject("$push", result), false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test
    public void testNestedMergeInsert_One_2_Many_one() throws Exception {
        DBObject n1_match = QueryBuilder.start().and(QueryBuilder.start("N5").exists(true).get(),
                new BasicDBObject("N5._id", 1)).get();

        BasicDBObject match = new BasicDBObject();
        match.append("_id", 3);

        BasicDBObject n6 = new BasicDBObject();
        n6.append("e2", 2);
        n6.append("e3", 3);

        BasicDBObject result = new BasicDBObject("N5.$.N6", n6);

        String query = "INSERT INTO N6 (e1, e2, e3) VALUES (1,2,3)";
        DBCollection dbCollection = helpUpdate(query, new String[]{"N1"}, match, null);
        Mockito.verify(dbCollection, Mockito.never()).insert(n1_match, WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(n1_match, new BasicDBObject("$set", result), false, true, WriteConcern.ACKNOWLEDGED);
    }

    @Test
    public void testNestedMergeInsert_One_2_Many_Many() throws Exception {
        DBObject n1_match = QueryBuilder.start().and(QueryBuilder.start("N5").exists(true).get(),
                new BasicDBObject("N5._id", 2)).get();

        BasicDBObject match = new BasicDBObject();
        match.append("_id", 3);

        BasicDBObject n7 = new BasicDBObject();
        n7.append("e3", 3);
        n7.append("_id",1);

        BasicDBObject result = new BasicDBObject("N5.$.N7", n7);

        String query = "INSERT INTO N7 (e1, e2, e3) VALUES (1,2,3)";
        DBCollection dbCollection = helpUpdate(query, new String[]{"N1"}, match, null);
        Mockito.verify(dbCollection, Mockito.never()).insert(n1_match, WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(n1_match, new BasicDBObject("$push", result), false, true, WriteConcern.ACKNOWLEDGED);
    }
}
