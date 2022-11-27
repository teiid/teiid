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
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.GeometryType;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.*;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionParameter;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.mongodb.MongoDBConnection;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.function.GeometryUtils;
import org.teiid.query.function.UDFSource;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.TestDDLParser;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

import com.mongodb.AggregationOptions;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

@SuppressWarnings("nls")
public class TestMongoDBQueryExecution {
    private MongoDBExecutionFactory translator;
    private TranslationUtility utility;
    private static AggregationOptions options = AggregationOptions.builder()
            .batchSize(256)
            .allowDiskUse(true)
            .build();

    @Before
    public void setUp() throws Exception {
        this.translator = new MongoDBExecutionFactory();
        this.translator.setDatabaseVersion("2.6");
        this.translator.start();

        MetadataFactory mf = TestDDLParser.helpParse(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("northwind.ddl")), "northwind");

        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "sakila", new FunctionTree("mongo", new UDFSource(translator.getPushDownFunctions())));
        ValidatorReport report = new MetadataValidator().validate(metadata.getVdbMetaData(), metadata.getMetadataStore());
        if (report.hasItems()) {
            throw new RuntimeException(report.getFailureMessage());
        }
        this.utility = new TranslationUtility(metadata);
    }

    private DBCollection helpExecute(String query, String[] expectedCollection) throws TranslatorException {
        Command cmd = this.utility.parseCommand(query);
        return helpExecute(cmd, expectedCollection);
    }
    private DBCollection helpExecute(Command cmd, String[] expectedCollection) throws TranslatorException {
        ExecutionContext context = Mockito.mock(ExecutionContext.class);
        Mockito.stub(context.getBatchSize()).toReturn(256);
        MongoDBConnection connection = Mockito.mock(MongoDBConnection.class);
        DB db = Mockito.mock(DB.class);

        DBCollection dbCollection = Mockito.mock(DBCollection.class);
        for(String collection:expectedCollection) {
            Mockito.stub(db.getCollection(collection)).toReturn(dbCollection);
        }

        Mockito.stub(db.collectionExists(Mockito.anyString())).toReturn(true);
        Mockito.stub(connection.getDatabase()).toReturn(db);

        ResultSetExecution execution = this.translator.createResultSetExecution((QueryExpression)cmd, context,
                this.utility.createRuntimeMetadata(), connection);
        execution.execute();
        return dbCollection;
    }

    @Test
    public void testSimpleSelectNoAssosiations() throws Exception {
        String query = "SELECT * FROM Customers";

        DBCollection dbCollection = helpExecute(query, new String[]{"Customers"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$_id");
        result.append( "_m1","$CompanyName");
        result.append( "_m2","$ContactName");
        result.append( "_m3","$ContactTitle");
        result.append( "_m4","$Address");
        result.append( "_m5","$City");
        result.append( "_m6","$Region");
        result.append( "_m7","$PostalCode");
        result.append( "_m8","$Country");
        result.append( "_m9","$Phone");
        result.append( "_m10","$Fax");

        List<DBObject> pipeline = buildArray(new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testSimpleWhere() throws Exception {
        String query = "SELECT CompanyName, ContactTitle FROM Customers WHERE Country='USA'";

        DBCollection dbCollection = helpExecute(query, new String[]{"Customers"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$CompanyName");
        result.append( "_m1","$ContactTitle");

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$match", new BasicDBObject("Country", "USA")),
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testSelectEmbeddable() throws Exception {
        String query = "SELECT CategoryName FROM Categories";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$CategoryName");

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testSelectEmbeddableWithWhere_ON_NONPK() throws Exception {
        String query = "SELECT CategoryName FROM Categories WHERE CategoryName = 'Drinks'";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$CategoryName");

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$match",new BasicDBObject("CategoryName", "Drinks")),
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testSelectEmbeddableWithWhere_ON_PK() throws Exception {
        String query = "SELECT CategoryName FROM Categories WHERE CategoryID = 10";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$CategoryName");

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$match",new BasicDBObject("_id", 10)),
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testSelectFromMerged() throws Exception {
        String query = "SELECT UnitPrice FROM OrderDetails";

        DBCollection dbCollection = helpExecute(query, new String[]{"Orders"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$OrderDetails.UnitPrice");

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$unwind","$OrderDetails"),
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test // one-2-many
    public void testSelectMergedWithWhere_ON_NON_PK() throws Exception {
        String query = "SELECT Quantity FROM OrderDetails WHERE UnitPrice = '0.99'";

        DBCollection dbCollection = helpExecute(query, new String[]{"Orders"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$OrderDetails.Quantity");

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$unwind","$OrderDetails"),
                        new BasicDBObject("$match", new BasicDBObject("OrderDetails.UnitPrice", 0.99)),
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test // one-2-one
    public void testSelectMergedWithWhere_ON_NON_PK_one_to_one() throws Exception {
        String query = "SELECT cust_id, zip FROM Address WHERE Street = 'Highway 100'";

        DBCollection dbCollection = helpExecute(query, new String[]{"customer"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$_id");
        result.append( "_m1","$address.zip");

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$match", QueryBuilder.start("address").exists("true").notEquals(null).get()),
                        new BasicDBObject("$match", new BasicDBObject("address.street", "Highway 100")),
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test // one-2-one
    public void testSelectONE_TO_ONE() throws Exception {
        String query = "SELECT c.name, a.zip " +
                "FROM customer c join address a " +
                "on c.customer_id=a.cust_id";

        DBCollection dbCollection = helpExecute(query, new String[]{"customer"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$name");
        result.append( "_m1","$address.zip");

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$match", new BasicDBObject("address", new BasicDBObject("$exists", "true").append("$ne", null))),
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test // one-2-one
    public void testSelectMergedWithNOWhere_one_to_one() throws Exception {
        String query = "SELECT cust_id, zip FROM Address";

        DBCollection dbCollection = helpExecute(query, new String[]{"customer"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$_id");
        result.append( "_m1","$address.zip");

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$match", QueryBuilder.start("address").exists("true").notEquals(null).get()),
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testTwoTableInnerJoinEmbeddableAssosiationOne() throws Exception {
        String query = "select p.ProductName, c.CategoryName from Products p " +
                "join Categories c on p.CategoryID = c.CategoryID";

        DBCollection dbCollection = helpExecute(query, new String[]{"Products"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$ProductName");
        result.append( "_m1","$Categories.CategoryName");

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$match", QueryBuilder.start("Categories").exists("true").notEquals(null).get()),
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testTwoTableInnerJoinEmbeddableWithWhere() throws Exception {
        String query = "select p.ProductName, c.CategoryName from Products p " +
                "JOIN Categories c on p.CategoryID = c.CategoryID " +
                "WHERE p.CategoryID = 1 AND c.CategoryID = 1";

        DBCollection dbCollection = helpExecute(query, new String[]{"Products"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$ProductName");
        result.append( "_m1","$Categories.CategoryName");

        DBObject exists = QueryBuilder.start("Categories").exists("true").notEquals(null).get();
        DBObject p1 = QueryBuilder.start("CategoryID").is(1).get();
        DBObject p2 =  QueryBuilder.start("CategoryID").is(1).get();

        DBObject match = QueryBuilder.start().and(p1, p2).get(); // duplicate criteria, mongo should ignore it
        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$match", exists),
                        new BasicDBObject("$match", match),
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testSelectNestedEmbedding()  throws Exception {
        String query = "select T1.e1, T1.e2, T2.t2e1, T2.t2e2, T3.t3e1, T3.t3e2 from T1 "
                + "JOIN T2 ON T1.e1=T2.t2e1 JOIN T3 ON T2.t2e1 = T3.t3e1";

        DBCollection dbCollection = helpExecute(query, new String[]{"T1", "T2", "T3"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$e1");
        result.append( "_m1","$_id");
        result.append( "_m2","$e1");
        result.append( "_m3","$T2.t2e2");
        result.append( "_m4","$e1");
        result.append( "_m5","$T3.t3e2");

        DBObject t2 = QueryBuilder.start("T2").exists("true").notEquals(null).get();
        DBObject t3 = QueryBuilder.start("T3").exists("true").notEquals(null).get();
        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$match", t2),
                new BasicDBObject("$match", t3),
                new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testSelectNestedMerge()  throws Exception {
        String query = "select * from payment";

        DBCollection dbCollection = helpExecute(query, new String[]{"customer"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$rental.payment._id");
        result.append( "_m1","$rental._id");
        result.append( "_m2","$rental.payment.amount");

        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$unwind","$rental"),
                new BasicDBObject("$unwind","$rental.payment"),
                new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }


    @Test // embedded means always nested as doc not as array
    public void testEmbeddedJoin_INNER()  throws Exception {
        String query = "SELECT p.ProductName,s.CompanyName " +
                "FROM Suppliers s " +
                "JOIN " +
                "Products p " +
                "ON s.SupplierID = p.SupplierID";

        DBCollection dbCollection = helpExecute(query, new String[]{"Products"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$ProductName");
        result.append( "_m1","$Suppliers.CompanyName");

        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$match", QueryBuilder.start("Suppliers").exists("true").notEquals(null).get()),
                new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test // embedded means always nested as doc not as array
    public void testEmbeddedJoin_INNER_REVERSE()  throws Exception {
        String query = "SELECT p.ProductName,s.CompanyName " +
                "FROM Products p " +
                "JOIN " +
                "Suppliers s " +
                "ON s.SupplierID = p.SupplierID";

        DBCollection dbCollection = helpExecute(query, new String[]{"Products"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$ProductName");
        result.append( "_m1","$Suppliers.CompanyName");

        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$match", QueryBuilder.start("Suppliers").exists("true").notEquals(null).get()),
                new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test(expected=TranslatorException.class) // embedded means always nested as doc not as array
    public void testEmbeddedJoin_LEFTOUTER()  throws Exception {
        String query = "SELECT p.ProductName,s.CompanyName " +
                "FROM Suppliers s " +
                "LEFT OUTER JOIN " +
                "Products p " +
                "ON s.SupplierID = p.SupplierID";

        DBCollection dbCollection = helpExecute(query, new String[]{"Products"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$ProductName");
        result.append( "_m1","$Suppliers.CompanyName");

        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$match", QueryBuilder.start("SupplierID").notEquals(null).and(QueryBuilder.start("Suppliers._id").notEquals(null).get()).get()),
                new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test // embedded means always nested as doc not as array
    public void testEmbeddedJoin_LEFTOUTER2()  throws Exception {
        String query = "SELECT p.ProductName,s.CompanyName " +
                "FROM  Products p " +
                "LEFT OUTER JOIN " +
                "Suppliers s " +
                "ON s.SupplierID = p.SupplierID";

        DBCollection dbCollection = helpExecute(query, new String[]{"Products"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$ProductName");
        result.append( "_m1","$Suppliers.CompanyName");

        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test // embedded means always nested as doc not as array
    public void testEmbeddedJoin_RIGHTOUTER()  throws Exception {
        String query = "SELECT p.ProductName,s.CompanyName " +
                "FROM Suppliers s " +
                "RIGHT OUTER JOIN " +
                "Products p " +
                "ON s.SupplierID = p.SupplierID";

        DBCollection dbCollection = helpExecute(query, new String[]{"Products"});
        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$ProductName");
        result.append( "_m1","$Suppliers.CompanyName");

        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    // embedded means always nested as doc not as array
    @Test(expected=TranslatorException.class)
    public void testEmbeddedJoin_RIGHTOUTER2()  throws Exception {
        String query = "SELECT p.ProductName,s.CompanyName " +
                "FROM  Products p " +
                "RIGHT OUTER JOIN " +
                "Suppliers s " +
                "ON s.SupplierID = p.SupplierID";

        DBCollection dbCollection = helpExecute(query, new String[]{"Products"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$ProductName");
        result.append( "_m1","$Suppliers.CompanyName");

        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$match", QueryBuilder.start("_id").notEquals(null).get()),
                new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test // merge where one to many relation
    public void testMERGE_ONE_TO_MANY_Join_INNER()  throws Exception {
        String query = "SELECT c.name,n.Comment,n.CustomerId " +
                "FROM customer c " +
                "JOIN " +
                "Notes n " +
                "ON c.customer_id = n.CustomerId";

        DBCollection dbCollection = helpExecute(query, new String[]{"customer"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$name");
        result.append( "_m1","$Notes.Comment");
        result.append( "_m2","$_id");

        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$unwind", "$Notes"),
                new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test // merge where one to many relation
    public void testMERGE_ONE_TO_ONE_Join_INNER_ORDERBY()  throws Exception {
        String query = "SELECT N2.e1 AS c_0, "
                + "N1.e1 AS c_1, N1.e2 AS c_2, "
                + "N1.e3 AS c_3, N2.e2 AS c_4, N2.e3 AS c_5 "
                + "FROM N1 INNER JOIN N2 ON N1.e1 = N2.e1 "
                + "ORDER BY c_0";

        DBCollection dbCollection = helpExecute(query, new String[]{"N1"});

        BasicDBObject result = new BasicDBObject();
        result.append( "c_0","$_id"); // same expr
        result.append( "c_1","$_id"); // same expr
        result.append( "c_2","$e2");
        result.append( "c_3","$e3");
        result.append( "c_4","$N2.e2");
        result.append( "c_5","$N2.e3");

        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$match", QueryBuilder.start("N2").exists("true").notEquals(null).get()),
                new BasicDBObject("$project", result),
                // note c_0, c_1 represent same expressions, so it does not matter
                new BasicDBObject("$sort", new BasicDBObject("c_1", 1)));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test // merge where one to many relation
    public void testMERGE_ONE_TO_MANY_Join_LEFT_OUTER()  throws Exception {
        String query = "SELECT c.name,n.Comment " +
                "FROM customer c " +
                "LEFT JOIN " +
                "Notes n " +
                "ON c.customer_id = n.CustomerId";

        DBCollection dbCollection = helpExecute(query, new String[]{"customer"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$name");
        result.append( "_m1","$__NN_Notes.Comment");

        BasicDBObject ifnull = buildIfNullExpression("Notes");

         BasicDBObject project = new BasicDBObject();
         project.append("customer_id", 1);
         project.append("name", 1);
         project.append("__NN_Notes", ifnull);
        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$project", project),
                new BasicDBObject("$unwind", "$__NN_Notes"),
                new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test // merge where one to many relation - equal to inner join with doc format teiid has
    public void testMERGE_ONE_TO_MANY_Join_LEFT_OUTER4()  throws Exception {
        String query = "SELECT c.name,n.Comment " +
                "FROM customer c " +
                "RIGHT JOIN " +
                "Notes n " +
                "ON c.customer_id = n.CustomerId";

        DBCollection dbCollection = helpExecute(query, new String[]{"customer"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$name");
        result.append( "_m1","$Notes.Comment");

        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$unwind", "$Notes"),
                new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }


    @Test // merge where one to many relation - equal to inner join with doc format teiid has
    public void testMERGE_ONE_TO_MANY_Join_LEFT_OUTER3()  throws Exception {
        String query = "SELECT c.name,n.Comment " +
                "FROM Notes n " +
                "LEFT JOIN " +
                "Customer c " +
                "ON c.customer_id = n.CustomerId";

        DBCollection dbCollection = helpExecute(query, new String[]{"customer"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$name");
        result.append( "_m1","$Notes.Comment");

        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$unwind", "$Notes"),
                new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test // merge where one to many relation (2 merged tables into customer)
    public void testMERGE_ONE_TO_MANY_Join_INNER_OUTER2()  throws Exception {
        String query = "SELECT c.name,n.Comment ,r.amount " +
                "FROM customer c " +
                "LEFT JOIN " +
                "Notes n " +
                "ON c.customer_id = n.CustomerId " +
                "LEFT JOIN rental r ON r.customer_id = c.customer_id";

        //[{ "$project" : { "customer_id" : 1 , "name" : 1 ,
        //"__NN_Notes" : { "$ifNull" : [ "$Notes" , [ { }]]} ,
        //"__NN_rental" : { "customer_id" : 1 , "name" : 1 , "__NN_rental" : { "$ifNull" : [ "$rental" , [ { }]]}}}}, { "$unwind" : "$__NN_Notes"}, { "$unwind" : "$__NN_rental"}, { "$project" : { "_m0" : "$name" , "_m1" : "$__NN_Notes.Comment" , "_m2" : "$__NN_rental.amount"}}],


        DBCollection dbCollection = helpExecute(query, new String[]{"customer"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$name");
        result.append( "_m1","$__NN_Notes.Comment");
        result.append( "_m2","$__NN_rental.amount");

         BasicDBObject project = new BasicDBObject();
         project.append("customer_id", 1);
         project.append("name", 1);
         project.append("__NN_Notes", buildIfNullExpression("Notes"));
         project.append("__NN_rental", buildIfNullExpression("rental"));
        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$project", project),
                new BasicDBObject("$unwind", "$__NN_Notes"),
                new BasicDBObject("$unwind", "$__NN_rental"),
                new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    private BasicDBObject buildIfNullExpression(String table) {
        BasicDBList exprs = new BasicDBList();
        exprs.add("$"+table); //$NON-NLS-1$
        BasicDBList list = new BasicDBList();
        list.add(new BasicDBObject());
        exprs.add(list);
        BasicDBObject ifnull = new BasicDBObject("$ifNull", exprs); //$NON-NLS-1$
        return ifnull;
    }

    @Test
    public void testSimpleGroupBy() throws Exception {
        String query = "SELECT Country FROM Customers GROUP BY Country";

        DBCollection dbCollection = helpExecute(query, new String[]{"Customers"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$_id._c0");

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$group", new BasicDBObject("_id", new BasicDBObject("_c0", "$Country"))),
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testMultipleGroupBy() throws Exception {
        String query = "SELECT Country,City FROM Customers GROUP BY Country,City";

        DBCollection dbCollection = helpExecute(query, new String[]{"Customers"});

        BasicDBObject project = new BasicDBObject();
        project.append( "_m0","$_id._c0");
        project.append( "_m1","$_id._c1");

        BasicDBObject group = new BasicDBObject();
        group.append( "_c0","$Country");
        group.append( "_c1","$City");

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$group", new BasicDBObject("_id", group)),
                        new BasicDBObject("$project", project));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testDistinctSingle() throws Exception {
        String query = "SELECT DISTINCT Country FROM Customers";

        DBCollection dbCollection = helpExecute(query, new String[]{"Customers"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$_id._m0");

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$group", new BasicDBObject("_id", new BasicDBObject("_m0", "$Country"))),
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testDistinctMulti() throws Exception {
        String query = "SELECT DISTINCT Country, City FROM Customers";

        DBCollection dbCollection = helpExecute(query, new String[]{"Customers"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$_id._m0");
        result.append( "_m1","$_id._m1");

        BasicDBObject group = new BasicDBObject();
        group.append( "_m0","$Country");
        group.append( "_m1","$City");

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$group", new BasicDBObject("_id", group)),
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test // embedded means always nested as doc not as array
    public void testONE_TO_ONE_WithGroupBy()  throws Exception {
        String query = "SELECT c.name, a.zip " +
                "FROM customer c join address a " +
                "on c.customer_id=a.cust_id " +
                "GROUP BY c.name, a.zip";

        DBCollection dbCollection = helpExecute(query, new String[]{"customer"});

        BasicDBObject project = new BasicDBObject();
        project.append( "_m0","$_id._c0");
        project.append( "_m1","$_id._c1");

        BasicDBObject group = new BasicDBObject();
        group.append( "_c0","$name");
        group.append( "_c1","$address.zip");

        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$match", new BasicDBObject("address", new BasicDBObject("$exists", "true").append("$ne", null))),
                new BasicDBObject("$group", new BasicDBObject("_id", group)),
                new BasicDBObject("$project", project));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test // embedded means always nested as doc not as array
    public void testONE_TO_ONE_WithGroupByOrderBy()  throws Exception {
        String query = "SELECT c.name, a.zip " +
                "FROM customer c join address a " +
                "on c.customer_id=a.cust_id " +
                "GROUP BY c.name, a.zip " +
                "ORDER BY c.name, a.zip " +
                "limit 2";

        DBCollection dbCollection = helpExecute(query, new String[]{"customer"});

        BasicDBObject project = new BasicDBObject();
        project.append( "_m0","$_id._c0");
        project.append( "_m1","$_id._c1");

        BasicDBObject group = new BasicDBObject();
        group.append( "_c0","$name");
        group.append( "_c1","$address.zip");

        BasicDBObject sort = new BasicDBObject();
        sort.append( "_m0",1);
        sort.append( "_m1",1);

        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$match", new BasicDBObject("address", new BasicDBObject("$exists", "true").append("$ne", null))),
                new BasicDBObject("$group", new BasicDBObject("_id", group)),
                new BasicDBObject("$project", project),
                new BasicDBObject("$sort", sort),
                new BasicDBObject("$skip", 0),
                new BasicDBObject("$limit", 2));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testSumWithGroupBy() throws Exception {
        String query = "SELECT SUM(age) as total FROM users GROUP BY user_id";

        DBCollection dbCollection = helpExecute(query, new String[]{"users"});
        BasicDBObject id = new BasicDBObject();
        id.append( "_c0","$user_id");

        BasicDBObject group = new BasicDBObject("_id", id);
        group.append("total", new BasicDBObject("$sum", "$age"));

        BasicDBObject project = new BasicDBObject();
        project.append( "total",1);

        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$group", group),
                new BasicDBObject("$project", project));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }


    @Test
    public void testSumWithGroupBy2() throws Exception {
        String query = "SELECT user_id, status, SUM(age) as total FROM users GROUP BY user_id, status";

        DBCollection dbCollection = helpExecute(query, new String[]{"users"});

        BasicDBObject project = new BasicDBObject();
        project.append( "_m0","$_id._c0");
        project.append( "_m1","$_id._c1");
        project.append( "total",1);

        BasicDBObject id = new BasicDBObject();
        id.append( "_c0","$user_id");
        id.append( "_c1","$status");

        BasicDBObject group = new BasicDBObject("_id", id);
        group.append("total", new BasicDBObject("$sum", "$age"));

        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$group", group),
                new BasicDBObject("$project", project));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testSumWithGroupBy3() throws Exception {
        String query = "SELECT user_id, SUM(age) as total FROM users GROUP BY user_id";

        DBCollection dbCollection = helpExecute(query, new String[]{"users"});

        BasicDBObject project = new BasicDBObject();
        project.append( "_m0","$_id._c0");
        project.append( "total",1);

        BasicDBObject id = new BasicDBObject();
        id.append( "_c0","$user_id");

        BasicDBObject group = new BasicDBObject("_id", id);
        group.append("total", new BasicDBObject("$sum", "$age"));

        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$group", group),
                new BasicDBObject("$project", project));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testAggregateWithHaving() throws Exception {
        String query = "SELECT SUM(age) as total FROM users GROUP BY user_id HAVING SUM(age) > 250";

        DBCollection dbCollection = helpExecute(query, new String[]{"users"});

        BasicDBObject project = new BasicDBObject();
        project.append( "_m0",1);

        BasicDBObject id = new BasicDBObject();
        id.append( "_c0","$user_id");

        BasicDBObject group = new BasicDBObject("_id", id);
        group.append("_m0", new BasicDBObject("$sum", "$age"));

        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$group", group),
                new BasicDBObject("$match", QueryBuilder.start("_m0").greaterThan(new BasicDBObject("$numberLong", "250")).get()),
                new BasicDBObject("$project", project));
        ArgumentCaptor<List> actualCapture = ArgumentCaptor.forClass(List.class);
        Mockito.verify(dbCollection).aggregate(actualCapture.capture(), Mockito.any(AggregationOptions.class));
        Assert.assertEquals(pipeline.toString(), actualCapture.getValue().toString());
    }

    @Test
    public void testAggregateWithHavingAndWhere() throws Exception {
        String query = "SELECT SUM(age) as total FROM users WHERE age > 45 GROUP BY user_id HAVING SUM(age) > 250";

        DBCollection dbCollection = helpExecute(query, new String[]{"users"});

        BasicDBObject project = new BasicDBObject();
        project.append( "_m0",1);

        BasicDBObject id = new BasicDBObject();
        id.append( "_c0","$user_id");

        BasicDBObject group = new BasicDBObject("_id", id);
        group.append("_m0", new BasicDBObject("$sum", "$age"));

        ArrayList<DBObject> pipeline = buildArray(
                new BasicDBObject("$match", QueryBuilder.start("age").greaterThan(45).get()),
                new BasicDBObject("$group", group),
                new BasicDBObject("$match", QueryBuilder.start("_m0").greaterThan(new BasicDBObject("$numberLong", "250")).get()),
                new BasicDBObject("$project", project));

        ArgumentCaptor<List> actualCapture = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<AggregationOptions> optionsCapture = ArgumentCaptor.forClass(AggregationOptions.class);
        Mockito.verify(dbCollection).aggregate(actualCapture.capture(), optionsCapture.capture());
        Assert.assertEquals(pipeline.toString(), actualCapture.getValue().toString());
        Assert.assertEquals(options.toString(), optionsCapture.getValue().toString());
    }

    public static ArrayList<DBObject> buildArray(DBObject ...basicDBObjects){
        ArrayList<DBObject> list = new ArrayList<DBObject>();
        for (DBObject obj:basicDBObjects) {
            list.add(obj);
        }
        return list;
    }

    @Test
    public void testCountStar() throws Exception {
        String query = "SELECT count(*) FROM Categories";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"});

        BasicDBObject group = new BasicDBObject();
        group.append( "_id", null);
        group.append( "_m0", new BasicDBObject("$sum", 1));

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0", 1);

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$group", group),
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testCountOnColumn() throws Exception {
        String query = "SELECT count(CategoryName) FROM Categories";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"});

        BasicDBList eq = new BasicDBList();
        eq.add(0, "$CategoryName");
        eq.add(1, null);
        BasicDBList values = new BasicDBList();
        values.add(0, new BasicDBObject("$eq", eq)); //$NON-NLS-1$
        values.add(1, 0);
        values.add(2, 1);
        BasicDBObject expr = new BasicDBObject("$sum",new BasicDBObject("$cond", values)); //$NON-NLS-1$ //$NON-NLS-2$

        BasicDBObject group = new BasicDBObject();
        group.append( "_m0", expr);
        group.append( "_id", null);

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0", 1);

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$group", group),
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testCountOnmultipleDifferentColumns() throws Exception {
        String query = "SELECT count(CategoryName), avg(CategoryID) FROM Categories";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"});

        BasicDBList eq = new BasicDBList();
        eq.add(0, "$CategoryName");
        eq.add(1, null);
        BasicDBList values = new BasicDBList();
        values.add(0, new BasicDBObject("$eq", eq)); //$NON-NLS-1$
        values.add(1, 0);
        values.add(2, 1);
        BasicDBObject expr = new BasicDBObject("$sum",new BasicDBObject("$cond", values)); //$NON-NLS-1$ //$NON-NLS-2$

        BasicDBObject group = new BasicDBObject();
        group.append( "_m0", expr);
        group.append( "_m1", new BasicDBObject("$avg", "$_id"));
        group.append( "_id", null);

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0", 1);
        result.append( "_m1", 1);

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$group", group),
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testMultipleAggregateWithCountOnSameColumn() throws Exception {
        String query = "SELECT count(CategoryName), avg(CategoryName) FROM Categories";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"});


        BasicDBList eq = new BasicDBList();
        eq.add(0, "$CategoryName");
        eq.add(1, null);
        BasicDBList values = new BasicDBList();
        values.add(0, new BasicDBObject("$eq", eq)); //$NON-NLS-1$
        values.add(1, 0);
        values.add(2, 1);
        BasicDBObject expr = new BasicDBObject("$sum",new BasicDBObject("$cond", values)); //$NON-NLS-1$ //$NON-NLS-2$

        BasicDBObject group = new BasicDBObject();
        group.append( "_m0", expr);
        group.append( "_m1", new BasicDBObject("$avg", "$CategoryName"));
        group.append( "_id", null);

        BasicDBObject project = new BasicDBObject();
        project.append( "_m0", 1);
        project.append( "_m1", 1);

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$group", group),
                        new BasicDBObject("$project", project));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testMultipleAggregateOnSameColumn() throws Exception {
        String query = "SELECT sum(CategoryName), avg(CategoryName) FROM Categories";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"});

        BasicDBObject group = new BasicDBObject();
        group.append( "_id", null);
        group.append( "_m0", new BasicDBObject("$sum", "$CategoryName"));
        group.append( "_m1", new BasicDBObject("$avg", "$CategoryName"));

        BasicDBObject project = new BasicDBObject();
        project.append( "_m0", 1);
        project.append( "_m1", 1);

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$group", group),
                        new BasicDBObject("$project", project));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testMultipleAggregateOnSameColumnWithGroupBy() throws Exception {
        String query = "SELECT sum(CategoryName), avg(CategoryName) FROM Categories Group By Picture";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"});

        BasicDBObject group = new BasicDBObject();
        group.append( "_id", new BasicDBObject("_c0", "$Picture"));
        group.append( "_m0", new BasicDBObject("$sum", "$CategoryName"));
        group.append( "_m1", new BasicDBObject("$avg", "$CategoryName"));

        BasicDBObject project = new BasicDBObject();
        project.append( "_m0", 1);
        project.append( "_m1", 1);

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$group", group),
                        new BasicDBObject("$project", project));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testMultipleAggregateOnSameColumn_withCountSTAR() throws Exception {
        String query = "SELECT count(*), avg(CategoryName) FROM Categories";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"});

        BasicDBObject group = new BasicDBObject();
        group.append( "_id", null);
        group.append( "_m0", new BasicDBObject("$sum", 1));
        group.append( "_m1", new BasicDBObject("$avg", "$CategoryName"));

        BasicDBObject project = new BasicDBObject();
        project.append( "_m0", 1);
        project.append( "_m1", 1);

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$group", group),
                        new BasicDBObject("$project", project));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testFunctionInWhere() throws Exception {
        String query = "SELECT CategoryName FROM Categories WHERE CONCAT(CategoryName, '2') = '2'";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"});

        // { "$project" : { "_m0" : { "$concat" : [ "$CategoryName" , "2"]} , "_m1" : "$CategoryName"}},
        BasicDBList params = new BasicDBList();
        params.add("$CategoryName");
        params.add("2");

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0", new BasicDBObject("$concat", params));
        result.append( "CategoryName", "$CategoryName");

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$project", result),
                        new BasicDBObject("$match", QueryBuilder.start("_m0").is("2").get()));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testDateFunction() throws Exception {
        String query = "SELECT YEAR(e2) FROM TIME_TEST";

        DBCollection dbCollection = helpExecute(query, new String[]{"TIME_TEST"});

        BasicDBList params = new BasicDBList();
        params.add("$e2");

        BasicDBList values = new BasicDBList();
        values.add(0, "$e2");
        values.add(1, false);
        BasicDBObject isNull = new BasicDBObject("$ifNull", values);

        BasicDBObject func = new BasicDBObject("$year", params);
        BasicDBObject expr = buildCondition(isNull, func, null);

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0", expr);

        //{ "$cond" : [ { "$ifNull" : [ "$e2" ,  false ]} ,  { "$year" : [ "$e2"]}, null]}

        List<DBObject> pipeline = buildArray(new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testSubStr() throws Exception {
        String query = "SELECT SUBSTRING(CategoryName, 3) FROM Categories";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"});

        //{ "$subtract" : [ 3 , 1]}
        BasicDBList subtract = new BasicDBList();
        subtract.add(3);
        subtract.add(1);

        BasicDBList params = new BasicDBList();
        params.add("$CategoryName");
        params.add(new BasicDBObject("$subtract", subtract));
        params.add(4000);

        DBObject ne = buildNE("$CategoryName", null);
        BasicDBObject func = new BasicDBObject("$substr", params);
        BasicDBObject expr = buildCondition(ne, func, null);

        //{ "$project" : { "_m0" : { "$substr" : [ "$CategoryName" , 1 , 4000]}}}
        BasicDBObject result = new BasicDBObject();
        result.append( "_m0", expr);

        List<DBObject> pipeline = buildArray(new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testToLower() throws Exception {
        String query = "SELECT LCASE(CategoryName) FROM Categories";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"});

        DBObject ne = buildNE("$CategoryName", null);
        BasicDBObject func = new BasicDBObject("$toLower", "$CategoryName");
        BasicDBObject expr = buildCondition(ne, func, null);

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0", expr);

        List<DBObject> pipeline = buildArray(new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    private BasicDBObject buildCondition(Object expr, Object trueExpr, Object falseExpr) {
        BasicDBList values = new BasicDBList();
        values.add(0, expr);
        values.add(1, trueExpr);
        values.add(2, falseExpr);
        return new BasicDBObject("$cond", values);
    }

    private BasicDBObject buildNE(Object leftExpr, Object rightExpr) {
        BasicDBList values = new BasicDBList();
        values.add(0, leftExpr);
        values.add(1, rightExpr);
        return new BasicDBObject("$ne", values);
    }

    @Test
    public void testSubStr2() throws Exception {
        String query = "SELECT SUBSTRING(CategoryName, CategoryID, 4) FROM Categories";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"});

        BasicDBList subtract = new BasicDBList();
        subtract.add("$_id");
        subtract.add(1);

        BasicDBList params = new BasicDBList();
        params.add("$CategoryName");
        params.add(new BasicDBObject("$subtract", subtract));
        params.add(4);

        DBObject ne = buildNE("$CategoryName", null);
        BasicDBObject func = new BasicDBObject("$substr", params);
        BasicDBObject expr = buildCondition(ne, func, null);

        //{ "$project" : { "_m0" : { "$substr" : [ "$CategoryName" , 1 , 4000]}}}
        BasicDBObject result = new BasicDBObject();
        result.append( "_m0", expr);

        List<DBObject> pipeline = buildArray(new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }


    @Test
    public void testSelectConstant() throws Exception {
        String query = "SELECT 'hit' FROM Categories";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0", new BasicDBObject("$literal", "hit"));

        List<DBObject> pipeline = buildArray(new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testOffsetWithoutLimit() throws Exception {
        String query = "SELECT CategoryName FROM Categories OFFSET 45 ROWS";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0", "$CategoryName");

        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$project", result),
                new BasicDBObject("$skip", 45));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }


    @Test
    public void testArrayType() throws Exception {
        String query = "SELECT * FROM ArrayTest";

        DBCollection dbCollection = helpExecute(query, new String[]{"ArrayTest"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0", "$id");
        result.append("_m1", "$column1");

        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));

        //empty list should map to an empty array
        assertArrayEquals(new String[0], (String[])this.translator.retrieveValue(new BasicDBList(), String[].class, Mockito.mock(DB.class), "fqn", "col"));
    }

    @Test
    public void testArrtyTypeInWhere() throws Exception {
        String query = "SELECT * FROM ArrayTest where column1 is not null";

        DBCollection dbCollection = helpExecute(query, new String[]{"ArrayTest"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0", "$id");
        result.append("_m1", "$column1");

        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$match", QueryBuilder.start("column1").notEquals(null).get()),
                new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testGeoFunctionInWhere() throws Exception {
        String query = "SELECT CategoryName FROM Categories WHERE mongo.geoWithin(CategoryName, 'Polygon', ((cast(1.0 as double), cast(2.0 as double)),(cast(3.0 as double), cast(4.0 as double)))) or CategoryID=1";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"});

        BasicDBObjectBuilder builder = new BasicDBObjectBuilder();
        builder.push("CategoryName");
        builder.push("$geoWithin");//$NON-NLS-1$
        builder.push("$geometry");//$NON-NLS-1$
        builder.add("type", "Polygon");//$NON-NLS-1$
        BasicDBList coordinates = new BasicDBList();

        BasicDBList pointOne = new BasicDBList();
        pointOne.add(new Double("1.0"));
        pointOne.add(new Double("2.0"));

        BasicDBList pointTwo = new BasicDBList();
        pointTwo.add(new Double("3.0"));
        pointTwo.add(new Double("4.0"));

        BasicDBList points = new BasicDBList();
        points.add(pointOne);
        points.add(pointTwo);

        coordinates.add(points);
        builder.add("coordinates", coordinates); //$NON-NLS-1$

        QueryBuilder qb = QueryBuilder.start().or(builder.get(), new BasicDBObject("_id", 1));
        BasicDBObject result = new BasicDBObject();
        result.append( "_m1", "$CategoryName");

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$match", qb.get()),
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    private FunctionMethod getFunctionMethod(String name) {
        for (FunctionMethod fm: this.translator.getPushDownFunctions()) {
            if (fm.getName().equalsIgnoreCase(name)) {
                for (FunctionParameter fp:fm.getInputParameters()) {
                    if (fp.getRuntimeType().equals(DataTypeManager.DefaultDataTypes.GEOMETRY)) {
                        return fm;
                    }
                }
            }
        }
        return null;
    }

    @Test
    public void testGeoFunctionInWhereWithGeometry() throws Exception {
        Table table = this.utility.createRuntimeMetadata().getTable("northwind.Categories");
        NamedTable namedTable = new NamedTable("Categories", "g0", table);
        ColumnReference colRef = new ColumnReference(namedTable, "CategoryName", table.getColumnByName("CategoryName"), String.class);
        DerivedColumn col = new DerivedColumn("CategoryName", colRef);
        Select select = new Select();
        select.setDerivedColumns(Arrays.asList(col));
        List<TableReference> tables = new ArrayList<TableReference>();
        tables.add(namedTable);
        select.setFrom(tables);

        final GeometryType geo = GeometryUtils.geometryFromClob(new ClobType(new ClobImpl("POLYGON ((1.0 2.0,3.0 4.0,5.0 6.0,1.0 2.0))")));
        Function function = new Function("mongo.geoWithin", Arrays.asList(colRef, new Literal(geo, GeometryType.class)), //$NON-NLS-1$
                Boolean.class); //$NON-NLS-2$
        function.setMetadataObject(getFunctionMethod("mongo.geoWithin"));

        Comparison c = new Comparison(function, new Literal(true, Boolean.class), Comparison.Operator.EQ);
        select.setWhere(c);

        DBCollection dbCollection = helpExecute(select, new String[]{"Categories"});

        BasicDBObjectBuilder builder = new BasicDBObjectBuilder();
        builder.push("CategoryName");
        builder.push("$geoWithin");//$NON-NLS-1$
        builder.add("$geometry", "{\"type\":\"Polygon\",\"coordinates\":[[[1.0,2.0],[3.0,4.0],[5.0,6.0],[1.0,2.0]]]}");//$NON-NLS-1$
        BasicDBObject result = new BasicDBObject();
        result.append( "CategoryName", "$CategoryName");

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$match", builder.get()),
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test(expected=TranslatorException.class)
    public void testGeoFunctionInWhereWithFalse() throws Exception {
        String query = "SELECT CategoryName FROM Categories WHERE mongo.geoWithin(CategoryName, 'Polygon', ((cast(1.0 as double), cast(2.0 as double)),(cast(3.0 as double), cast(4.0 as double)))) = false";
        helpExecute(query, new String[]{"Categories"});
    }

    @Test
    public void testAdd() throws Exception {
        String query = "SELECT SupplierID+1 FROM Suppliers";

        DBCollection dbCollection = helpExecute(query, new String[]{"Suppliers"});
        //{ "$project" : { "_m0" : { "$add" : [ "$_id" , 1]}}}
        BasicDBObject result = new BasicDBObject();
        result.append( "_m0",new BasicDBObject("$add", buildObjectArray("$_id", 1)));

        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    ArrayList<Object> buildObjectArray(Object ...objs){
        ArrayList<Object> list = new ArrayList<Object>();
        for (Object obj:objs) {
            list.add(obj);
        }
        return list;
    }

    @Test
    public void testNextWithGroupAndOrder() throws Exception {
        String query = "select \"FirstName\" from \"TeiidArray\" group by \"FirstName\" order by \"FirstName\" limit 1000";

        String[] expectedCollection = new String[]{"TeiidArray"};

        TransformationMetadata metadata = RealMetadataFactory.fromDDL("CREATE FOREIGN TABLE TeiidArray (ID String PRIMARY KEY, FirstName varchar(25), LastName varchar(25), Score object[]) OPTIONS(UPDATABLE 'TRUE');", "x", "y");

        TranslationUtility util = new TranslationUtility(metadata);

        Command cmd = util.parseCommand(query);
        ExecutionContext context = Mockito.mock(ExecutionContext.class);
        Mockito.stub(context.getBatchSize()).toReturn(256);
        MongoDBConnection connection = Mockito.mock(MongoDBConnection.class);
        DB db = Mockito.mock(DB.class);

        DBCollection dbCollection = Mockito.mock(DBCollection.class);
        for(String collection:expectedCollection) {
            Mockito.stub(db.getCollection(collection)).toReturn(dbCollection);
        }

        Cursor c = Mockito.mock(Cursor.class);

        Mockito.stub(c.hasNext()).toAnswer(new Answer<Boolean>() {
            boolean next = true;
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                if (next) {
                    next = false;
                    return true;
                }
                return false;
            }
        });

        DBObject dbo = Mockito.mock(DBObject.class);

        Mockito.stub(c.next()).toReturn(dbo);

        Mockito.stub(dbCollection.aggregate((List<DBObject>)Mockito.anyList(), (AggregationOptions)Mockito.anyObject())).toReturn(c);

        Mockito.stub(db.collectionExists(Mockito.anyString())).toReturn(true);
        Mockito.stub(connection.getDatabase()).toReturn(db);

        ResultSetExecution execution = this.translator.createResultSetExecution((QueryExpression)cmd, context,
                util.createRuntimeMetadata(), connection);
        execution.execute();
        execution.next();
    }

    @Test
    public void testNestedMergeSelect_one_2_one() throws Exception {
        String query = "SELECT e1, e2, e3 FROM N3";

        DBCollection dbCollection = helpExecute(query, new String[]{"N1"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$_id");
        result.append( "_m1","$N2.N3.e2");
        result.append( "_m2","$N2.N3.e3");

        DBObject n2 = QueryBuilder.start("N2").exists("true").notEquals(null).get();
        DBObject n3 = QueryBuilder.start("N2.N3").exists("true").notEquals(null).get();
        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$match",n2),
                        new BasicDBObject("$match",n3),
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testNestedMergeSelect_one_2_one_inner_joined() throws Exception {
        String query = "select N1.e1, N2.e2, N3.e3 FROM N1 JOIN N2 ON N1.e1=N2.e1 JOIN N3 ON N2.e1 = N3.e1";

        DBCollection dbCollection = helpExecute(query, new String[]{"N1"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$_id");
        result.append( "_m1","$N2.e2");
        result.append( "_m2","$N2.N3.e3");

        DBObject n2 = QueryBuilder.start("N2").exists("true").notEquals(null).get();
        DBObject n3 = QueryBuilder.start("N2.N3").exists("true").notEquals(null).get();

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$match",n2),
                        new BasicDBObject("$match",n3),
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testNestedMergeSelect_one_2_many() throws Exception {
        String query = "SELECT e1, e2, e3 FROM N4 Where N4.e3 = 4";

        DBCollection dbCollection = helpExecute(query, new String[]{"N1"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$N2.N4._id");
        result.append( "_m1","$_id");
        result.append( "_m2","$N2.N4.e3");

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$match", QueryBuilder.start("N2").exists("true").notEquals(null).get()),
                        new BasicDBObject("$unwind", "$N2.N4"),
                        new BasicDBObject("$match", QueryBuilder.start("N2.N4.e3").is(4).get()),
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testNestedMergeSelect_one_2_many_onid() throws Exception {
        String query = "SELECT e1, e2, e3 FROM N4 Where N4.e2 = 4";

        DBCollection dbCollection = helpExecute(query, new String[]{"N1"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$N2.N4._id");
        result.append( "_m1","$_id");
        result.append( "_m2","$N2.N4.e3");

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$match", QueryBuilder.start("N2").exists("true").notEquals(null).get()),
                        new BasicDBObject("$unwind", "$N2.N4"),
                        new BasicDBObject("$match", QueryBuilder.start("_id").is(4).get()),
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testNestedMergeSelect_one_2_many_inner_joined() throws Exception {
        String query = "select N1.e1, N2.e2, N4.e3 FROM N1 JOIN N2 ON N1.e1=N2.e1 JOIN N4 ON N2.e1 = N4.e2 Order by N1.e1";

        DBCollection dbCollection = helpExecute(query, new String[]{"N1"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$_id");
        result.append( "_m1","$N2.e2");
        result.append( "_m2","$N2.N4.e3");

        DBObject n2 = QueryBuilder.start("N2").exists("true").notEquals(null).get();

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$match", n2),
                        new BasicDBObject("$unwind", "$N2.N4"),
                        new BasicDBObject("$project", result),
                        new BasicDBObject("$sort", new BasicDBObject("_m0", 1)));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testNestedMergeSelect_one_2_many2() throws Exception {
        String query = "select N2.*, N4.* FROM N2 LEFT OUTER JOIN N4 ON N2.e1 = N4.e2";

        DBCollection dbCollection = helpExecute(query, new String[]{"N1"});

        // [{ "$project" : { "N2.e1" : 1 , "N2.e2" : 1 , "N2.e3" : 1 , "__NN_N4" : { "$ifNull" : [ "$N2.N4" , [ { }]]}}},
        //{ "$match" : { "N2" : { "$exists" : "true" , "$ne" :  null }}}, { "$unwind" : "$__NN_N4"}, { "$project" : { "c_0" : "$_id" , "c_1" : "$N2.e2" , "c_2" : "$N2.e3" , "c_3" : "$__NN_N4._id" , "c_4" : "$_id" , "c_5" : "$__NN_N4.e3"}}, { "$skip" : 0}, { "$limit" : 100}]
        BasicDBObject projection = new BasicDBObject();
        projection.append( "N2.e1", 1);
        projection.append( "N2.e2", 1);
        projection.append( "N2.e3", 1);
        projection.append("__NN_N4", buildIfNullExpression("N2.N4"));

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$_id");
        result.append( "_m1","$N2.e2");
        result.append( "_m2","$N2.e3");
        result.append( "_m3","$__NN_N4._id");
        result.append( "_m4","$_id");
        result.append( "_m5","$__NN_N4.e3");

        DBObject n2 = QueryBuilder.start("N2").exists("true").notEquals(null).get();

        List<DBObject> pipeline = buildArray(
                new BasicDBObject("$project", projection),
                        new BasicDBObject("$match", n2),
                        new BasicDBObject("$unwind", "$__NN_N4"),
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

    @Test
    public void testNestedMergeSelect_inner_and_left_joined() throws Exception {
        String query = "select N1.e1, N2.e2, N4.e1, N4.e2, N4.e3  "
                + "FROM N1 JOIN N2 ON N1.e1=N2.e1 "
                + "LEFT JOIN N4 ON N2.e1 = N4.e2 ";


        DBCollection dbCollection = helpExecute(query, new String[]{"N1"});

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0","$_id");
        result.append( "_m1","$N2.e2");
        result.append( "_m2","$__NN_N4._id");
        result.append( "_m3","$_id");
        result.append( "_m4","$__NN_N4.e3");

        BasicDBObject projection = new BasicDBObject();
        projection.append( "e1", 1);
        projection.append( "e2", 1);
        projection.append( "e3", 1);
        projection.append("__NN_N4", buildIfNullExpression("N2.N4"));

        DBObject n2 = QueryBuilder.start("N2").exists("true").notEquals(null).get();

        List<DBObject> pipeline = buildArray(
                        new BasicDBObject("$match", n2),
                        new BasicDBObject("$project", projection),
                        new BasicDBObject("$unwind", "$__NN_N4"),
                        new BasicDBObject("$project", result));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));
    }

}
