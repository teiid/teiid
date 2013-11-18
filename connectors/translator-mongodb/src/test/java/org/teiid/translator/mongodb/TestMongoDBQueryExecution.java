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
package org.teiid.translator.mongodb;

import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.mongodb.MongoDBConnection;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

@SuppressWarnings("nls")
public class TestMongoDBQueryExecution {
    private MongoDBExecutionFactory translator;
    private TranslationUtility utility;

    @Before
    public void setUp() throws Exception {
    	this.translator = new MongoDBExecutionFactory();
    	this.translator.start();

    	TransformationMetadata metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("northwind.ddl")), "sakila", "northwind");
    	this.utility = new TranslationUtility(metadata);
    }

	private DBCollection helpExecute(String query, String[] expectedCollection, int expectedParameters) throws TranslatorException {
		Command cmd = this.utility.parseCommand(query);
		ExecutionContext context = Mockito.mock(ExecutionContext.class);
		MongoDBConnection connection = Mockito.mock(MongoDBConnection.class);
		DB db = Mockito.mock(DB.class);

		DBCollection dbCollection = Mockito.mock(DBCollection.class);
		for(String collection:expectedCollection) {
			Mockito.stub(db.getCollection(collection)).toReturn(dbCollection);
		}

		AggregationOutput output = Mockito.mock(AggregationOutput.class);
		Mockito.stub(output.results()).toReturn(new ArrayList<DBObject>());

		ArrayList<DBObject> params = new ArrayList<DBObject>();
		for (int i = 0; i < expectedParameters; i++) {
			params.add(Mockito.any(DBObject.class));
		}

		Mockito.stub(dbCollection.aggregate(params.remove(0), params.toArray(new DBObject[params.size()]))).toReturn(output);

		Mockito.stub(db.collectionExists(Mockito.anyString())).toReturn(true);
		Mockito.stub(connection.getDatabase()).toReturn(db);

		Mockito.stub(db.getCollectionFromString(Mockito.anyString())).toReturn(dbCollection);

		ResultSetExecution execution = this.translator.createResultSetExecution((QueryExpression)cmd, context, this.utility.createRuntimeMetadata(), connection);
		execution.execute();
		return dbCollection;
	}

	@Test
	public void testSimpleSelectNoAssosiations() throws Exception {
		String query = "SELECT * FROM Customers";

		DBCollection dbCollection = helpExecute(query, new String[]{"Customers"}, 1);

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

		Mockito.verify(dbCollection).aggregate(new BasicDBObject("$project", result));
	}

	@Test
	public void testSimpleWhere() throws Exception {
		String query = "SELECT CompanyName, ContactTitle FROM Customers WHERE Country='USA'";

		DBCollection dbCollection = helpExecute(query, new String[]{"Customers"}, 2);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$CompanyName");
	    result.append( "_m1","$ContactTitle");

		Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$match", new BasicDBObject("Country", "USA")),
						new BasicDBObject("$project", result));
	}

	@Test
	public void testSelectEmbeddable() throws Exception {
		String query = "SELECT CategoryName FROM Categories";

		DBCollection dbCollection = helpExecute(query, new String[]{"Categories"}, 1);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$CategoryName");

		Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$project", result));
	}

	@Test
	public void testSelectFromMerged() throws Exception {
		String query = "SELECT UnitPrice FROM OrderDetails";

		DBCollection dbCollection = helpExecute(query, new String[]{"Orders"}, 2);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$OrderDetails.UnitPrice");

		Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$unwind","$OrderDetails"),
						new BasicDBObject("$project", result));
	}

	@Test
	public void testSelectMergedWithWhere() throws Exception {
		String query = "SELECT * FROM OrderDetails WHERE odID = 10248";

		DBCollection dbCollection = helpExecute(query, new String[]{"Orders"}, 3);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$OrderDetails._id.odID");
	    result.append( "_m1","$OrderDetails._id.ProductID");
	    result.append( "_m2","$OrderDetails.UnitPrice");
	    result.append( "_m3","$OrderDetails.Quantity");
	    result.append( "_m4","$OrderDetails.Discount");


		Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$unwind","$OrderDetails"),
						new BasicDBObject("$match", new BasicDBObject("OrderDetails._id.odID.$id", 10248)),
						new BasicDBObject("$project", result));
	}

	@Test
	public void testTwoTableInnerJoinMergeAssosiationMany() throws Exception {
		String query = "SELECT o.CustomerID, od.ProductID FROM Orders o JOIN OrderDetails od ON o.OrderID=od.odID";

		DBCollection dbCollection = helpExecute(query, new String[]{"Orders"}, 2);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$CustomerID");
	    result.append( "_m1","$OrderDetails._id.ProductID");

		Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$unwind","$OrderDetails"),
						new BasicDBObject("$project", result));
	}

	@Test
	public void testTwoTableInnerJoinEmbeddableAssosiationOne() throws Exception {
		String query = "select p.ProductName, c.CategoryName from Products p join Categories c on p.CategoryID = c.CategoryID";

		DBCollection dbCollection = helpExecute(query, new String[]{"Products"}, 1);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$ProductName");
	    result.append( "_m1","$Categories.CategoryName");

		Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$project", result));
	}

	@Test
	public void testTwoTableInnerJoinEmbeddableWithWhere() throws Exception {
		String query = "select p.ProductName, c.CategoryName from Products p " +
				"JOIN Categories c on p.CategoryID = c.CategoryID " +
				"WHERE p.CategoryID = 1 AND c.CategoryID = 1";

		DBCollection dbCollection = helpExecute(query, new String[]{"Products"}, 2);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$ProductName");
	    result.append( "_m1","$Categories.CategoryName");

	    DBObject p1 = QueryBuilder.start("CategoryID.$id").is(1).get();
	    DBObject p2 =  QueryBuilder.start("Categories._id").is(1).get();

	    DBObject match = QueryBuilder.start().and(p1, p2).get();
		Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$match", match),
						new BasicDBObject("$project", result));
	}

	@Test
	public void testThreeTableInnerJoin() throws Exception {
		String query = "SELECT o.CustomerID, od.ProductID, s.CompanyName " +
				"FROM Orders o JOIN OrderDetails od ON o.OrderID=od.odID " +
				"JOIN Shippers s ON o.ShipVia = s.ShipperID";

		DBCollection dbCollection = helpExecute(query, new String[]{"Orders"}, 2);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$CustomerID");
	    result.append( "_m1","$OrderDetails._id.ProductID");
	    result.append( "_m2","$Shippers.CompanyName");

	    Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$unwind","$OrderDetails"),
						new BasicDBObject("$project", result));
	}

	@Test
	public void testLeftOuterJoin() throws Exception {
		String query = "SELECT Orders.CustomerID, OrderDetails.ProductID FROM Orders " +
				"LEFT OUTER JOIN OrderDetails ON Orders.OrderID = OrderDetails.odID " +
				"WHERE OrderDetails.odID IS NOT NULL";

		DBCollection dbCollection = helpExecute(query, new String[]{"Orders"}, 3);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$CustomerID");
	    result.append( "_m1","$OrderDetails._id.ProductID");

	    DBObject match = QueryBuilder.start("OrderDetails._id.odID.$id").notEquals(null).get();
	    Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$unwind","$OrderDetails"),
						new BasicDBObject("$match",match),
						new BasicDBObject("$project", result));
	}


    @Test
    public void testSelectNestedEmbedding()  throws Exception {
    	String query = "select T1.e1, T2.e1, T3.e1 from T1 JOIN T2 ON T1.e1=T2.e1 JOIN T3 ON T2.e1 = T3.e1";

		DBCollection dbCollection = helpExecute(query, new String[]{"T1", "T2", "T3"}, 1);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$e1");
	    result.append( "_m1","$T2._id");
	    result.append( "_m2","$T2.T3._id");

	    Mockito.verify(dbCollection).aggregate(new BasicDBObject("$project", result));
    }

    @Test
    public void testSelectNestedMerge()  throws Exception {
    	String query = "select * from payment";

		DBCollection dbCollection = helpExecute(query, new String[]{"customer"}, 3);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$rental.payment._id");
	    result.append( "_m1","$rental.payment.rental_id");
	    result.append( "_m2","$rental.payment.amount");

	    Mockito.verify(dbCollection).aggregate(
				new BasicDBObject("$unwind","$rental"),
				new BasicDBObject("$unwind","$rental.payment"),
				new BasicDBObject("$project", result));
    }
}
