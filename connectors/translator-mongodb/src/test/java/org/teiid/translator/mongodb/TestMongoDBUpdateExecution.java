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
import org.teiid.mongodb.MongoDBConnection;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

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

	private DBCollection helpExecute(String query, String[] expectedCollection, DBObject match) throws TranslatorException {
		Command cmd = this.utility.parseCommand(query);
		ExecutionContext context = Mockito.mock(ExecutionContext.class);
		MongoDBConnection connection = Mockito.mock(MongoDBConnection.class);
		DB db = Mockito.mock(DB.class);
		DBCollection dbCollection = Mockito.mock(DBCollection.class);
		for(String collection:expectedCollection) {
			Mockito.stub(db.getCollection(collection)).toReturn(dbCollection);
		}
		Mockito.stub(db.collectionExists(Mockito.anyString())).toReturn(true);
		Mockito.stub(connection.getDatabase()).toReturn(db);

		Mockito.stub(db.getCollectionFromString(Mockito.anyString())).toReturn(dbCollection);
		Mockito.stub(dbCollection.findOne(Mockito.any())).toReturn(match);

		UpdateExecution execution = this.translator.createUpdateExecution(cmd, context, this.utility.createRuntimeMetadata(), connection);
		execution.execute();
		return dbCollection;
	}

	@Test
	public void testSimpleInsertNoAssosiations() throws Exception {
		String query = "insert into Customers (CustomerID,CompanyName,ContactName,"
				+ "ContactTitle,Address,City,Region,"
				+ "PostalCode,Country,Phone,Fax) VALUES ('11', 'jboss', 'teiid', 'Mr.Lizard', "
				+ "'jboss.org', 'internet', 'all', '1111', 'US', '555-1212', '555-1212')";

		DBCollection dbCollection = helpExecute(query, new String[]{"Customers"}, null);

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

		DBCollection dbCollection = helpExecute(query, new String[]{"Categories"}, null);
	    BasicDBObject result = new BasicDBObject();
		result.append("_id", 12);
		result.append("CategoryName", "mongo");
		result.append("Description" , "mongo with sql");
		result.append("Picture",null);
		Mockito.verify(dbCollection).insert(result, WriteConcern.ACKNOWLEDGED);
		Mockito.verify(dbCollection, Mockito.never()).update(new BasicDBObject(), result, false, true, WriteConcern.ACKNOWLEDGED);
	}

	@Test
	public void testEmbedInInsert() throws Exception {
		// tests one-to-many situation
		String query = "INSERT INTO OrderDetails (odID, OrderID, ProductID, UnitPrice, Quantity, Discount) " +
				"VALUES (12, 14, 15, 34.50, 10, 12)";

		BasicDBObject match = new BasicDBObject("OrderID", 14);
		DBCollection dbCollection = helpExecute(query, new String[]{"Orders"}, match);

		BasicDBObject details = new BasicDBObject();
		details.append("OrderID", new DBRef(null,"Orders", 14));
		details.append("ProductID", new DBRef(null,"Products", 15));
		details.append("_id", 12);
		details.append("UnitPrice", 34.50);
		details.append("Quantity", 10);
		details.append("Discount", 12);

		details = new BasicDBObject("OrderDetails", details);
		Mockito.verify(dbCollection, Mockito.never()).insert(details, WriteConcern.ACKNOWLEDGED);
		Mockito.verify(dbCollection).update(match, new BasicDBObject("$push", details), false, true, WriteConcern.ACKNOWLEDGED);
	}

	@Test
	public void testSimpleUpdate() throws Exception {
		String query = "UPDATE Customers SET CompanyName='JBOSS', ContactName='TEIID' WHERE CustomerID = '11'";

		BasicDBObject match = new BasicDBObject("_id", "11");
		DBCollection dbCollection = helpExecute(query, new String[]{"Customers"}, match);

		BasicDBObject details = new BasicDBObject();
		details.append("CompanyName", "JBOSS");
		details.append("ContactName", "TEIID");

		details = new BasicDBObject("$set", details);

		Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
		Mockito.verify(dbCollection).update(match, details, false, true, WriteConcern.ACKNOWLEDGED);
	}

	private DBCollection helpUpdate(String query, String[] expectedCollection, DBObject match, ArrayList<DBObject> results) throws TranslatorException {
		Command cmd = this.utility.parseCommand(query);
		ExecutionContext context = Mockito.mock(ExecutionContext.class);
		MongoDBConnection connection = Mockito.mock(MongoDBConnection.class);
		DB db = Mockito.mock(DB.class);
		DBCollection dbCollection = Mockito.mock(DBCollection.class);
		for(String collection:expectedCollection) {
			Mockito.stub(db.getCollection(collection)).toReturn(dbCollection);
		}
		Mockito.stub(db.collectionExists(Mockito.anyString())).toReturn(true);
		Mockito.stub(connection.getDatabase()).toReturn(db);

		Mockito.stub(db.getCollectionFromString(Mockito.anyString())).toReturn(dbCollection);
		Mockito.stub(dbCollection.findOne(Mockito.any())).toReturn(match);
		Mockito.stub(dbCollection.update(Mockito.any(BasicDBObject.class),
				Mockito.any(BasicDBObject.class),
				Mockito.eq(false),
				Mockito.eq(true),
				Mockito.any(WriteConcern.class))).toReturn(Mockito.mock(WriteResult.class));;

		if (results != null) {
			AggregationOutput out = Mockito.mock(AggregationOutput.class);
			Mockito.stub(out.results()).toReturn(results);

			for (DBObject obj:results) {
				match = new BasicDBObject("_id", obj.get("_id"));
				Mockito.stub(dbCollection.aggregate(new BasicDBObject("$match", match))).toReturn(out);
			}
		}

		UpdateExecution execution = this.translator.createUpdateExecution(cmd, context, this.utility.createRuntimeMetadata(), connection);
		execution.execute();
		return dbCollection;
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
				new BasicDBObject("CategoryID.$id", 11), new BasicDBObject("$set", new BasicDBObject("Categories", new BasicDBObject("_id", 11).append("key","value"))),
				false, true, WriteConcern.ACKNOWLEDGED);
	}

	@Test
	public void testUpdateEmbedIn() throws Exception {
		String query = "UPDATE OrderDetails SET UnitPrice = 12.50 WHERE ProductID = 14";

		DBCollection dbCollection = helpUpdate(query, new String[]{"Orders"}, null, null);

		Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
		Mockito.verify(dbCollection, Mockito.times(1)).update(
				new BasicDBObject("OrderDetails.ProductID.$id", 14), new BasicDBObject("$set", new BasicDBObject("OrderDetails.$.UnitPrice", 12.5)),
				false, true, WriteConcern.ACKNOWLEDGED);
	}
}
