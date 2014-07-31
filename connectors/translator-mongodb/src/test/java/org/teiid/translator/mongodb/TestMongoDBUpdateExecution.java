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
import org.junit.Ignore;
import org.junit.Test;
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
		details.append("Discount", 12);
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
	public void testMergeUpdateOne2One() throws Exception {
		// tests one-to-many situation
		String query = "UPDATE address SET street = 'Highway 100' WHERE cust_id = 100";

		ArrayList<DBObject> results = new ArrayList<DBObject>();
		BasicDBObject address = new BasicDBObject();
		address.append("street", "123 Street").append("zip", "90210");
		results.add(new BasicDBObject("_id", 100).append("address", address));
		
		BasicDBObject match = new BasicDBObject("_id", 100);
		DBCollection dbCollection = helpUpdate(query, new String[]{"customer"}, match, results);

		BasicDBObject details = new BasicDBObject();
		details.append("street", "Highway 100");
		details.append("zip", "90210");

		details = new BasicDBObject("address", details);
		Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
		Mockito.verify(dbCollection).update(match, new BasicDBObject("$set", details), false, true, WriteConcern.ACKNOWLEDGED);
	}	
	
    @Test // one-2-many mapping update merge case; 
    public void testMergeUpdateOne2Many() throws Exception {
        // tests one-to-many situation
        String query = "UPDATE Notes SET Comment = 'Hiya' WHERE CustomerId = 100";

        ArrayList<DBObject> results = new ArrayList<DBObject>();
        BasicDBList row = new BasicDBList();
        row.add(new BasicDBObject("Comment", "Hello").append("PostDate", "{ts '2014-07-15 09:06:04'}"));
        row.add(new BasicDBObject("Comment", "Hola").append("PostDate", "{ts '2013-07-15 09:06:04'}"));
        results.add(new BasicDBObject("_id", 100).append("Notes", row));
        
        BasicDBObject match = new BasicDBObject("_id", 100);
        DBCollection dbCollection = helpUpdate(query, new String[]{"customer"}, match, results);

        BasicDBList expected = new BasicDBList();
        expected.add(new BasicDBObject("Comment", "Hiya").append("PostDate", "{ts '2014-07-15 09:06:04'}"));
        expected.add(new BasicDBObject("Comment", "Hiya").append("PostDate", "{ts '2013-07-15 09:06:04'}"));

        BasicDBObject update = new BasicDBObject("Notes", expected);
        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, new BasicDBObject("$set", update), false, true, WriteConcern.ACKNOWLEDGED);
    }	
	
	@Test // one-2-one mapping update merge case; 
	public void testMergeDeleteOne2OneonPK() throws Exception {
		// tests one-to-many situation
		String query = "DELETE FROM address WHERE cust_id = 100";

		ArrayList<DBObject> results = new ArrayList<DBObject>();
		BasicDBObject address = new BasicDBObject();
		address.append("street", "123 Street").append("zip", "90210").append("_id", new DBRef(null, "customer", 100));
		results.add(new BasicDBObject("_id", 100).append("address", address));
		
		BasicDBObject match = new BasicDBObject("_id", 100);
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
	
    @Test // one-2-one mapping update merge case; 
    public void testMergeDeleteOne2ManyOnFK() throws Exception {
        // tests one-to-many situation
        String query = "DELETE FROM Notes WHERE CustomerId = 100";

        ArrayList<DBObject> results = new ArrayList<DBObject>();
        BasicDBList row = new BasicDBList();
        row.add(new BasicDBObject("Comment", "Hello").append("PostDate", "{ts '2014-07-15 09:06:04'}"));
        row.add(new BasicDBObject("Comment", "Hola").append("PostDate", "{ts '2013-07-15 09:06:04'}"));
        results.add(new BasicDBObject("_id", 100).append("Notes", row));
        
        BasicDBObject match = new BasicDBObject("_id", 100);
        DBCollection dbCollection = helpUpdate(query, new String[]{"customer"}, match, results);

        BasicDBObject details = new BasicDBObject("Notes", new BasicDBObject());
        Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
        Mockito.verify(dbCollection).update(match, new BasicDBObject("$pull", details), false, true, WriteConcern.ACKNOWLEDGED);
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
    
	@Test
	public void testSimpleUpdate() throws Exception {
		String query = "UPDATE Customers SET CompanyName='JBOSS', ContactName='TEIID' WHERE CustomerID = '11'";

		BasicDBObject match = new BasicDBObject("_id", "11");
		DBCollection dbCollection = helpUpdate(query, new String[]{"Customers"}, match, null);

		BasicDBObject details = new BasicDBObject();
		details.append("CompanyName", "JBOSS");
		details.append("ContactName", "TEIID");

		details = new BasicDBObject("$set", details);

		Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
		Mockito.verify(dbCollection).update(match, details, false, true, WriteConcern.ACKNOWLEDGED);
	}

	private DBCollection helpUpdate(String query, String[] expectedCollection, DBObject match_result, ArrayList<DBObject> results) throws TranslatorException {
		Command cmd = this.utility.parseCommand(query);
		ExecutionContext context = Mockito.mock(ExecutionContext.class);
		Mockito.stub(context.getCommandContext()).toReturn(Mockito.mock(CommandContext.class));
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
		Mockito.stub(dbCollection.update(Mockito.any(BasicDBObject.class),
				Mockito.any(BasicDBObject.class),
				Mockito.eq(false),
				Mockito.eq(true),
				Mockito.any(WriteConcern.class))).toReturn(Mockito.mock(WriteResult.class));;

		if (results != null) {
			AggregationOutput out = Mockito.mock(AggregationOutput.class);
			Mockito.stub(out.results()).toReturn(results);

			for (DBObject obj:results) {
				Mockito.stub(dbCollection.aggregate(Mockito.any(BasicDBObject.class))).toReturn(out);
				Mockito.stub(dbCollection.aggregate(Mockito.any(BasicDBObject.class))).toReturn(out);
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
				new BasicDBObject("CategoryID", 11), new BasicDBObject("$set", new BasicDBObject("Categories", new BasicDBObject("key","value"))),
				false, true, WriteConcern.ACKNOWLEDGED);
	}

	@Test // one to many
	public void testDeleteMergeOneToManyOnPK() throws Exception {
		String query = "DELETE FROM OrderDetails WHERE ProductID = 14 and odID = 1";

		DBCollection dbCollection = helpUpdate(query, new String[]{"Orders"}, new BasicDBObject("oid", 1), null);

		Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
		Mockito.verify(dbCollection, Mockito.times(1)).update(
		        new BasicDBObject("_id", 1), new BasicDBObject("$pull", new BasicDBObject("OrderDetails", new BasicDBObject("_id.ProductID", 14))),
				false, true, WriteConcern.ACKNOWLEDGED);
	}
	
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
	public void testDeleteMergeOnetoManyAllRows() throws Exception {
		String query = "DELETE FROM OrderDetails";

		DBCollection dbCollection = helpUpdate(query, new String[]{"Orders"}, new BasicDBObject("oid", 1), null);

		DBObject match = QueryBuilder.start("OrderDetails").exists(true).get();

		BasicDBObject details = new BasicDBObject("$pull", new BasicDBObject("OrderDetails", new BasicDBObject()));

		Mockito.verify(dbCollection, Mockito.never()).insert(new BasicDBObject(), WriteConcern.ACKNOWLEDGED);
		Mockito.verify(dbCollection, Mockito.times(1)).update(
				match, details,
				false, true, WriteConcern.ACKNOWLEDGED);
	}	

	@Test // one to many
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
	public void testNestedMergeInsert2() throws Exception {
	    BasicDBObject customer_result = new BasicDBObject();
	    customer_result.append("rental._id", 2);

	    BasicDBObject match = new BasicDBObject();
		match.append("_id", 1);

		BasicDBObject rental = new BasicDBObject();
		rental.append("rental_id", 2);
		rental.append("amount", 3.99);
		rental.append("_id", 3);
		BasicDBObject rentalresult = new BasicDBObject("rental.$.payment", rental);

		String query = "INSERT INTO payment (payment_id, rental_id, amount) VALUES (3, 2, 3.99)";
	    DBCollection dbCollection = helpUpdate(query, new String[]{"customer"}, match, null);
		Mockito.verify(dbCollection, Mockito.never()).insert(customer_result, WriteConcern.ACKNOWLEDGED);
		Mockito.verify(dbCollection).update(customer_result, new BasicDBObject("$push", rentalresult), false, true, WriteConcern.ACKNOWLEDGED);
	}

	@Test(expected=TranslatorException.class)
	public void testNestedMergeUpdate() throws Exception {
	    BasicDBObject customer_result = new BasicDBObject();
	    customer_result.append("rental._id", 2);
	    customer_result.append("_id", 1);

	    BasicDBObject match = new BasicDBObject();
		match.append("_id", 1);

		BasicDBObject rental = new BasicDBObject();
		rental.append("rental_id", new DBRef(null, "rental", 2));
		rental.append("amount", "3.99");
		rental.append("_id", 3);
		BasicDBObject rentalresult = new BasicDBObject("rental.$.payment", rental);

		String query = "UPDATE payment SET amount = 3.99 WHERE payment_id = 1 and rental_id = 1";
	    DBCollection dbCollection = helpUpdate(query, new String[]{"customer"}, match, null);
		Mockito.verify(dbCollection, Mockito.never()).insert(customer_result, WriteConcern.ACKNOWLEDGED);
		Mockito.verify(dbCollection).update(customer_result, new BasicDBObject("$push", rentalresult), false, true, WriteConcern.ACKNOWLEDGED);
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
}
