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
import org.teiid.metadata.MetadataFactory;
import org.teiid.mongodb.MongoDBConnection;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.function.UDFSource;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.TestDDLParser;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
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
	public void testSelectEmbeddableWithWhere_ON_NONPK() throws Exception {
		String query = "SELECT CategoryName FROM Categories WHERE CategoryName = 'Drinks'";

		DBCollection dbCollection = helpExecute(query, new String[]{"Categories"}, 2);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$CategoryName");

		Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$match",new BasicDBObject("CategoryName", "Drinks")),
						new BasicDBObject("$project", result));
	}	
	
	@Test
	public void testSelectEmbeddableWithWhere_ON_PK() throws Exception {
		String query = "SELECT CategoryName FROM Categories WHERE CategoryID = 10";

		DBCollection dbCollection = helpExecute(query, new String[]{"Categories"}, 2);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$CategoryName");

		Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$match",new BasicDBObject("_id", 10)),
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
	    result.append( "_m0","$_id");
	    result.append( "_m1","$OrderDetails._id.ProductID");
	    result.append( "_m2","$OrderDetails.UnitPrice");
	    result.append( "_m3","$OrderDetails.Quantity");
	    result.append( "_m4","$OrderDetails.Discount");


		Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$unwind","$OrderDetails"),
						new BasicDBObject("$match", new BasicDBObject("_id", 10248)),
						new BasicDBObject("$project", result));
	}
	
	@Test // one-2-many
	public void testSelectMergedWithWhere_ON_NON_PK() throws Exception {
		String query = "SELECT Quantity FROM OrderDetails WHERE UnitPrice = '0.99'";

		DBCollection dbCollection = helpExecute(query, new String[]{"Orders"}, 3);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$OrderDetails.Quantity");

		Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$unwind","$OrderDetails"),
						new BasicDBObject("$match", new BasicDBObject("OrderDetails.UnitPrice", 0.99)),
						new BasicDBObject("$project", result));
	}	
	
	@Test // one-2-one
	public void testSelectMergedWithWhere_ON_NON_PK_one_to_one() throws Exception {
		String query = "SELECT cust_id, zip FROM Address WHERE Street = 'Highway 100'";

		DBCollection dbCollection = helpExecute(query, new String[]{"customer"}, 2);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$_id");
	    result.append( "_m1","$address.zip");

		Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$match", new BasicDBObject("address.street", "Highway 100")),
						new BasicDBObject("$project", result));
	}	
	
	@Test // one-2-one
	public void testSelectONE_TO_ONE() throws Exception {
		String query = "SELECT c.name, a.zip " +
				"FROM customer c join address a " +
				"on c.customer_id=a.cust_id";

		DBCollection dbCollection = helpExecute(query, new String[]{"customer"}, 2);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$name");
	    result.append( "_m1","$address.zip");

		Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$match", new BasicDBObject("address", new BasicDBObject("$exists", "true").append("$ne", null))),
						new BasicDBObject("$project", result));
	}	
	
	@Test // one-2-one
	public void testSelectMergedWithNOWhere_one_to_one() throws Exception {
		String query = "SELECT cust_id, zip FROM Address";

		DBCollection dbCollection = helpExecute(query, new String[]{"customer"}, 2);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$_id");
	    result.append( "_m1","$address.zip");

		Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$match", QueryBuilder.start("address").exists("true").get()),
						new BasicDBObject("$project", result));
	}	

	@Test
	public void testTwoTableInnerJoinMergeAssosiationMany() throws Exception {
		String query = "SELECT o.CustomerID, od.ProductID FROM Orders o JOIN OrderDetails od ON o.OrderID=od.odID";

		DBCollection dbCollection = helpExecute(query, new String[]{"Orders"}, 3);
	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$CustomerID");
	    result.append( "_m1","$OrderDetails._id.ProductID");

		Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$unwind","$OrderDetails"),
						new BasicDBObject("$match", QueryBuilder.start("OrderDetails").exists("true").notEquals(null).get()),
						new BasicDBObject("$project", result));
	}

	@Test
	public void testTwoTableInnerJoinEmbeddableAssosiationOne() throws Exception {
		String query = "select p.ProductName, c.CategoryName from Products p " +
				"join Categories c on p.CategoryID = c.CategoryID";

		DBCollection dbCollection = helpExecute(query, new String[]{"Products"}, 2);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$ProductName");
	    result.append( "_m1","$Categories.CategoryName");

		Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$match", QueryBuilder.start("Categories").exists("true").notEquals(null).get()),
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

	    DBObject exists = QueryBuilder.start("Categories").exists("true").notEquals(null).get();
	    DBObject p1 = QueryBuilder.start("CategoryID").is(1).get();
	    DBObject p2 =  QueryBuilder.start("CategoryID").is(1).get();

	    DBObject match = QueryBuilder.start().and(exists, p1, p2).get(); // duplicate criteria, mongo should ignore it
		Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$match", match),
						new BasicDBObject("$project", result));
	}

	@Test
	public void testThreeTableInnerJoin() throws Exception {
		String query = "SELECT o.CustomerID, od.ProductID, s.CompanyName " +
				"FROM Orders o JOIN OrderDetails od ON o.OrderID=od.odID " +
				"JOIN Shippers s ON o.ShipVia = s.ShipperID";

		DBCollection dbCollection = helpExecute(query, new String[]{"Orders"}, 3);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$CustomerID");
	    result.append( "_m1","$OrderDetails._id.ProductID");
	    result.append( "_m2","$Shippers.CompanyName");

	    DBObject match = QueryBuilder.start().and(QueryBuilder.start("OrderDetails").exists("true").notEquals(null).get(),
	    		(QueryBuilder.start("Shippers").exists("true").notEquals(null).get())).get();

	    
	    Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$unwind","$OrderDetails"),
						new BasicDBObject("$match", match),
						new BasicDBObject("$project", result));
	}

	@Test
	public void testLeftOuterJoin() throws Exception {
		String query = "SELECT Orders.CustomerID, OrderDetails.ProductID FROM Orders " +
				"LEFT OUTER JOIN OrderDetails ON Orders.OrderID = OrderDetails.odID " +
				"WHERE OrderDetails.UnitPrice IS NOT NULL";
		
		DBCollection dbCollection = helpExecute(query, new String[]{"Orders"}, 4);

	    BasicDBObject projection = new BasicDBObject();
	    projection.append( "OrderID", 1);
	    projection.append( "CustomerID", 1);
	    projection.append("EmployeeID", 1).append("OrderDate",1).append("RequiredDate",1).append("ShippedDate",1);
	    projection.append("ShipVia",1).append("Freight",1).append("ShipName",1);
	    projection.append("ShipAddress",1).append("ShipCity",1).append("ShipRegion",1);
	    projection.append("ShipPostalCode",1).append("ShipCountry",1 );
		projection.append("__NN_OrderDetails", buildIfNullExpression("OrderDetails"));
		
	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$CustomerID");
	    result.append( "_m1","$__NN_OrderDetails._id.ProductID");

	    DBObject match = QueryBuilder.start("__NN_OrderDetails.UnitPrice").notEquals(null).get();
	    Mockito.verify(dbCollection).aggregate(
	    				new BasicDBObject("$project", projection),
						new BasicDBObject("$unwind","$__NN_OrderDetails"),
						new BasicDBObject("$match",match),
						new BasicDBObject("$project", result));
	}


    @Test
    public void testSelectNestedEmbedding()  throws Exception {
    	String query = "select T1.e1, T2.e1, T3.e1 from T1 JOIN T2 ON T1.e1=T2.e1 JOIN T3 ON T2.e1 = T3.e1";

		DBCollection dbCollection = helpExecute(query, new String[]{"T1", "T2", "T3"}, 2);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$e1");
	    result.append( "_m1","$T2._id");
	    result.append( "_m2","$T2.T3._id");

	    DBObject match = QueryBuilder.start().and(QueryBuilder.start("T2").exists("true").notEquals(null).get(),
	    		(QueryBuilder.start("T3").exists("true").notEquals(null).get())).get();
	    Mockito.verify(dbCollection).aggregate(
	    		new BasicDBObject("$match", match),
	    		new BasicDBObject("$project", result));
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
    
        
    @Test // embedded means always nested as doc not as array 
    public void testEmbeddedJoin_INNER()  throws Exception {
    	String query = "SELECT p.ProductName,s.CompanyName " +
    			"FROM Suppliers s " +
    			"JOIN " +
    			"Products p " +
    			"ON s.SupplierID = p.SupplierID";

		DBCollection dbCollection = helpExecute(query, new String[]{"Products"}, 2);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$ProductName");
	    result.append( "_m1","$Suppliers.CompanyName");

	    Mockito.verify(dbCollection).aggregate(
				new BasicDBObject("$match", QueryBuilder.start("Suppliers").exists("true").notEquals(null).get()),
				new BasicDBObject("$project", result));
    }
    
    @Test // embedded means always nested as doc not as array 
    public void testEmbeddedJoin_INNER_REVERSE()  throws Exception {
    	String query = "SELECT p.ProductName,s.CompanyName " +
    			"FROM Products p " +
    			"JOIN " +
    			"Suppliers s " +
    			"ON s.SupplierID = p.SupplierID";

		DBCollection dbCollection = helpExecute(query, new String[]{"Products"}, 2);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$ProductName");
	    result.append( "_m1","$Suppliers.CompanyName");

	    Mockito.verify(dbCollection).aggregate(
				new BasicDBObject("$match", QueryBuilder.start("Suppliers").exists("true").notEquals(null).get()),
				new BasicDBObject("$project", result));
    }    
    
    @Test(expected=TranslatorException.class) // embedded means always nested as doc not as array 
    public void testEmbeddedJoin_LEFTOUTER()  throws Exception {
    	String query = "SELECT p.ProductName,s.CompanyName " +
    			"FROM Suppliers s " +
    			"LEFT OUTER JOIN " +
    			"Products p " +
    			"ON s.SupplierID = p.SupplierID";

		DBCollection dbCollection = helpExecute(query, new String[]{"Products"}, 2);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$ProductName");
	    result.append( "_m1","$Suppliers.CompanyName");

	    Mockito.verify(dbCollection).aggregate(
				new BasicDBObject("$match", QueryBuilder.start("SupplierID").notEquals(null).and(QueryBuilder.start("Suppliers._id").notEquals(null).get()).get()),
				new BasicDBObject("$project", result));
    }    
    
    @Test // embedded means always nested as doc not as array 
    public void testEmbeddedJoin_LEFTOUTER2()  throws Exception {
    	String query = "SELECT p.ProductName,s.CompanyName " +
    			"FROM  Products p " +
    			"LEFT OUTER JOIN " +
    			"Suppliers s " +
    			"ON s.SupplierID = p.SupplierID";

		DBCollection dbCollection = helpExecute(query, new String[]{"Products"}, 1);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$ProductName");
	    result.append( "_m1","$Suppliers.CompanyName");

	    Mockito.verify(dbCollection).aggregate(
				new BasicDBObject("$project", result));
    }    
    
    @Test // embedded means always nested as doc not as array 
    public void testEmbeddedJoin_RIGHTOUTER()  throws Exception {
    	String query = "SELECT p.ProductName,s.CompanyName " +
    			"FROM Suppliers s " +
    			"RIGHT OUTER JOIN " +
    			"Products p " +
    			"ON s.SupplierID = p.SupplierID";

		DBCollection dbCollection = helpExecute(query, new String[]{"Products"}, 1);
	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$ProductName");
	    result.append( "_m1","$Suppliers.CompanyName");

	    Mockito.verify(dbCollection).aggregate(
				new BasicDBObject("$project", result));
    }    
    
    // embedded means always nested as doc not as array
    @Test(expected=TranslatorException.class) 
    public void testEmbeddedJoin_RIGHTOUTER2()  throws Exception {
    	String query = "SELECT p.ProductName,s.CompanyName " +
    			"FROM  Products p " +
    			"RIGHT OUTER JOIN " +
    			"Suppliers s " +
    			"ON s.SupplierID = p.SupplierID";

		DBCollection dbCollection = helpExecute(query, new String[]{"Products"}, 2);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$ProductName");
	    result.append( "_m1","$Suppliers.CompanyName");

	    Mockito.verify(dbCollection).aggregate(
				new BasicDBObject("$match", QueryBuilder.start("_id").notEquals(null).get()),
				new BasicDBObject("$project", result));
    }
    
    @Test // merge where one to many relation 
    public void testMERGE_ONE_TO_MANY_Join_INNER()  throws Exception {
    	String query = "SELECT c.name,n.Comment,n.CustomerId " +
    			"FROM customer c " +
    			"JOIN " +
    			"Notes n " +
    			"ON c.customer_id = n.CustomerId";

		DBCollection dbCollection = helpExecute(query, new String[]{"customer"}, 3);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$name");
	    result.append( "_m1","$Notes.Comment");
	    result.append( "_m2","$_id");

	    Mockito.verify(dbCollection).aggregate(
	    		new BasicDBObject("$unwind", "$Notes"),
				new BasicDBObject("$match", QueryBuilder.start("Notes").exists("true").notEquals(null).get()),
				new BasicDBObject("$project", result));
    }
    
    @Test // merge where one to many relation 
    public void testMERGE_ONE_TO_MANY_Join_LEFT_OUTER()  throws Exception {
    	String query = "SELECT c.name,n.Comment " +
    			"FROM customer c " +
    			"LEFT JOIN " +
    			"Notes n " +
    			"ON c.customer_id = n.CustomerId";

		DBCollection dbCollection = helpExecute(query, new String[]{"customer"}, 3);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$name");
	    result.append( "_m1","$__NN_Notes.Comment");
	    
		BasicDBObject ifnull = buildIfNullExpression("Notes");
	    
	 	BasicDBObject project = new BasicDBObject();
	 	project.append("customer_id", 1);
	 	project.append("name", 1);
	 	project.append("__NN_Notes", ifnull);
	    Mockito.verify(dbCollection).aggregate(
	    		new BasicDBObject("$project", project),
	    		new BasicDBObject("$unwind", "$__NN_Notes"),
				new BasicDBObject("$project", result));
    }
    
    @Test // merge where one to many relation - equal to inner join with doc format teiid has
    public void testMERGE_ONE_TO_MANY_Join_LEFT_OUTER4()  throws Exception {
    	String query = "SELECT c.name,n.Comment " +
    			"FROM customer c " +
    			"RIGHT JOIN " +
    			"Notes n " +
    			"ON c.customer_id = n.CustomerId";

		DBCollection dbCollection = helpExecute(query, new String[]{"customer"}, 3);
		
	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$name");
	    result.append( "_m1","$Notes.Comment");
		
	    Mockito.verify(dbCollection).aggregate(
	    		new BasicDBObject("$unwind", "$Notes"),
	    		new BasicDBObject("$match", QueryBuilder.start("Notes").exists("true").notEquals(null).get()),
				new BasicDBObject("$project", result));
    }    
    
    
    @Test // merge where one to many relation - equal to inner join with doc format teiid has
    public void testMERGE_ONE_TO_MANY_Join_LEFT_OUTER3()  throws Exception {
    	String query = "SELECT c.name,n.Comment " +
    			"FROM Notes n " +
    			"LEFT JOIN " +
    			"Customer c " +
    			"ON c.customer_id = n.CustomerId";

		DBCollection dbCollection = helpExecute(query, new String[]{"customer"}, 3);
		
	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$name");
	    result.append( "_m1","$Notes.Comment");
		
	    Mockito.verify(dbCollection).aggregate(
	    		new BasicDBObject("$unwind", "$Notes"),
	    		new BasicDBObject("$match", QueryBuilder.start("Notes").exists("true").notEquals(null).get()),
				new BasicDBObject("$project", result));
    }    
    
    @Test // merge where one to many relation (2 merged tables into customer)
    public void testMERGE_ONE_TO_MANY_Join_INNER_OUTER2()  throws Exception {
    	String query = "SELECT c.name,n.Comment ,r.amount " +
    			"FROM customer c " +
    			"LEFT JOIN " +
    			"Notes n " +
    			"ON c.customer_id = n.CustomerId " +
    			"LEFT JOIN rental r ON r.customer_id = c.customer_id";

		DBCollection dbCollection = helpExecute(query, new String[]{"customer"}, 4);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$name");
	    result.append( "_m1","$__NN_Notes.Comment");
	    result.append( "_m2","$__NN_rental.amount");
	    
	 	BasicDBObject project = new BasicDBObject();
	 	project.append("customer_id", 1);
	 	project.append("name", 1);
	 	project.append("__NN_Notes", buildIfNullExpression("Notes"));
	 	project.append("__NN_rental", buildIfNullExpression("rental"));
	    Mockito.verify(dbCollection).aggregate(
	    		new BasicDBObject("$project", project),
	    		new BasicDBObject("$unwind", "$__NN_rental"),
	    		new BasicDBObject("$unwind", "$__NN_Notes"),
				new BasicDBObject("$project", result));
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

		DBCollection dbCollection = helpExecute(query, new String[]{"Customers"}, 2);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$_id._c0");

		Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$group", new BasicDBObject("_id", new BasicDBObject("_c0", "$Country"))),
						new BasicDBObject("$project", result));
	}
	
	@Test
	public void testMultipleGroupBy() throws Exception {
		String query = "SELECT Country,City FROM Customers GROUP BY Country,City";

		DBCollection dbCollection = helpExecute(query, new String[]{"Customers"}, 2);

	    BasicDBObject project = new BasicDBObject();
	    project.append( "_m0","$_id._c0");
	    project.append( "_m1","$_id._c1");

	    BasicDBObject group = new BasicDBObject();
	    group.append( "_c0","$Country");
	    group.append( "_c1","$City");
	    	    
		Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$group", new BasicDBObject("_id", group)),
						new BasicDBObject("$project", project));
	}	
	
	@Test
	public void testDistinctSingle() throws Exception {
		String query = "SELECT DISTINCT Country FROM Customers";

		DBCollection dbCollection = helpExecute(query, new String[]{"Customers"}, 2);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$_id._m0");

		Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$group", new BasicDBObject("_id", new BasicDBObject("_m0", "$Country"))),
						new BasicDBObject("$project", result));
	}	
	
	@Test
	public void testDistinctMulti() throws Exception {
		String query = "SELECT DISTINCT Country, City FROM Customers";

		DBCollection dbCollection = helpExecute(query, new String[]{"Customers"}, 2);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$_id._m0");
	    result.append( "_m1","$_id._m1");
	    
	    BasicDBObject group = new BasicDBObject();
	    group.append( "_m0","$Country");
	    group.append( "_m1","$City");
	    
		Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$group", new BasicDBObject("_id", group)),
						new BasicDBObject("$project", result));
	}	
	
    @Test // embedded means always nested as doc not as array 
    public void testONE_TO_ONE_WithGroupBy()  throws Exception {
		String query = "SELECT c.name, a.zip " +
				"FROM customer c join address a " +
				"on c.customer_id=a.cust_id " +
				"GROUP BY c.name, a.zip";

		DBCollection dbCollection = helpExecute(query, new String[]{"customer"}, 3);

	    BasicDBObject project = new BasicDBObject();
	    project.append( "_m0","$_id._c0");
	    project.append( "_m1","$_id._c1");
	    
	    BasicDBObject group = new BasicDBObject();
	    group.append( "_c0","$name");
	    group.append( "_c1","$address.zip");	    

	    Mockito.verify(dbCollection).aggregate(
	    		new BasicDBObject("$match", new BasicDBObject("address", new BasicDBObject("$exists", "true").append("$ne", null))),
	    		new BasicDBObject("$group", new BasicDBObject("_id", group)),
				new BasicDBObject("$project", project));
    }	
    
    @Test // embedded means always nested as doc not as array 
    public void testONE_TO_ONE_WithGroupByOrderBy()  throws Exception {
		String query = "SELECT c.name, a.zip " +
				"FROM customer c join address a " +
				"on c.customer_id=a.cust_id " +
				"GROUP BY c.name, a.zip " +
				"ORDER BY c.name, a.zip " +
				"limit 2";

		DBCollection dbCollection = helpExecute(query, new String[]{"customer"}, 6);

	    BasicDBObject project = new BasicDBObject();
	    project.append( "_m0","$_id._c0");
	    project.append( "_m1","$_id._c1");
	    
	    BasicDBObject group = new BasicDBObject();
	    group.append( "_c0","$name");
	    group.append( "_c1","$address.zip");	    

	    BasicDBObject sort = new BasicDBObject();
	    sort.append( "_m0",1);
	    sort.append( "_m1",1);
	    
	    Mockito.verify(dbCollection).aggregate(
	    		new BasicDBObject("$match", new BasicDBObject("address", new BasicDBObject("$exists", "true").append("$ne", null))),
	    		new BasicDBObject("$group", new BasicDBObject("_id", group)),
				new BasicDBObject("$project", project),
				new BasicDBObject("$sort", sort),
				new BasicDBObject("$skip", 0),
				new BasicDBObject("$limit", 2));
    }    
    
    @Test
    public void testSumWithGroupBy() throws Exception {
    	String query = "SELECT SUM(age) as total FROM users GROUP BY user_id";
    	
    	DBCollection dbCollection = helpExecute(query, new String[]{"users"}, 2);
    	BasicDBObject id = new BasicDBObject();
	    id.append( "_c0","$user_id");
	    
	    BasicDBObject group = new BasicDBObject("_id", id);
	    group.append("total", new BasicDBObject("$sum", "$age"));
	    
		BasicDBObject project = new BasicDBObject();
	    project.append( "total",1);

	    Mockito.verify(dbCollection).aggregate(	    		
	    		new BasicDBObject("$group", group),
				new BasicDBObject("$project", project));
    }
    
    
    @Test
    public void testSumWithGroupBy2() throws Exception {
    	String query = "SELECT user_id, status, SUM(age) as total FROM users GROUP BY user_id, status";
    	
		DBCollection dbCollection = helpExecute(query, new String[]{"users"}, 2);

		BasicDBObject project = new BasicDBObject();
	    project.append( "_m0","$_id._c0");
	    project.append( "_m1","$_id._c1");
	    project.append( "total",1);

	    BasicDBObject id = new BasicDBObject();
	    id.append( "_c0","$user_id");
	    id.append( "_c1","$status");	    

	    BasicDBObject group = new BasicDBObject("_id", id);
	    group.append("total", new BasicDBObject("$sum", "$age"));
		
	    Mockito.verify(dbCollection).aggregate(	    		
	    		new BasicDBObject("$group", group),
				new BasicDBObject("$project", project));
    }
 
    @Test
    public void testSumWithGroupBy3() throws Exception {
    	String query = "SELECT user_id, SUM(age) as total FROM users GROUP BY user_id";
    	
		DBCollection dbCollection = helpExecute(query, new String[]{"users"}, 2);

		BasicDBObject project = new BasicDBObject();
	    project.append( "_m0","$_id._c0");
	    project.append( "total",1);

	    BasicDBObject id = new BasicDBObject();
	    id.append( "_c0","$user_id");

	    BasicDBObject group = new BasicDBObject("_id", id);
	    group.append("total", new BasicDBObject("$sum", "$age"));
		
	    Mockito.verify(dbCollection).aggregate(	    		
	    		new BasicDBObject("$group", group),
				new BasicDBObject("$project", project));    	
    }     
    
    @Test
    public void testAggregateWithHaving() throws Exception {
    	String query = "SELECT SUM(age) as total FROM users GROUP BY user_id HAVING SUM(age) > 250";

    	DBCollection dbCollection = helpExecute(query, new String[]{"users"}, 3);

		BasicDBObject project = new BasicDBObject();
	    project.append( "total",1);

	    BasicDBObject id = new BasicDBObject();
	    id.append( "_c0","$user_id");

	    BasicDBObject group = new BasicDBObject("_id", id);
	    group.append("total", new BasicDBObject("$sum", "$age"));
		
	    Mockito.verify(dbCollection).aggregate(	   
	    		new BasicDBObject("$group", group),
	    		new BasicDBObject("$match", QueryBuilder.start("total").greaterThan(250).get()),
				new BasicDBObject("$project", project));    	
    	
    }    
    
    @Test
    public void testAggregateWithHavingAndWhere() throws Exception {
    	String query = "SELECT SUM(age) as total FROM users WHERE age > 45 GROUP BY user_id HAVING SUM(age) > 250";
    	
		DBCollection dbCollection = helpExecute(query, new String[]{"users"}, 4);

		BasicDBObject project = new BasicDBObject();
	    project.append( "total",1);

	    BasicDBObject id = new BasicDBObject();
	    id.append( "_c0","$user_id");

	    BasicDBObject group = new BasicDBObject("_id", id);
	    group.append("total", new BasicDBObject("$sum", "$age"));
		
	    Mockito.verify(dbCollection).aggregate(	   
	    		new BasicDBObject("$match", QueryBuilder.start("age").greaterThan(45).get()),
	    		new BasicDBObject("$group", group),
	    		new BasicDBObject("$match", QueryBuilder.start("total").greaterThan(250).get()),
				new BasicDBObject("$project", project));    	
    }    
    
    @Test
    public void testCountStar() throws Exception {
        String query = "SELECT count(*) FROM Categories";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"}, 2);

        BasicDBObject group = new BasicDBObject();
        group.append( "_id", null);
        group.append( "_m0", new BasicDBObject("$sum", 1));

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0", 1);

        Mockito.verify(dbCollection).aggregate(
                        new BasicDBObject("$group", group),
                        new BasicDBObject("$project", result));
    }  
    
    @Test
    public void testCountOnColumn() throws Exception {
        String query = "SELECT count(CategoryName) FROM Categories";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"}, 3);
        
        BasicDBObject group = new BasicDBObject();
        group.append( "_id", null);
        group.append( "_m0", new BasicDBObject("$sum", 1));

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0", 1);

        Mockito.verify(dbCollection).aggregate(
                        new BasicDBObject("$match", QueryBuilder.start("CategoryName").notEquals(null).get()),
                        new BasicDBObject("$group", group),
                        new BasicDBObject("$project", result));
    }     
    
    @Test(expected=TranslatorException.class)
    public void testCountOnmultipleColumns() throws Exception {
        String query = "SELECT count(CategoryName), count(CategoryID) FROM Categories";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"}, 3);
        
        BasicDBObject group = new BasicDBObject();
        group.append( "_id", null);
        group.append( "_m0", new BasicDBObject("$sum", 1));

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0", 1);

        Mockito.verify(dbCollection).aggregate(
                        new BasicDBObject("$match", QueryBuilder.start("CategoryName").notEquals(null).get()),
                        new BasicDBObject("$group", group),
                        new BasicDBObject("$project", result));
    }  
    
    @Test
    public void testFunctionInWhere() throws Exception {
        String query = "SELECT CategoryName FROM Categories WHERE CONCAT(CategoryName, '2') = '2'";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"}, 2);
        
        // { "$project" : { "_m0" : { "$concat" : [ "$CategoryName" , "2"]} , "_m1" : "$CategoryName"}},
        BasicDBList params = new BasicDBList();
        params.add("$CategoryName");
        params.add("2");
        
        BasicDBObject result = new BasicDBObject();
        result.append( "_m0", new BasicDBObject("$concat", params));
        result.append( "_m1", "$CategoryName");

        Mockito.verify(dbCollection).aggregate(
                        new BasicDBObject("$project", result),
                        new BasicDBObject("$match", QueryBuilder.start("_m0").is("2").get()));
    }   
    
    @Test
    public void testSubStr() throws Exception {
        String query = "SELECT SUBSTRING(CategoryName, 3) FROM Categories";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"}, 1);
        
        //{ "$subtract" : [ 3 , 1]}
        BasicDBList subtract = new BasicDBList();
        subtract.add(3);
        subtract.add(1);
        
        BasicDBList params = new BasicDBList();
        params.add("$CategoryName");
        params.add(new BasicDBObject("$subtract", subtract));
        params.add(4000);
        
        //{ "$project" : { "_m0" : { "$substr" : [ "$CategoryName" , 1 , 4000]}}}
        BasicDBObject result = new BasicDBObject();
        result.append( "_m0", new BasicDBObject("$substr", params));

        Mockito.verify(dbCollection).aggregate(new BasicDBObject("$project", result));
    }
    
    @Test
    public void testSubStr2() throws Exception {
        String query = "SELECT SUBSTRING(CategoryName, CategoryID, 4) FROM Categories";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"}, 1);

        BasicDBList subtract = new BasicDBList();
        subtract.add("$_id");
        subtract.add(1);
        
        BasicDBList params = new BasicDBList();
        params.add("$CategoryName");
        params.add(new BasicDBObject("$subtract", subtract));
        params.add(4);
        
        //{ "$project" : { "_m0" : { "$substr" : [ "$CategoryName" , 1 , 4000]}}}
        BasicDBObject result = new BasicDBObject();
        result.append( "_m0", new BasicDBObject("$substr", params));

        Mockito.verify(dbCollection).aggregate(new BasicDBObject("$project", result));
    }     
    
    @Test
    public void testSelectConstant() throws Exception {
        String query = "SELECT 'hit' FROM Categories";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"}, 1);

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0", new BasicDBObject("$literal", "hit"));

        Mockito.verify(dbCollection).aggregate(new BasicDBObject("$project", result));
    }
    
    @Test
    public void testOffsetWithoutLimit() throws Exception {
        String query = "SELECT CategoryName FROM Categories OFFSET 45 ROWS";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"}, 2);

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0", "$CategoryName");

        Mockito.verify(dbCollection).aggregate(
                new BasicDBObject("$project", result),
                new BasicDBObject("$skip", 45));
    }     
    
    
    @Test
    public void testArrtyType() throws Exception {
        String query = "SELECT * FROM ArrayTest";

        DBCollection dbCollection = helpExecute(query, new String[]{"ArrayTest"}, 1);

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0", "$id");
        result.append("_m1", "$column1");

        Mockito.verify(dbCollection).aggregate(
                new BasicDBObject("$project", result));
    }    
    
    @Test
    public void testArrtyTypeInWhere() throws Exception {
        String query = "SELECT * FROM ArrayTest where column1 is not null";

        DBCollection dbCollection = helpExecute(query, new String[]{"ArrayTest"}, 2);

        BasicDBObject result = new BasicDBObject();
        result.append( "_m0", "$id");
        result.append("_m1", "$column1");

        Mockito.verify(dbCollection).aggregate(
        		new BasicDBObject("$match", QueryBuilder.start("column1").notEquals(null).get()),
                new BasicDBObject("$project", result));
    }     
    
    @Test
    public void testGeoFunctionInWhere() throws Exception {
        String query = "SELECT CategoryName FROM Categories WHERE mongo.geoWithin(CategoryName, 'Polygon', ((cast(1.0 as double), cast(2.0 as double)),(cast(3.0 as double), cast(4.0 as double)))) or CategoryID=1";

        DBCollection dbCollection = helpExecute(query, new String[]{"Categories"}, 2);
        
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

        Mockito.verify(dbCollection).aggregate(
                        new BasicDBObject("$match", qb.get()),
                        new BasicDBObject("$project", result));
    }
    
    @Test(expected=TranslatorException.class)
    public void testGeoFunctionInWhereWithFalse() throws Exception {
        String query = "SELECT CategoryName FROM Categories WHERE mongo.geoWithin(CategoryName, 'Polygon', ((cast(1.0 as double), cast(2.0 as double)),(cast(3.0 as double), cast(4.0 as double)))) = false";
        helpExecute(query, new String[]{"Categories"}, 2);
    }
    
    @Test
    public void testAdd() throws Exception {
        String query = "SELECT SupplierID+1 FROM Suppliers";

        DBCollection dbCollection = helpExecute(query, new String[]{"Suppliers"}, 1);
        //{ "$project" : { "_m0" : { "$add" : [ "$_id" , 1]}}}
        BasicDBObject result = new BasicDBObject();
        result.append( "_m0",new BasicDBObject("$add", buildObjectArray("$_id", 1)));

        Mockito.verify(dbCollection).aggregate(new BasicDBObject("$project", result));
    }
    
    ArrayList<Object> buildObjectArray(Object ...objs){
        ArrayList<Object> list = new ArrayList<Object>();
        for (Object obj:objs) {
            list.add(obj);
        }
        return list;
    }
}
