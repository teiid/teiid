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

		Mockito.stub(dbCollection.aggregate(Mockito.any(DBObject.class), params.toArray(new DBObject[params.size()]))).toReturn(output);

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

		DBCollection dbCollection = helpExecute(query, new String[]{"Customers"}, 0);

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

		DBCollection dbCollection = helpExecute(query, new String[]{"Customers"}, 1);

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

		DBCollection dbCollection = helpExecute(query, new String[]{"Categories"}, 0);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$CategoryName");

		Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$project", result));
	}

	@Test
	public void testSelectEmbedIn() throws Exception {
		String query = "SELECT UnitPrice FROM OrderDetails";

		DBCollection dbCollection = helpExecute(query, new String[]{"Orders"}, 1);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$OrderDetails.UnitPrice");

		Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$unwind","$OrderDetails"),
						new BasicDBObject("$project", result));
	}

	@Test
	public void testSelectEmbedInWithWhere() throws Exception {
		String query = "SELECT * FROM OrderDetails WHERE OrderId = 10248";

		DBCollection dbCollection = helpExecute(query, new String[]{"Orders"}, 2);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$OrderDetails.odID");
	    result.append( "_m1","$OrderDetails._id.OrderID");
	    result.append( "_m2","$OrderDetails._id.ProductID");
	    result.append( "_m3","$OrderDetails.UnitPrice");
	    result.append( "_m4","$OrderDetails.Quantity");
	    result.append( "_m5","$OrderDetails.Discount");


		Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$unwind","$OrderDetails"),
						new BasicDBObject("$match", new BasicDBObject("OrderDetails._id.OrderID.$id", 10248)),
						new BasicDBObject("$project", result));
	}

	@Test
	public void testTwoTableInnerJoinEmbedIn() throws Exception {
		String query = "SELECT o.CustomerID, od.ProductID FROM Orders o JOIN OrderDetails od ON o.OrderID=od.OrderID";

		DBCollection dbCollection = helpExecute(query, new String[]{"Orders"}, 1);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$CustomerID");
	    result.append( "_m1","$OrderDetails._id.ProductID");

		Mockito.verify(dbCollection).aggregate(
						//new BasicDBObject("$match",new BasicDBObject("_id", "OrderDetails.OrderID")),
						new BasicDBObject("$unwind","$OrderDetails"),
						new BasicDBObject("$project", result));
	}

	@Test
	public void testTwoTableInnerJoinEmbeddable() throws Exception {
		String query = "select p.ProductName, c.CategoryName from Products p join Categories c on p.CategoryID = c.CategoryID";

		DBCollection dbCollection = helpExecute(query, new String[]{"Products"}, 0);

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

		DBCollection dbCollection = helpExecute(query, new String[]{"Products"}, 1);

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
				"FROM Orders o JOIN OrderDetails od ON o.OrderID=od.OrderID " +
				"JOIN Shippers s ON o.ShipVia = s.ShipperID";

		DBCollection dbCollection = helpExecute(query, new String[]{"Orders"}, 1);

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
				"LEFT OUTER JOIN OrderDetails ON Orders.OrderID = OrderDetails.OrderID " +
				"WHERE OrderDetails.OrderID IS NOT NULL";

		DBCollection dbCollection = helpExecute(query, new String[]{"Orders"}, 2);

	    BasicDBObject result = new BasicDBObject();
	    result.append( "_m0","$CustomerID");
	    result.append( "_m1","$OrderDetails._id.ProductID");

	    DBObject match = QueryBuilder.start("OrderDetails._id.OrderID.$id").notEquals(null).get();
	    Mockito.verify(dbCollection).aggregate(
						new BasicDBObject("$unwind","$OrderDetails"),
						new BasicDBObject("$match",match),
						new BasicDBObject("$project", result));
	}
}
