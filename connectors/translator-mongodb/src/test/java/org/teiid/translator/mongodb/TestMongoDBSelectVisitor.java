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

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Select;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;

import com.mongodb.BasicDBObject;

@SuppressWarnings("nls")
public class TestMongoDBSelectVisitor {
    private MongoDBExecutionFactory translator;
    private TranslationUtility utility;

    @Before
    public void setUp() throws Exception {
    	this.translator = new MongoDBExecutionFactory();
    	this.translator.start();

    	TransformationMetadata metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("northwind.ddl")), "sakila", "northwind");
    	this.utility = new TranslationUtility(metadata);

    }

    private void helpExecute(String query, String collection, String project, String match) throws Exception {
    	helpExecute(query, collection, project, match, null, null);
    }
    private void helpExecute(String query, String collection, String project, String match, String groupby, String having) throws Exception {
    	Select cmd = (Select)this.utility.parseCommand(query);
    	MongoDBSelectVisitor visitor = new MongoDBSelectVisitor(this.translator, this.utility.createRuntimeMetadata());
    	visitor.visitNode(cmd);
    	if (!visitor.exceptions.isEmpty()) {
    		throw visitor.exceptions.get(0);
    	}

    	assertEquals(collection, visitor.collectionTable.getName());
    	if (project != null) {
    		assertEquals("project wrong", project, visitor.project.toString());
    	}

    	if (match != null) {
    		assertEquals("match wrong", match, visitor.match.toString());
    	}

    	if (groupby != null) {
    		assertEquals("groupby wrong", groupby, visitor.group.toString());
    	}

    	if (having != null) {
    		assertEquals("having wrong", having, visitor.having.toString());
    	}
    }

    @Test
    public void testSelectStar() throws Exception {
		helpExecute(
				"select * from customers",
				"Customers",
				"{ \"_m0\" : \"$_id\" , \"_m1\" : \"$CompanyName\" , \"_m2\" : \"$ContactName\" , \"_m3\" : \"$ContactTitle\" , \"_m4\" : \"$Address\" , \"_m5\" : \"$City\" , \"_m6\" : \"$Region\" , \"_m7\" : \"$PostalCode\" , \"_m8\" : \"$Country\" , \"_m9\" : \"$Phone\" , \"_m10\" : \"$Fax\"}",
				null);
    }

    @Test
    public void testSelectColum() throws Exception {
		helpExecute("select CompanyName, ContactName from customers",
				"Customers",
				"{ \"_m0\" : \"$CompanyName\" , \"_m1\" : \"$ContactName\"}",
				null);
    }

    @Test
    public void testWhereEQ() throws Exception {
		helpExecute(
				"SELECT CompanyName, ContactName FROM customers WHERE CompanyName = 'A'",
				"Customers",
				"{ \"_m0\" : \"$CompanyName\" , \"_m1\" : \"$ContactName\"}",
				"{ \"CompanyName\" : \"A\"}");
    }

    @Test
    public void testAND() throws Exception {
		helpExecute(
				"SELECT CompanyName, ContactName FROM customers WHERE CompanyName = 'A' AND ContactName = 'B'",
				"Customers",
				"{ \"_m0\" : \"$CompanyName\" , \"_m1\" : \"$ContactName\"}",
				"{ \"$and\" : [ { \"CompanyName\" : \"A\"} , { \"ContactName\" : \"B\"}]}");
    }

    @Test
    public void testOR() throws Exception {
		helpExecute(
				"SELECT CompanyName, ContactName FROM customers WHERE CompanyName = 'A' OR ContactName = 'B'",
				"Customers",
				"{ \"_m0\" : \"$CompanyName\" , \"_m1\" : \"$ContactName\"}",
				"{ \"$or\" : [ { \"CompanyName\" : \"A\"} , { \"ContactName\" : \"B\"}]}");
    }


    @Test
    public void testComplexAndOr() throws Exception {
		helpExecute(
				"SELECT CompanyName, ContactName FROM customers WHERE (CompanyName = 'A' AND ContactName = 'B') OR (CompanyName = 'B' AND ContactName = 'A')",
				"Customers",
				"{ \"_m0\" : \"$CompanyName\" , \"_m1\" : \"$ContactName\"}",
				"{ \"$or\" : [ { \"$and\" : [ { \"CompanyName\" : \"A\"} , { \"ContactName\" : \"B\"}]} , { \"$and\" : [ { \"CompanyName\" : \"B\"} , { \"ContactName\" : \"A\"}]}]}");
    }

    @Test
    public void testComplexOrAnd() throws Exception {
		helpExecute(
				"SELECT CompanyName, ContactName FROM customers WHERE (CompanyName = 'A' OR ContactName = 'B') AND (CompanyName = 'B' OR ContactName = 'A')",
				"Customers",
				"{ \"_m0\" : \"$CompanyName\" , \"_m1\" : \"$ContactName\"}",
				"{ \"$and\" : [ { \"$or\" : [ { \"CompanyName\" : \"A\"} , { \"ContactName\" : \"B\"}]} , { \"$or\" : [ { \"CompanyName\" : \"B\"} , { \"ContactName\" : \"A\"}]}]}");
    }

    @Test
    public void testOrRewriteToIn() throws Exception {
		helpExecute(
				"SELECT CompanyName, ContactName FROM customers WHERE CompanyName = 'A' OR CompanyName = 'B'",
				"Customers",
				"{ \"_m0\" : \"$CompanyName\" , \"_m1\" : \"$ContactName\"}",
				"{ \"CompanyName\" : { \"$in\" : [ \"B\" , \"A\"]}}");
    }

    @Test
    public void testIn() throws Exception {
		helpExecute(
				"SELECT CompanyName, ContactName FROM customers WHERE CompanyName IN('A', 'B')",
				"Customers",
				"{ \"_m0\" : \"$CompanyName\" , \"_m1\" : \"$ContactName\"}",
				"{ \"CompanyName\" : { \"$in\" : [ \"A\" , \"B\"]}}");
    }

    @Test
    public void testNotIn() throws Exception {
		helpExecute(
				"SELECT CompanyName, ContactName FROM customers WHERE CompanyName NOT IN ('A', 'B')",
				"Customers",
				"{ \"_m0\" : \"$CompanyName\" , \"_m1\" : \"$ContactName\"}",
				"{ \"CompanyName\" : { \"$nin\" : [ \"A\" , \"B\"]}}");
    }

    @Test
    public void testIsNull() throws Exception {
		helpExecute(
				"SELECT CompanyName, ContactName FROM Customers WHERE ContactName IS NULL",
				"Customers",
				"{ \"_m0\" : \"$CompanyName\" , \"_m1\" : \"$ContactName\"}",
				"{ \"ContactName\" :  null }");
    }

    @Test
    public void testIsNotNull() throws Exception {
		helpExecute(
				"SELECT CompanyName, ContactName FROM Customers WHERE ContactName IS NOT NULL",
				"Customers",
				"{ \"_m0\" : \"$CompanyName\" , \"_m1\" : \"$ContactName\"}",
				"{ \"ContactName\" : { \"$ne\" :  null }}");
    }

    @Test
    public void testGtLt() throws Exception {
		helpExecute(
				"SELECT age,status FROM users WHERE age > 25 AND age <= 50",
				"users", "{ \"_m0\" : \"$age\" , \"_m1\" : \"$status\"}",
				"{ \"$and\" : [ { \"age\" : { \"$gt\" : 25}} , { \"age\" : { \"$lte\" : 50}}]}");
    }

    @Test
    public void testLike() throws Exception {
		helpExecute(
				"SELECT user_id, age, status FROM users WHERE user_id like '%bc%'",
				"users",
				"{ \"_m0\" : \"$user_id\" , \"_m1\" : \"$age\" , \"_m2\" : \"$status\"}",
				"{ \"user_id.$id\" : \"/bc/\"}");
    }

    @Test
    public void testLike2() throws Exception {
		helpExecute(
				"SELECT user_id, age, status FROM users WHERE user_id like 'bc%'",
				"users",
				"{ \"_m0\" : \"$user_id\" , \"_m1\" : \"$age\" , \"_m2\" : \"$status\"}",
				"{ \"user_id.$id\" : \"/^bc/\"}");
    }

    @Test
    public void testLike3() throws Exception {
		helpExecute(
				"SELECT user_id, age, status FROM users WHERE user_id like 'b%c'",
				"users",
				"{ \"_m0\" : \"$user_id\" , \"_m1\" : \"$age\" , \"_m2\" : \"$status\"}",
				"{ \"user_id.$id\" : \"/^b.*c$/\"}");
    }

    @Test
    public void testLike4() throws Exception {
		helpExecute(
				"SELECT user_id, age, status FROM users WHERE user_id NOT LIKE 'b%c'",
				"users",
				"{ \"_m0\" : \"$user_id\" , \"_m1\" : \"$age\" , \"_m2\" : \"$status\"}",
				"{ \"user_id.$id\" : { \"$not\" : \"/^b.*c$/\"}}");
    }

    @Test
    public void testLimit() throws Exception {
    	String query = "SELECT user_id, age, status FROM users LIMIT 2,5";
    	Select cmd = (Select)this.utility.parseCommand(query);
    	MongoDBSelectVisitor visitor = new MongoDBSelectVisitor(this.translator, this.utility.createRuntimeMetadata());
    	visitor.visitNode(cmd);
    	assertEquals(new Integer(5), visitor.limit);
    	assertEquals(new Integer(2), visitor.skip);
    }

    @Test
    public void testOrderBy() throws Exception {
    	String query = "SELECT user_id, age, status FROM users ORDER BY age, status DESC";
    	Select cmd = (Select)this.utility.parseCommand(query);
    	MongoDBSelectVisitor visitor = new MongoDBSelectVisitor(this.translator, this.utility.createRuntimeMetadata());
    	visitor.visitNode(cmd);
    	BasicDBObject expected = new BasicDBObject("_m1", 1);
    	expected.put("_m2", -1);
    	System.out.println(visitor.project.toString());
    	assertEquals(expected, visitor.sort);
    }

    @Test
    public void testCountStar() throws Exception {
    	String query = "SELECT COUNT(*) AS allusers FROM users";
		helpExecute(query, "users", "{ \"allusers\" : 1}", null,
				"{ \"_id\" :  null  , \"allusers\" : { \"$sum\" : 1}}", null);
    }

    @Test
    public void testCountStarWithoutAlias() throws Exception {
    	String query = "SELECT COUNT(*) FROM users";
		helpExecute(query, "users", "{ \"_m0\" : 1}", null,
				"{ \"_m0\" : { \"$sum\" : 1} , \"_id\" :  null }", null);
    }

    @Test
    public void testCountStarWithDistinct() throws Exception {
    	String query = "SELECT DISTINCT COUNT(*) FROM users";
		helpExecute(query, "users", "{ \"_m0\" : 1}", null,
				"{ \"_m0\" : { \"$sum\" : 1} , \"_id\" :  null }", null);
    }
    @Test
    public void testDistinct() throws Exception {
    	String query = "SELECT DISTINCT user_id, age FROM users";
		helpExecute(query, "users",
				"{ \"_m0\" : \"$_id._m0\" , \"_m1\" : \"$_id._m1\"}", //project
				null, 												  // match
				"{ \"_id\" : { \"_m0\" : \"$user_id\" , \"_m1\" : \"$age\"}}", //group by
				null); 												  // having
    }

    @Test
    public void testDistinctEquivalent() throws Exception {
    	String query = "SELECT user_id, age age FROM users group by user_id, age";

		helpExecute(
				query,
				"users",
				"{ \"_m0\" : \"$_id.user_id\" , \"age\" : \"$_id.age\"}",
				null,
				"{ \"_id\" : { \"user_id\" : \"$user_id\" , \"age\" : \"$age\"}}",
				null);
    }

    @Test
    public void testSum() throws Exception {
    	String query = "SELECT SUM(age) as total FROM users";
		helpExecute(query, "users", "{ \"total\" : 1}", null,
				"{ \"total\" : { \"$sum\" : \"$age\"} , \"_id\" :  null }",
				null);
    }

    @Test
    public void testSumWithGroupBy() throws Exception {
    	String query = "SELECT SUM(age) as total FROM users GROUP BY user_id";
		helpExecute(
				query,
				"users",
				"{ \"total\" : 1}",
				null,
				"{ \"total\" : { \"$sum\" : \"$age\"} , \"_id\" : \"$user_id\"}",
				null);
    }

    @Test
    public void testSumWithGroupBy2() throws Exception {
    	String query = "SELECT user_id, status, SUM(age) as total FROM users GROUP BY user_id, status";
		helpExecute(
				query,
				"users",
				"{ \"_m0\" : \"$_id.user_id\" , \"_m1\" : \"$_id.status\" , \"total\" : 1}",
				null,
				"{ \"total\" : { \"$sum\" : \"$age\"} , \"_id\" : { \"user_id\" : \"$user_id\" , \"status\" : \"$status\"}}",
				null);
    }

    @Test
    public void testAggregateWithHaving() throws Exception {
    	String query = "SELECT SUM(age) as total FROM users GROUP BY user_id HAVING SUM(age) > 250";
		helpExecute(
				query,
				"users",
				"{ \"total\" : 1}",
				null,
				"{ \"total\" : { \"$sum\" : \"$age\"} , \"_id\" : \"$user_id\"}",
				"{ \"total\" : { \"$gt\" : 250}}");
    }

    @Test
    public void testAggregateWithHaving1() throws Exception {
    	String query = "SELECT age FROM users GROUP BY user_id HAVING SUM(age) > 250";
		helpExecute(query, "users", "{ \"_m0\" : \"$age\" , \"_m1\" : 1}",
				null,
				"{ \"_m1\" : { \"$sum\" : \"$age\"} , \"_id\" : \"$user_id\"}",
				"{ \"_m1\" : { \"$gt\" : 250}}");
    }

    @Test
    public void testAggregateWithHavingAndWhere() throws Exception {
    	String query = "SELECT SUM(age) as total FROM users WHERE age > 45 GROUP BY user_id HAVING SUM(age) > 250";
		helpExecute(
				query,
				"users",
				"{ \"total\" : 1}",
				"{ \"age\" : { \"$gt\" : 45}}",
				"{ \"total\" : { \"$sum\" : \"$age\"} , \"_id\" : \"$user_id\"}",
				"{ \"total\" : { \"$gt\" : 250}}");
    }

    @Test
    public void testPlusOperatorWithAlias() throws Exception {
    	String query = "SELECT (age+age) as total FROM users";
    	helpExecute(query, "users", "{ \"total\" : { \"$add\" : [ \"$age\" , \"$age\"]}}", null, null, null);
    }

    @Test
    public void testPlusOperatorWithOutAlias() throws Exception {
    	String query = "SELECT (age+age) FROM users";
    	helpExecute(query, "users", "{ \"_m0\" : { \"$add\" : [ \"$age\" , \"$age\"]}}", null, null, null);
    }

    //TODO: fix this
    public void testPlusOperatorInWhere() throws Exception {
    	String query = "SELECT age FROM users WHERE age*2 > 2.5";
    	helpExecute(query, "users", "{ \"_m0\" : \"$age\"}", "{ \"$divide\" : [ \"$age\" , 2]}");
    }

    @Test
    public void testPlusOperatorInWhere2() throws Exception {
    	String query = "SELECT age FROM users WHERE age/2 > age*3";
		helpExecute(
				query,
				"users",
				"{ \"_m0\" : { \"$divide\" : [ \"$age\" , 2]} , \"_m1\" : { \"$multiply\" : [ \"$age\" , 3]} , \"_m2\" : \"$age\"}",
				"{ \"_m0\" : { \"$gt\" : \"_m1\"}}");
    }

    @Test
    public void testFunction() throws Exception {
    	String query = "SELECT concat(user_id, user_id) FROM users";
		helpExecute(query, "users",
				"{ \"_m0\" : { \"$concat\" : [ \"$user_id\" , \"$user_id\"]}}",
				null);
    }

    @Test
    public void testWhereReference() throws Exception {
    	String query = "SELECT age FROM users WHERE user_id = 'bob'";
		helpExecute(query, "users",
				"{ \"_m0\" : \"$age\"}",
				"{ \"user_id.$id\" : \"bob\"}");
    }

    @Test
    public void testSelectStarCompositeKey() throws Exception {
    	String query = "SELECT * from G1 where e1 = 50";
		helpExecute(query, "G1",
				"{ \"_m0\" : \"$_id.e1\" , \"_m1\" : \"$_id.e2\" , \"_m2\" : \"$e3\"}",
				"{ \"_id.e1\" : 50}");
    }

    @Test
    public void testCompositeFKKeyWhere() throws Exception {
    	String query = "SELECT * from G2 where e2 = 50";
		helpExecute(query, "G2",
				"{ \"_m0\" : \"$e1\" , \"_m1\" : \"$e2\" , \"_m2\" : \"$e3\"}",
				"{ \"e2.$id.e2\" : 50}");
    }
}
