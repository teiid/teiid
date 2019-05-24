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

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Select;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.function.UDFSource;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.TestDDLParser;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.ValidatorReport;

import com.mongodb.BasicDBObject;

@SuppressWarnings("nls")
public class TestMongoDBSelectVisitor {
    private MongoDBExecutionFactory translator;
    private TranslationUtility utility;

    @Before
    public void setUp() throws Exception {
        this.translator = new MongoDBExecutionFactory();
        this.translator.start();

        MetadataFactory mf = TestDDLParser.helpParse(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("northwind.ddl")), "northwind");

        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "sakila", new FunctionTree("mongo", new UDFSource(translator.getPushDownFunctions())));
        ValidatorReport report = new MetadataValidator().validate(metadata.getVdbMetaData(), metadata.getMetadataStore());
        if (report.hasItems()) {
            throw new RuntimeException(report.getFailureMessage());
        }
        this.utility = new TranslationUtility(metadata);
    }

    private void helpExecute(String query, String collection, String project, String match) throws Exception {
        helpExecute(query, collection, project, match, null, null);
    }
    private MongoDBSelectVisitor helpExecute(String query, String collection, String project, String match, String groupby, String having) throws Exception {
        Select cmd = (Select)this.utility.parseCommand(query);
        MongoDBSelectVisitor visitor = new MongoDBSelectVisitor(this.translator, this.utility.createRuntimeMetadata());
        visitor.visitNode(cmd);
        if (!visitor.exceptions.isEmpty()) {
            throw visitor.exceptions.get(0);
        }

        assertEquals(collection, visitor.mongoDoc.getTargetTable().getName());
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
        return visitor;
    }

    @Test
    public void testSelectStar() throws Exception {
        helpExecute(
                "select * from customers",
                "Customers",
                "{ \"_m0\" : \"$_id\", \"_m1\" : \"$CompanyName\", \"_m2\" : \"$ContactName\", \"_m3\" : \"$ContactTitle\", \"_m4\" : \"$Address\", \"_m5\" : \"$City\", \"_m6\" : \"$Region\", \"_m7\" : \"$PostalCode\", \"_m8\" : \"$Country\", \"_m9\" : \"$Phone\", \"_m10\" : \"$Fax\" }",
                null);
    }

    @Test
    public void testSelectColum() throws Exception {
        helpExecute("select CompanyName, ContactName from customers",
                "Customers",
                "{ \"_m0\" : \"$CompanyName\", \"_m1\" : \"$ContactName\" }",
                null);
    }

    @Test
    public void testWhereEQ() throws Exception {
        helpExecute(
                "SELECT CompanyName, ContactName FROM customers WHERE CompanyName = 'A'",
                "Customers",
                "{ \"_m0\" : \"$CompanyName\", \"_m1\" : \"$ContactName\" }",
                "{ \"CompanyName\" : \"A\" }");
    }

    @Test
    public void testAND() throws Exception {
        helpExecute(
                "SELECT CompanyName, ContactName FROM customers WHERE CompanyName = 'A' AND ContactName = 'B'",
                "Customers",
                "{ \"_m0\" : \"$CompanyName\", \"_m1\" : \"$ContactName\" }",
                "{ \"$and\" : [{ \"CompanyName\" : \"A\" }, { \"ContactName\" : \"B\" }] }");
    }

    @Test
    public void testOR() throws Exception {
        helpExecute(
                "SELECT CompanyName, ContactName FROM customers WHERE CompanyName = 'A' OR ContactName = 'B'",
                "Customers",
                "{ \"_m0\" : \"$CompanyName\", \"_m1\" : \"$ContactName\" }",
                "{ \"$or\" : [{ \"CompanyName\" : \"A\" }, { \"ContactName\" : \"B\" }] }");
    }


    @Test
    public void testComplexAndOr() throws Exception {
        helpExecute(
                "SELECT CompanyName, ContactName FROM customers WHERE (CompanyName = 'A' AND ContactName = 'B') OR (CompanyName = 'B' AND ContactName = 'A')",
                "Customers",
                "{ \"_m0\" : \"$CompanyName\", \"_m1\" : \"$ContactName\" }",
                "{ \"$or\" : [{ \"$and\" : [{ \"CompanyName\" : \"A\" }, { \"ContactName\" : \"B\" }] }, { \"$and\" : [{ \"CompanyName\" : \"B\" }, { \"ContactName\" : \"A\" }] }] }");
    }

    @Test
    public void testComplexOrAnd() throws Exception {
        helpExecute(
                "SELECT CompanyName, ContactName FROM customers WHERE (CompanyName = 'A' OR ContactName = 'B') AND (CompanyName = 'B' OR ContactName = 'A')",
                "Customers",
                "{ \"_m0\" : \"$CompanyName\", \"_m1\" : \"$ContactName\" }",
                "{ \"$and\" : [{ \"$or\" : [{ \"CompanyName\" : \"A\" }, { \"ContactName\" : \"B\" }] }, { \"$or\" : [{ \"CompanyName\" : \"B\" }, { \"ContactName\" : \"A\" }] }] }");
    }

    @Test
    public void testOrRewriteToIn() throws Exception {
        helpExecute(
                "SELECT CompanyName, ContactName FROM customers WHERE CompanyName = 'A' OR CompanyName = 'B'",
                "Customers",
                "{ \"_m0\" : \"$CompanyName\", \"_m1\" : \"$ContactName\" }",
                "{ \"CompanyName\" : { \"$in\" : [\"B\", \"A\"] } }");
    }

    @Test
    public void testIn() throws Exception {
        helpExecute(
                "SELECT CompanyName, ContactName FROM customers WHERE CompanyName IN('A', 'B')",
                "Customers",
                "{ \"_m0\" : \"$CompanyName\", \"_m1\" : \"$ContactName\" }",
                "{ \"CompanyName\" : { \"$in\" : [\"A\", \"B\"] } }");
    }

    @Test
    public void testNotIn() throws Exception {
        helpExecute(
                "SELECT CompanyName, ContactName FROM customers WHERE CompanyName NOT IN ('A', 'B')",
                "Customers",
                "{ \"_m0\" : \"$CompanyName\", \"_m1\" : \"$ContactName\" }",
                "{ \"CompanyName\" : { \"$nin\" : [\"A\", \"B\"] } }");
    }

    @Test
    public void testIsNull() throws Exception {
        helpExecute(
                "SELECT CompanyName, ContactName FROM Customers WHERE ContactName IS NULL",
                "Customers",
                "{ \"_m0\" : \"$CompanyName\", \"_m1\" : \"$ContactName\" }",
                "{ \"ContactName\" : null }");
    }

    @Test
    public void testIsNotNull() throws Exception {
        helpExecute(
                "SELECT CompanyName, ContactName FROM Customers WHERE ContactName IS NOT NULL",
                "Customers",
                "{ \"_m0\" : \"$CompanyName\", \"_m1\" : \"$ContactName\" }",
                "{ \"ContactName\" : { \"$ne\" : null } }");
    }

    @Test
    public void testGtLt() throws Exception {
        helpExecute(
                "SELECT age,status FROM users WHERE age > 25 AND age <= 50",
                "users", "{ \"_m0\" : \"$age\", \"_m1\" : \"$status\" }",
                "{ \"$and\" : [{ \"age\" : { \"$gt\" : 25 } }, { \"age\" : { \"$lte\" : 50 } }] }");
    }

    @Test
    public void testLike() throws Exception {
        helpExecute(
                "SELECT user_id, age, status FROM users WHERE user_id like '%bc%'",
                "users",
                "{ \"_m0\" : \"$user_id\", \"_m1\" : \"$age\", \"_m2\" : \"$status\" }",
                "{ \"user_id\" : { \"$regex\" : \"bc\", \"$options\" : \"\" } }");
    }

    @Test
    public void testLike2() throws Exception {
        helpExecute(
                "SELECT user_id, age, status FROM users WHERE user_id like 'bc%'",
                "users",
                "{ \"_m0\" : \"$user_id\", \"_m1\" : \"$age\", \"_m2\" : \"$status\" }",
                "{ \"user_id\" : { \"$regex\" : \"^bc\", \"$options\" : \"\" } }");
    }

    @Test
    public void testLike3() throws Exception {
        helpExecute(
                "SELECT user_id, age, status FROM users WHERE user_id like 'b%c'",
                "users",
                "{ \"_m0\" : \"$user_id\", \"_m1\" : \"$age\", \"_m2\" : \"$status\" }",
                "{ \"user_id\" : { \"$regex\" : \"^b.*c$\", \"$options\" : \"\" } }");
    }

    @Test
    public void testLike4() throws Exception {
        helpExecute(
                "SELECT user_id, age, status FROM users WHERE user_id NOT LIKE 'b%c'",
                "users",
                "{ \"_m0\" : \"$user_id\", \"_m1\" : \"$age\", \"_m2\" : \"$status\" }",
                "{ \"user_id\" : { \"$not\" : { \"$regex\" : \"^b.*c$\", \"$options\" : \"\" } } }");
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
        assertEquals(expected, visitor.sort);
    }

    @Test
    public void testCountStar() throws Exception {
        String query = "SELECT COUNT(*) AS allusers FROM users";
        helpExecute(query, "users", "{ \"allusers\" : 1 }", null,
                "{ \"_id\" : null, \"allusers\" : { \"$sum\" : 1 } }", null);
    }

    @Test
    public void testCountStarWithoutAlias() throws Exception {
        String query = "SELECT COUNT(*) FROM users";
        helpExecute(query, "users", "{ \"_m0\" : 1 }", null,
                "{ \"_id\" : null, \"_m0\" : { \"$sum\" : 1 } }", null);
    }

    @Test
    public void testCountStarWithDistinct() throws Exception {
        String query = "SELECT DISTINCT COUNT(*) FROM users";
        helpExecute(query, "users", "{ \"_m0\" : 1 }", null,
                "{ \"_id\" : null, \"_m0\" : { \"$sum\" : 1 } }", null);
    }
    @Test
    public void testDistinct() throws Exception {
        String query = "SELECT DISTINCT user_id, age FROM users";
        helpExecute(query, "users",
                "{ \"_m0\" : \"$_id._m0\", \"_m1\" : \"$_id._m1\" }", //project
                null,                                                   // match
                "{ \"_id\" : { \"_m0\" : \"$user_id\", \"_m1\" : \"$age\" } }", //group by
                null);                                                   // having
    }

    @Test
    public void testDistinctEquivalent() throws Exception {
        String query = "SELECT user_id, age age FROM users group by user_id, age";

        MongoDBSelectVisitor visitor = helpExecute(
                query,
                "users",
                "{ \"_m0\" : \"$_id._c0\", \"age\" : \"$_id._c1\" }",
                null,
                "{ \"_id\" : { \"_c0\" : \"$user_id\", \"_c1\" : \"$age\" } }",
                null);
        assertEquals(Arrays.asList("_m0", "age"), visitor.selectColumnReferences);
    }

    @Test
    public void testAggregateWithGroupBy() throws Exception {
        String query = "SELECT user_id, sum(age) FROM users group by user_id";

        MongoDBSelectVisitor visitor = helpExecute(
                query,
                "users",
                "{ \"_m0\" : \"$_id._c0\", \"_m1\" : 1 }",
                null,
                "{ \"_id\" : { \"_c0\" : \"$user_id\" }, \"_m1\" : { \"$sum\" : \"$age\" } }",
                null);
        assertEquals(Arrays.asList("_m0", "_m1"), visitor.selectColumnReferences);
    }

    @Test
    public void testSum() throws Exception {
        String query = "SELECT SUM(age) as total FROM users";
        helpExecute(query, "users", "{ \"total\" : 1 }", null,
                "{ \"_id\" : null, \"total\" : { \"$sum\" : \"$age\" } }",
                null);
    }

    @Test
    public void testPlusOperatorWithAlias() throws Exception {
        String query = "SELECT (age+age) as total FROM users";
        helpExecute(query, "users", "{ \"total\" : { \"$add\" : [\"$age\", \"$age\"] } }", null, null, null);
    }

    @Test
    public void testPlusOperatorWithOutAlias() throws Exception {
        String query = "SELECT (age+age) FROM users";
        helpExecute(query, "users", "{ \"_m0\" : { \"$add\" : [\"$age\", \"$age\"] } }", null, null, null);
    }

    @Test
    public void testPlusOperatorInWhere() throws Exception {
        String query = "SELECT age FROM users WHERE age > 5.0";
        helpExecute(query, "users", "{ \"_m0\" : \"$age\" }", "{ \"age\" : { \"$gt\" : 5 } }");
    }

    @Test
    public void testFunction() throws Exception {
        String query = "SELECT concat(user_id, user_id) FROM users";
        helpExecute(query, "users",
                "{ \"_m0\" : { \"$concat\" : [\"$user_id\", \"$user_id\"] } }",
                null);
    }

    @Test
    public void testSelectBooleanExpression() throws Exception {
        String query = "SELECT (user_id = 'USER') as X1 FROM users";
        helpExecute(query, "users",
                "{ \"X1\" : { \"$cond\" : [{ \"$eq\" : [\"$user_id\", \"USER\"] }, true, false] } }",
                null);
    }

    @Test
    public void testSelectBooleanExpression2() throws Exception {
        String query = "SELECT (user_id > 'USER') as X1 FROM users";
        helpExecute(query, "users",
                "{ \"X1\" : { \"$cond\" : [{ \"$gt\" : [\"$user_id\", \"USER\"] }, true, false] } }",
                null);
    }

    @Test
    public void testSelectBooleanExpression3() throws Exception {
        String query = "SELECT (user_id = 'USER' OR user_id = 'user') as X1 FROM users";
        helpExecute(query, "users",
                "{ \"X1\" : { \"$cond\" : [{ \"user_id\" : { \"$in\" : [\"user\", \"USER\"] } }, true, false] } }",
                null);
    }

    @Test
    public void testSelectBooleanExpression4() throws Exception {
        String query = "SELECT (user_id = 'USER' AND age > 30) as X1 FROM users";
        helpExecute(query, "users",
                "{ \"X1\" : { \"$cond\" : [{ \"$and\" : [{ \"$eq\" : [\"$user_id\", \"USER\"] }, { \"$gt\" : [\"$age\", 30] }] }, true, false] } }",
                null);
    }

    @Test
    public void testNestedFunction() throws Exception {
        String query = "SELECT concat(concat(user_id, user_id), user_id) FROM users";
        helpExecute(query, "users",
                "{ \"_m0\" : { \"$concat\" : [{ \"$concat\" : [\"$user_id\", \"$user_id\"] }, \"$user_id\"] } }",
                null);
    }

    @Test
    public void testWhereReference() throws Exception {
        String query = "SELECT age FROM users WHERE user_id = 'bob'";
        helpExecute(query, "users",
                "{ \"_m0\" : \"$age\" }",
                "{ \"user_id\" : \"bob\" }");
    }

    @Test
    public void testSelectStarCompositeKey() throws Exception {
        String query = "SELECT * from G1 where e1 = 50";
        helpExecute(query, "G1",
                "{ \"_m0\" : \"$_id.e1\", \"_m1\" : \"$_id.e2\", \"_m2\" : \"$e3\" }",
                "{ \"_id.e1\" : 50 }");
    }

    @Test
    public void testCompositeFKKeyWhere() throws Exception {
        String query = "SELECT * from G2 where e2 = 50";
        helpExecute(query, "G2",
                "{ \"_m0\" : \"$e1\", \"_m1\" : \"$e2\", \"_m2\" : \"$e3\" }",
                "{ \"e2\" : 50 }");
    }

    @Test
    public void testGeoWithinPloygonFunction() throws Exception {
        String query = "SELECT mongo.geoWithin(user_id, 'LineString', ((cast(1.0 as double), cast(2.0 as double)), (cast(1.0 as double), cast(2.0 as double)))) FROM users";
        helpExecute(query, "users",
                "{ \"_m0\" : { \"user_id\" : { \"$geoWithin\" : { \"$geometry\" : { \"type\" : \"LineString\", \"coordinates\" : [[[1.0, 2.0], [1.0, 2.0]]] } } } } }",
                null);
    }

    @Test
    public void testGeoNearFunction() throws Exception {
        String query = "SELECT mongo.geonear(user_id, (cast(1.0 as double), cast(2.0 as double)), 22, 10) FROM users";
        helpExecute(query, "users",
                "{ \"_m0\" : { \"user_id\" : { \"$near\" : { \"$geometry\" : { \"type\" : \"Point\", \"coordinates\" : [[1.0, 2.0]] }, \"$maxDistance\" : 22, \"$minDistance\" : 10 } } } }",
                null);
    }

    @Test
    public void testGeoWithinPloygonFunctionInWhere() throws Exception {
        String query = "SELECT user_id FROM users where mongo.geoWithin(user_id, 'LineString', ((cast(1.0 as double), cast(2.0 as double)), (cast(1.0 as double), cast(2.0 as double))))";
        helpExecute(query, "users",
                "{ \"_m1\" : \"$user_id\" }",
                "{ \"user_id\" : { \"$geoWithin\" : { \"$geometry\" : { \"type\" : \"LineString\", \"coordinates\" : [[[1.0, 2.0], [1.0, 2.0]]] } } } }"
                );
    }

    @Test
    public void testAliasPloygonFunctionInWhere() throws Exception {
        String query = "SELECT user_id FROM users where mongo.geoPolygonWithin(user_id, 1.0, 2.0, 3.0, 4.0)";
        helpExecute(query, "users",
                "{ \"_m1\" : \"$user_id\" }",
                "{ \"user_id\" : { \"$geoWithin\" : { \"$geometry\" : { \"type\" : \"Polygon\", \"coordinates\" : [[[3.0, 1.0], [2.0, 1.0], [2.0, 4.0], [3.0, 4.0], [3.0, 1.0]]] } } } }"
                );
    }
}
