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

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Argument;
import org.teiid.language.Argument.Direction;
import org.teiid.language.Command;
import org.teiid.language.Literal;
import org.teiid.mongodb.MongoDBConnection;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;

import com.mongodb.AggregationOptions;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

@SuppressWarnings("nls")
public class TestMongoDBDirectQueryExecution {
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
    public void testDirect() throws Exception {
        Command cmd = this.utility.parseCommand("SELECT * FROM Customers");
        MongoDBConnection connection = Mockito.mock(MongoDBConnection.class);
        ExecutionContext context = Mockito.mock(ExecutionContext.class);
        DBCollection dbCollection = Mockito.mock(DBCollection.class);
        DB db = Mockito.mock(DB.class);
        Mockito.stub(db.getCollection("MyTable")).toReturn(dbCollection);

        Mockito.stub(db.collectionExists(Mockito.anyString())).toReturn(true);
        Mockito.stub(connection.getDatabase()).toReturn(db);

        Argument arg = new Argument(Direction.IN, null, String.class, null);
        arg.setArgumentValue(new Literal("MyTable;{$match:{\"id\":\"$1\"}};{$project:{\"_m0\":\"$user\"}}", String.class));

        Argument arg2 = new Argument(Direction.IN, null, String.class, null);
        arg2.setArgumentValue(new Literal("foo", String.class));

        ResultSetExecution execution = this.translator.createDirectExecution(Arrays.asList(arg, arg2), cmd, context, this.utility.createRuntimeMetadata(), connection);
        execution.execute();

        List<DBObject> pipeline = TestMongoDBQueryExecution.buildArray(new BasicDBObject("$match", new BasicDBObject("id", "foo")),
                new BasicDBObject("$project", new BasicDBObject("_m0", "$user")));
        Mockito.verify(dbCollection).aggregate(Mockito.eq(pipeline), Mockito.any(AggregationOptions.class));

    }

    @Test
    public void testShellDirect() throws Exception {
        Command cmd = this.utility.parseCommand("SELECT * FROM Customers");
        MongoDBConnection connection = Mockito.mock(MongoDBConnection.class);
        ExecutionContext context = Mockito.mock(ExecutionContext.class);
        DBCollection dbCollection = Mockito.mock(DBCollection.class);
        DB db = Mockito.mock(DB.class);
        Mockito.stub(db.getCollection("MyTable")).toReturn(dbCollection);

        Mockito.stub(db.collectionExists(Mockito.anyString())).toReturn(true);
        Mockito.stub(connection.getDatabase()).toReturn(db);

        Argument arg = new Argument(Direction.IN, null, String.class, null);
        arg.setArgumentValue(new Literal("$ShellCmd;MyTable;remove;{ qty: { $gt: 20 }}", String.class));

        ResultSetExecution execution = this.translator.createDirectExecution(Arrays.asList(arg), cmd, context, this.utility.createRuntimeMetadata(), connection);
        execution.execute();
        Mockito.verify(dbCollection).remove(QueryBuilder.start("qty").greaterThan(20).get());
    }
}
