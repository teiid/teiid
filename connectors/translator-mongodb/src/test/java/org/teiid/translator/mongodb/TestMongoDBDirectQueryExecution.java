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
import java.util.Arrays;

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

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

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
		
		AggregationOutput output = Mockito.mock(AggregationOutput.class);
		Mockito.stub(output.results()).toReturn(new ArrayList<DBObject>());
		
		Mockito.stub(dbCollection.aggregate(Mockito.any(DBObject.class),Mockito.any(DBObject.class))).toReturn(output);
		
		
		Argument arg = new Argument(Direction.IN, null, String.class, null);
		arg.setArgumentValue(new Literal("MyTable;{$match:{\"id\":\"$1\"}};{$project:{\"_m0\":\"$user\"}}", String.class));

		Argument arg2 = new Argument(Direction.IN, null, String.class, null);
		arg2.setArgumentValue(new Literal("foo", String.class));
		
		ResultSetExecution execution = this.translator.createDirectExecution(Arrays.asList(arg, arg2), cmd, context, this.utility.createRuntimeMetadata(), connection);
		execution.execute();
		Mockito.verify(dbCollection).aggregate(
				new BasicDBObject("$match", new BasicDBObject("id", "foo")), 
				new BasicDBObject("$project", new BasicDBObject("_m0", "$user")));
	}
}
