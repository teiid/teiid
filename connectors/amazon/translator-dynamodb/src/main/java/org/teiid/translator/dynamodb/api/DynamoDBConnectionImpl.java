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
package org.teiid.translator.dynamodb.api;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import org.teiid.translator.TranslatorException;

import java.util.List;

public class DynamoDBConnectionImpl implements DynamoDBConnection {
    private AmazonDynamoDB dynamoDBClient;

    public DynamoDBConnectionImpl(AmazonDynamoDB dynamoDBClient) {
        this.dynamoDBClient = dynamoDBClient;
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public void createTable(String tableName, List<KeySchemaElement> keySchema, List<AttributeDefinition> attributeDefinitions) throws TranslatorException {
        CreateTableRequest createTableRequest = new CreateTableRequest()
                .withTableName(tableName)
                .withAttributeDefinitions(attributeDefinitions)
                .withKeySchema(keySchema)
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits(5L)
                        .withWriteCapacityUnits(6L));

        try {
            this.dynamoDBClient
                    .createTable(createTableRequest);
        } catch(AmazonClientException exception) {
            throw new TranslatorException(exception);
        }
    }

    @Override
    public void deleteTable(String tableName) throws TranslatorException {
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest();
        deleteTableRequest.setTableName(tableName);
        try {
            this.dynamoDBClient
                    .deleteTable(deleteTableRequest);
        } catch(AmazonClientException exception) {
            throw new TranslatorException(exception);
        }
    }

    @Override
    public List<AttributeDefinition> getTableAttributeNames(String tableName) throws TranslatorException {
        DescribeTableRequest describeTableRequest = new DescribeTableRequest();
        describeTableRequest.setTableName(tableName);
        try {
            return this.dynamoDBClient
                    .describeTable(describeTableRequest)
                    .getTable()
                    .getAttributeDefinitions();
        } catch(AmazonClientException exception) {
            throw new TranslatorException(exception);
        }
    }

    @Override
    public List<String> getTableNames() throws TranslatorException {
        return this.dynamoDBClient
                .listTables()
                .getTableNames();
    }

    @Override
    public QueryResult executeSelect(QueryRequest queryRequest) {
        System.out.println(queryRequest);
        QueryResult queryResult = dynamoDBClient.query(queryRequest);
        return queryResult;
    }
}
