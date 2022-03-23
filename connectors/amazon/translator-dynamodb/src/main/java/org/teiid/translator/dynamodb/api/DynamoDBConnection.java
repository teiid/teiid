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

import com.amazonaws.services.dynamodbv2.model.*;
import org.teiid.resource.api.Connection;
import org.teiid.translator.TranslatorException;

import java.util.List;

public interface DynamoDBConnection extends Connection {

    public void createTable(String tableName, List<KeySchemaElement> keySchema, List<AttributeDefinition> attributeDefinitions) throws TranslatorException;

    public void deleteTable(String tableName) throws TranslatorException;

    public List<AttributeDefinition> getTableAttributeNames(String tableName) throws TranslatorException;

    public List<String> getTableNames() throws TranslatorException;

    public QueryResult executeSelect(QueryRequest queryRequest);
}
