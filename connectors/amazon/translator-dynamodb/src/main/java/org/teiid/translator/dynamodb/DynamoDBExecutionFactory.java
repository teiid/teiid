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
package org.teiid.translator.dynamodb;

import org.teiid.language.Command;
import org.teiid.language.Delete;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.resource.api.ConnectionFactory;
import org.teiid.translator.*;
import org.teiid.translator.dynamodb.api.DynamoDBConnection;
import org.teiid.translator.dynamodb.execution.DynamoDBDeleteExecute;
import org.teiid.translator.dynamodb.execution.DynamoDBQueryExecution;

@Translator(name = "dynamodb", description = "Translator for Dynamo DB")
public class DynamoDBExecutionFactory extends ExecutionFactory<ConnectionFactory, DynamoDBConnection> {

    @Override
    public MetadataProcessor<DynamoDBConnection> getMetadataProcessor() {
        return new DynamoDBMetadataProcessor();
    }

    @Override
    public UpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, DynamoDBConnection connection) throws TranslatorException {
        if(command instanceof Delete) {
            return new DynamoDBDeleteExecute(command, connection);
        } else {
            throw new TranslatorException("Just DELETE are supported");
        }
    }

    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, DynamoDBConnection connection) throws TranslatorException {
        return new DynamoDBQueryExecution((Select)command, executionContext, metadata, connection);
    }
}
