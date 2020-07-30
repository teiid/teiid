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
package org.teiid.translator.dynamodb.execution;

import org.teiid.language.Command;
import org.teiid.language.Delete;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.dynamodb.DynamoDBMetadataProcessor;
import org.teiid.translator.dynamodb.api.DynamoDBConnection;

public class DynamoDBDeleteExecute implements UpdateExecution {
    private DynamoDBConnection dynamoDBConnection;
    private DynamoDBDeleteVisitor deleteVisitor;
    private int updatedRowCount = 0;

    public DynamoDBDeleteExecute(Command command, DynamoDBConnection dynamoDBConnection) throws TranslatorException {
        this.dynamoDBConnection = dynamoDBConnection;
        this.deleteVisitor = new DynamoDBDeleteVisitor((Delete) command);
        this.deleteVisitor.checkExceptions();
    }

    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
        return new int[] { this.updatedRowCount };
    }

    @Override
    public void close() {
    }

    @Override
    public void cancel() throws TranslatorException {
    }

    @Override
    public void execute() throws TranslatorException {
        String tableName = DynamoDBMetadataProcessor.getName(this.deleteVisitor.getTable());

    }
}
