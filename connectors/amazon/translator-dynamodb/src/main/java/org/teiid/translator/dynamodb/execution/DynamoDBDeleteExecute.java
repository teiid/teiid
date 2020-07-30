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
        if (this.deleteVisitor.getCriteria() == null) {
            // this is table delete. otherwise this could be lot of items. deleted count can
            // not be measured.
            this.dynamoDBConnection.deleteTable(tableName);
        } else {

        }
    }
}
