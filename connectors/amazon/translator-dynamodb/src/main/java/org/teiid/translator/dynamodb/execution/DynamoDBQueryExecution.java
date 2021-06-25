package org.teiid.translator.dynamodb.execution;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.dynamodb.api.DynamoDBConnection;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DynamoDBQueryExecution implements ResultSetExecution {
    private ExecutionContext executionContext;
    private RuntimeMetadata metadata;
    private DynamoDBConnection dynamoDBConnection;
    private DynamoDBSQLVisitor sqlVisitor;

    private QueryRequest queryRequest;
    private static final int MAX_PAGE_SIZE = 2500;
    private Class<?>[] expectedColumnTypes;
    private Map<String, AttributeValue> lastEvaluatedKey = null;

    private Iterator<Map<String, AttributeValue>> queryResultListIterator;

    public DynamoDBQueryExecution(final Select command,
              ExecutionContext executionContext, RuntimeMetadata metadata,
              final DynamoDBConnection dynamoDBConnection) throws TranslatorException {
        this.executionContext = executionContext;
        this.metadata = metadata;
        this.dynamoDBConnection = dynamoDBConnection;
        this.sqlVisitor = new DynamoDBSQLVisitor();
        this.queryRequest = new QueryRequest();

        if(command != null) {
            this.sqlVisitor.append(command);
            this.sqlVisitor.checkExceptions();
            this.expectedColumnTypes = command.getColumnTypes();
        }
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        List<?> nextRowResultSet = getNextQueryRow();
        if(nextRowResultSet != null) {
            return nextRowResultSet;
        }
        if(this.lastEvaluatedKey != null) {
            this.execute();
            nextRowResultSet = getNextQueryRow();
            if(nextRowResultSet != null) {
                return nextRowResultSet;
            }
        }
        return null;
    }

    @Override
    public void close() {
    }

    @Override
    public void cancel() throws TranslatorException {
    }

    @Override
    public void execute() throws TranslatorException {
        QueryResult queryResult =  dynamoDBConnection.executeSelect(this.updateQueryRequest());
        this.updateItemsFromFetchedResult(queryResult);
    }

    private List<?> getNextQueryRow() {
        if(this.queryResultListIterator != null) {
            if(this.queryResultListIterator.hasNext()) {
                return buildRow(this.queryResultListIterator.next());
            }
        }
        return null;
    }

    private List<?> buildRow(Map<String, AttributeValue> item) {
        return null;
    }

    private void updateItemsFromFetchedResult(QueryResult queryResult) {
        this.lastEvaluatedKey = queryResult.getLastEvaluatedKey();
        this.queryResultListIterator = queryResult.getItems().iterator();
    }

    private QueryRequest updateQueryRequest() {
        //TODO: make query request
        queryRequest.setLimit(Math.min(this.executionContext.getBatchSize(), MAX_PAGE_SIZE));
        if(this.lastEvaluatedKey != null) {
            queryRequest.setExclusiveStartKey(this.lastEvaluatedKey);
        }
        return queryRequest;
    }
}
