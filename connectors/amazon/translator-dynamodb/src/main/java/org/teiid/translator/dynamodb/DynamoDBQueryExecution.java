package org.teiid.translator.dynamodb;

import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

import java.util.List;

public class DynamoDBQueryExecution implements ResultSetExecution {
    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
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

    }
}
