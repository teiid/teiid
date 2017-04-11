package org.teiid.translator.couchbase;

import org.teiid.couchbase.CouchbaseConnection;
import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

public class CouchbaseUpdateExecution extends CouchbaseExecution implements UpdateExecution {
    
    private Command command;

    protected CouchbaseUpdateExecution(Command command, CouchbaseExecutionFactory ef, ExecutionContext context, RuntimeMetadata metadata, CouchbaseConnection conn) {
        super(ef, context, metadata, conn);
        this.command = command;
    }

    @Override
    public void execute() throws TranslatorException {
        // TODO Auto-generated method stub

    }

    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public void cancel() throws TranslatorException {
        // TODO Auto-generated method stub

    }

}
