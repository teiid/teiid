package org.teiid.translator.couchbase;

import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.RuntimeMetadata;

public class N1QLVisitor extends SQLStringVisitor{
    
    private RuntimeMetadata metadata;
    private CouchbaseExecutionFactory ef;

    public N1QLVisitor(CouchbaseExecutionFactory ef, RuntimeMetadata metadata) {
        this.ef = ef;
        this.metadata = metadata;
    }
}
