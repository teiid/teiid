package org.teiid.translator.couchbase;

import static org.teiid.language.SQLConstants.Reserved.CAST;
import static org.teiid.language.SQLConstants.Reserved.CONVERT;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.IS_ARRAY_TABLE;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.IS_TOP_TABLE;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.TRUE;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.FALSE;

import java.util.List;

import org.teiid.language.ColumnReference;
import org.teiid.language.Function;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.RuntimeMetadata;

public class N1QLVisitor extends SQLStringVisitor{
    
    private RuntimeMetadata metadata;
    private CouchbaseExecutionFactory ef;

    public N1QLVisitor(CouchbaseExecutionFactory ef, RuntimeMetadata metadata) {
        this.ef = ef;
        this.metadata = metadata;
    }

    @Override
    public void visit(ColumnReference obj) {
        
        String group = obj.getTable().getCorrelationName();
        String isArrayTable = obj.getTable().getMetadataObject().getProperty(IS_ARRAY_TABLE, false);
        String isTopTable = obj.getTable().getMetadataObject().getProperty(IS_TOP_TABLE, false);
        if(isTopTable.equals(FALSE) && group == null) {
            shortNameOnly = true;
            super.visit(obj);
            shortNameOnly = false;
        } else if(isArrayTable.equals(TRUE) && group != null) {
            buffer.append(group);
        }else {
            super.visit(obj);
        }
    }

    @Override
    public void visit(Function obj) {
        String functionName = obj.getName();
        if(functionName.equalsIgnoreCase(CONVERT) || functionName.equalsIgnoreCase(CAST)) {
            List<?> parts =  this.ef.getFunctionModifiers().get(functionName).translate(obj);
            buffer.append(parts.get(0));
            super.append(obj.getParameters().get(0));
            buffer.append(parts.get(2));
            return;
        } else if (this.ef.getFunctionModifiers().containsKey(functionName)) {
            List<?> parts =  this.ef.getFunctionModifiers().get(functionName).translate(obj);
            if (parts != null) {
                obj = (Function)parts.get(0);
            }
        } 
        super.visit(obj);
    }
}
