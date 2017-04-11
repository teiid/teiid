package org.teiid.translator.couchbase;

import static org.teiid.language.SQLConstants.NonReserved.KEY;
import static org.teiid.language.SQLConstants.Reserved.INTO;
import static org.teiid.language.SQLConstants.Reserved.VALUE;
import static org.teiid.language.SQLConstants.Tokens.COMMA;
import static org.teiid.language.SQLConstants.Tokens.SPACE;
import static org.teiid.language.SQLConstants.Tokens.LPAREN;
import static org.teiid.language.SQLConstants.Tokens.RPAREN;

import org.teiid.language.Insert;
import org.teiid.language.NamedTable;

public class N1QLUpdateVisitor extends N1QLVisitor {

    public N1QLUpdateVisitor(CouchbaseExecutionFactory ef) {
        super(ef);
    }
    
    @Override
    public void visit(Insert obj) {
        if (obj.isUpsert()) {
            buffer.append(getUpsertKeyword()).append(SPACE);
        } else {
            buffer.append(getInsertKeyword()).append(SPACE);
        }
        buffer.append(INTO).append(SPACE);
        append(obj.getTable());
        buffer.append(SPACE).append(LPAREN);
        buffer.append(KEY).append(COMMA).append(SPACE);
        buffer.append(VALUE).append(RPAREN).append(SPACE);
        
        append(obj.getValueSource());
    }

    @Override
    public void visit(NamedTable obj) {
        
        retrieveTableProperty(obj);
        String tableNameInSource = obj.getMetadataObject().getNameInSource();
        if(isArrayTable) {
            
        } else {
            buffer.append(tableNameInSource); 
        }
    }
}
