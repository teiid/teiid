package org.teiid.translator.couchbase;

import static org.teiid.language.SQLConstants.Reserved.CAST;
import static org.teiid.language.SQLConstants.Reserved.CONVERT;
import static org.teiid.language.SQLConstants.Reserved.LIMIT;
import static org.teiid.language.SQLConstants.Reserved.OFFSET;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.IS_ARRAY_TABLE;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.IS_TOP_TABLE;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.TRUE;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.FALSE;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.ColumnReference;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Function;
import org.teiid.language.Limit;
import org.teiid.language.NamedTable;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.RuntimeMetadata;

public class N1QLVisitor extends SQLStringVisitor{
    
    private RuntimeMetadata metadata;
    private CouchbaseExecutionFactory ef;
    
    private List<String> selectColumns = new ArrayList<>();
    private List<String> selectColumnReferences = new ArrayList<>();

    public N1QLVisitor(CouchbaseExecutionFactory ef, RuntimeMetadata metadata) {
        this.ef = ef;
        this.metadata = metadata;
    }

    @Override
    public void visit(DerivedColumn obj) {
        selectColumnReferences.add(obj.getAlias());
        super.visit(obj);
    }

    @Override
    public void visit(ColumnReference obj) {
        
        NamedTable groupTable = obj.getTable();
        if(groupTable != null) {
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
            
            //add selectColumns
            selectColumns.add(obj.getName());
        } else {
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
        } else if (functionName.equalsIgnoreCase(NonReserved.TRIM)){
            buffer.append(obj.getName()).append(Tokens.LPAREN);
            append(obj.getParameters());
            buffer.append(Tokens.RPAREN);
            return;
        } else if (this.ef.getFunctionModifiers().containsKey(functionName)) {
            List<?> parts =  this.ef.getFunctionModifiers().get(functionName).translate(obj);
            if (parts != null) {
                obj = (Function)parts.get(0);
            }
        } 
        super.visit(obj);
    }

    @Override
    public void visit(Limit limit) {
        if(limit.getRowOffset() > 0) {
            buffer.append(LIMIT).append(Tokens.SPACE);
            buffer.append(limit.getRowLimit()).append(Tokens.SPACE);
            buffer.append(OFFSET).append(Tokens.SPACE);
            buffer.append(limit.getRowOffset());
        } else {
            super.visit(limit);
        }
    }

    public List<String> getSelectColumns() {
        return selectColumns;
    }

    public List<String> getSelectColumnReferences() {
        return selectColumnReferences;
    }
}
