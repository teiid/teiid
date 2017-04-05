/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.translator.couchbase;

import static org.teiid.language.SQLConstants.Reserved.CAST;
import static org.teiid.language.SQLConstants.Reserved.CONVERT;
import static org.teiid.language.SQLConstants.Reserved.LIMIT;
import static org.teiid.language.SQLConstants.Reserved.OFFSET;
import static org.teiid.language.SQLConstants.Reserved.SELECT;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.GETTEXTDOCUMENTS;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.GETDOCUMENTS;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.GETTEXTDOCUMENT;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.GETDOCUMENT;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.SAVEDOCUMENT;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.DELETEDOCUMENT;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.GETTEXTMETADATADOCUMENT;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.GETMETADATADOCUMENT;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.ID;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.RESULT;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.IS_ARRAY_TABLE;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.IS_TOP_TABLE;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.ARRAY_TABLE_GROUP;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.TRUE;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.FALSE;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.DOCUMENT_ID;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.buildNameInSource;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Function;
import org.teiid.language.GroupBy;
import org.teiid.language.LanguageObject;
import org.teiid.language.Limit;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.OrderBy;
import org.teiid.language.Argument.Direction;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.visitor.SQLStringVisitor;

public class N1QLVisitor extends SQLStringVisitor{
    
    private CouchbaseExecutionFactory ef;
    
    private boolean recordColumnName = true;
    private boolean isNestedArrayColumns = false;
    private List<String> selectColumns = new ArrayList<>();
    private List<String> selectColumnReferences = new ArrayList<>();

    public N1QLVisitor(CouchbaseExecutionFactory ef) {
        this.ef = ef;
    }

    @Override
    protected void append(List<? extends LanguageObject> items) {
        
        if (items != null && items.size() != 0) {
            append(items.get(0));
            for (int i = 1; i < items.size(); i++) {
                if(!isNestedArrayColumns) {
                    buffer.append(Tokens.COMMA).append(Tokens.SPACE);
                }
                append(items.get(i));
            }
        }
        isNestedArrayColumns = false;
    }

    @Override
    public void visit(GroupBy obj) {
        recordColumnName = false;
        super.visit(obj);
        recordColumnName = true;
    }

    @Override
    public void visit(OrderBy obj) {
        recordColumnName = false;
        super.visit(obj);
        recordColumnName = true;
    }

    @Override
    public void visit(DerivedColumn obj) {
        if(recordColumnName) {
            selectColumnReferences.add(obj.getAlias());
        }
        super.visit(obj);
    }

    @Override
    public void visit(ColumnReference obj) {
        
        NamedTable groupTable = obj.getTable();
        if(groupTable != null) {
            String group = obj.getTable().getCorrelationName();
            String isArrayTable = obj.getTable().getMetadataObject().getProperty(IS_ARRAY_TABLE, false);
            String isTopTable = obj.getTable().getMetadataObject().getProperty(IS_TOP_TABLE, false);
            
            if(obj.getName().equals(DOCUMENT_ID)) {
                if(recordColumnName) {
                    buffer.append("META().id AS PK"); //$NON-NLS-1$ 
                    selectColumns.add("PK"); //$NON-NLS-1$ 
                } else {
                    buffer.append("META().id"); //$NON-NLS-1$ 
                }
                return;
            }
            
            if(isArrayTable.equals(TRUE) && !isNestedArrayColumns){
                if(group == null) {
                    group = obj.getTable().getMetadataObject().getProperty(ARRAY_TABLE_GROUP, false);
                }
                buffer.append(group);
                selectColumns.add(group);
                isNestedArrayColumns = true;
            } else if(isArrayTable.equals(FALSE) && isTopTable.equals(FALSE) && group == null) {
                shortNameOnly = true;
                super.visit(obj);
                shortNameOnly = false;
            } else if(isArrayTable.equals(FALSE)){
                super.visit(obj);
            }
            
            //add selectColumns
            if(recordColumnName){
                selectColumns.add(obj.getName());
            }
        } else {
            super.visit(obj);
        }
    }

    @Override
    public void visit(Comparison obj) {
        recordColumnName = false;
        super.visit(obj);
        recordColumnName = true;
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
        } else if(functionName.equalsIgnoreCase("METAID")) { //$NON-NLS-1$
            buffer.append("META").append(Tokens.LPAREN); //$NON-NLS-1$
            Literal literal = (Literal) obj.getParameters().get(0);
            String tableName = (String) literal.getValue();
            buffer.append(tableName);
            buffer.append(Tokens.RPAREN).append(".id"); //$NON-NLS-1$
            return;
        }else if (this.ef.getFunctionModifiers().containsKey(functionName)) {
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
    
    private String keySpace;

    public String getKeySpace() {
        return keySpace;
    }

    public void setKeySpace(String keySpace) {
        this.keySpace = keySpace;
    }

    @Override
    public void visit(Call call) {
                
        if(call.getProcedureName().equalsIgnoreCase(GETTEXTDOCUMENTS)) {
            appendClobN1QL();
            appendN1QLWhere(call);
            return;
        } else if(call.getProcedureName().equalsIgnoreCase(GETTEXTDOCUMENT)) {
            appendClobN1QL();
            appendN1QLPK(call);
            return;
        } else if(call.getProcedureName().equalsIgnoreCase(GETDOCUMENTS)) {
            appendBlobN1QL();
            appendN1QLWhere(call);
            return;
        } else if(call.getProcedureName().equalsIgnoreCase(GETDOCUMENT)) {
            appendBlobN1QL();
            appendN1QLPK(call);
            return;
        } else if(call.getProcedureName().equalsIgnoreCase(SAVEDOCUMENT)) {
            buffer.append("UPSERT INTO").append(Tokens.SPACE); //$NON-NLS-1$
            buffer.append(buildNameInSource(keySpace, null)).append(Tokens.SPACE);
            buffer.append(Reserved.AS).append(Tokens.SPACE);
            buffer.append(RESULT).append(Tokens.SPACE); 
            buffer.append("(KEY, VALUE) VALUES").append(Tokens.SPACE); //$NON-NLS-1$
            buffer.append(Tokens.LPAREN);
            final List<Argument> params = call.getArguments();
            for (int i = 0; i < params.size(); i++) {
                Argument param = params.get(i);
                if (param.getDirection() == Direction.IN ) {
                    if (i != 0) {
                        buffer.append(Tokens.COMMA).append(Tokens.SPACE);
                    }
                    append(param);
                }
            }
            buffer.append(Tokens.RPAREN).append(Tokens.SPACE);
            buffer.append("RETURNING").append(Tokens.SPACE); //$NON-NLS-1$
            buffer.append(RESULT);
            return;
        } else if(call.getProcedureName().equalsIgnoreCase(DELETEDOCUMENT)) {
            buffer.append(Reserved.DELETE).append(Tokens.SPACE);
            buffer.append(Reserved.FROM).append(Tokens.SPACE);
            buffer.append(buildNameInSource(keySpace, null)).append(Tokens.SPACE);
            buffer.append(Reserved.AS).append(Tokens.SPACE);
            buffer.append(RESULT).append(Tokens.SPACE); 
            appendN1QLPK(call);
            buffer.append(Tokens.SPACE);
            buffer.append("RETURNING").append(Tokens.SPACE); //$NON-NLS-1$
            buffer.append(RESULT);
            return;
        } else if(call.getProcedureName().equalsIgnoreCase(GETTEXTMETADATADOCUMENT) || call.getProcedureName().equalsIgnoreCase(GETMETADATADOCUMENT)) {
            buffer.append(SELECT).append(Tokens.SPACE);
            buffer.append("META").append(Tokens.LPAREN).append(Tokens.RPAREN).append(Tokens.SPACE); //$NON-NLS-1$
            buffer.append(Reserved.AS).append(Tokens.SPACE);
            buffer.append(RESULT).append(Tokens.SPACE);
            buffer.append(Reserved.FROM).append(Tokens.SPACE);
            buffer.append(buildNameInSource(keySpace, null));
            return;
        } 
    }

    private void appendClobN1QL() {
        buffer.append(SELECT).append(Tokens.SPACE);
        buffer.append("META").append(Tokens.LPAREN).append(Tokens.RPAREN); //$NON-NLS-1$
        buffer.append(".id").append(Tokens.SPACE);
        buffer.append(Reserved.AS).append(Tokens.SPACE).append(ID); //$NON-NLS-1$
        buffer.append(Tokens.COMMA).append(Tokens.SPACE); 
        buffer.append(RESULT).append(Tokens.SPACE); 
        buffer.append(Reserved.FROM).append(Tokens.SPACE);
        buffer.append(buildNameInSource(keySpace, null)).append(Tokens.SPACE);
        buffer.append(Reserved.AS).append(Tokens.SPACE).append(RESULT).append(Tokens.SPACE);
    }
    
    private void appendBlobN1QL() {
        buffer.append(SELECT).append(Tokens.SPACE);
        buffer.append(RESULT).append(Tokens.SPACE); 
        buffer.append(Reserved.FROM).append(Tokens.SPACE);
        buffer.append(buildNameInSource(keySpace, null)).append(Tokens.SPACE);
        buffer.append(Reserved.AS).append(Tokens.SPACE).append(RESULT).append(Tokens.SPACE);
    }
    
    private void appendN1QLWhere(Call call) {
        buffer.append(Reserved.WHERE).append(Tokens.SPACE);
        buffer.append("META").append(Tokens.LPAREN).append(Tokens.RPAREN); //$NON-NLS-1$
        buffer.append(".id").append(Tokens.SPACE);
        buffer.append(Reserved.LIKE).append(Tokens.SPACE);
        append(call.getArguments().get(0));
    }
    
    private void appendN1QLPK(Call call) {
        buffer.append("USE PRIMARY KEYS").append(Tokens.SPACE);
        append(call.getArguments().get(0));
    }
}
