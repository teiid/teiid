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

import static org.teiid.language.SQLConstants.Reserved.*;
import static org.teiid.language.SQLConstants.Tokens.*;
import static org.teiid.translator.couchbase.CouchbaseProperties.*;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.core.util.StringUtil;
import org.teiid.language.*;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.visitor.SQLStringVisitor;

public class N1QLVisitor extends SQLStringVisitor{
    
    protected CouchbaseExecutionFactory ef;
    
    private boolean recordColumnName = true;
    private List<String> selectColumns = new ArrayList<>();
    private List<String> selectColumnReferences = new ArrayList<>();
    private Map<String, CBColumn> columnMap = new HashMap<>();
    
    private AliasGenerator columnAliasGenerator;
    private AliasGenerator tableAliasGenerator;
    
    protected boolean isArrayTable = false;
    private List<CBColumn> letStack = new ArrayList<>();
    private List<CBColumn> unrelatedStack = new ArrayList<>();
    
    private List<CBColumn> tmpStack = new ArrayList<>();
    
    protected String typedName = null;
    protected String typedValue = null;
    
    private boolean isUnrelatedColumns = false;
    

    public N1QLVisitor(CouchbaseExecutionFactory ef) {
        this.ef = ef;
    }
    
    @Override
    public void visit(Select obj) {
        
        buffer.append(SELECT).append(Tokens.SPACE);
        if (obj.isDistinct()) {
            buffer.append(DISTINCT).append(Tokens.SPACE);
        }
        
        append(obj.getDerivedColumns());
        
        if (obj.getFrom() != null && !obj.getFrom().isEmpty()) {
            buffer.append(Tokens.SPACE).append(FROM).append(Tokens.SPACE);      
            append(obj.getFrom());
        }
        
        appendLet(obj);
        
        appendWhere(obj);
            
        if (obj.getGroupBy() != null) {
            buffer.append(Tokens.SPACE);
            append(obj.getGroupBy());
        }
        
        if (obj.getHaving() != null) {
            buffer.append(Tokens.SPACE).append(HAVING).append(Tokens.SPACE);
            append(obj.getHaving());
        }
        
        if (obj.getOrderBy() != null) {
            buffer.append(Tokens.SPACE);
            append(obj.getOrderBy());
        }
        
        if (obj.getLimit() != null) {
            buffer.append(Tokens.SPACE);
            append(obj.getLimit());
        }
    }
    
    private void appendLet(Select obj) {

        if(this.letStack.size() > 0) {
            buffer.append(SPACE).append(LET).append(SPACE);
            boolean comma = false;
            for(int i = 0 ; i < this.letStack.size() ; i++) {
                if (comma) {
                    buffer.append(COMMA).append(SPACE);
                }
                comma = true;
                buffer.append(this.letStack.get(i).getValueReference());
            }
        }
        
        initUnrelatedColumns(obj);
        
        for(int i = 0 ; i < this.unrelatedStack.size() ; i ++) {
            CBColumn column = this.unrelatedStack.get(i);
            String nameReference = column.getNameReference();
            StringBuilder letValueReference = new StringBuilder();
            letValueReference.append(buildEQ(nameReference));
            
            if(column.isPK()) {
                letValueReference.append(buildMeta(column.getTableAlias()));
                column.setValueReference(letValueReference.toString());
            } else if (column.isIdx()) {
                //todo - handle unreleated column in idx conlumn
            } else {
                letValueReference.append(this.nameInSource(column.getTableAlias()));
                String nameInSource = column.getNameInSource();
                if(nameInSource != null) {
                    nameInSource = nameInSource.substring(nameInSource.indexOf(SOURCE_SEPARATOR) + 1, nameInSource.length());
                    letValueReference.append(SOURCE_SEPARATOR).append(nameInSource);
                }
                column.setValueReference(letValueReference.toString());
            }
        }
        
        boolean comma = this.letStack.size() > 0;
        for(int i = 0 ; i < this.unrelatedStack.size() ; i ++) {
            if (comma) {
                buffer.append(COMMA).append(SPACE);
            }
            buffer.append(this.unrelatedStack.get(i).getValueReference()); 
        }
    }

    private void initUnrelatedColumns(Select obj) {
        
        isUnrelatedColumns = true;
        
        if(obj.getWhere() != null) {
            append(obj.getWhere());
        }
        
        if (obj.getOrderBy() != null) {
            append(obj.getOrderBy());
        }
                
        isUnrelatedColumns = false;
        
    }

    @Override
    public void visit(AndOr obj) {
        appendNestedCondition(obj, obj.getLeftCondition());
        if(!isUnrelatedColumns) {
            buffer.append(Tokens.SPACE).append(obj.getOperator().toString()).append(Tokens.SPACE);
        }
        appendNestedCondition(obj, obj.getRightCondition());
    }

    private void appendWhere(Select obj) {
        
        this.tmpStack.clear();
        
        if(this.typedName != null && this.typedValue != null) {
            
            boolean isTypedNameInLetStack = false;
            List<CBColumn> typedColumn = new ArrayList<>();
            for(CBColumn column : this.letStack) {
                if(column.hasTypedWhere() && column.getLeafName().equals(trimWave(this.typedName))) {
                    typedColumn.add(column);
                    isTypedNameInLetStack = true;
                }
            }
            
            if(isTypedNameInLetStack) {
                if(obj.getWhere() != null) {
                    buffer.append(SPACE).append(WHERE).append(SPACE);
                    append(obj.getWhere());
                    if(!isDuplicatedTypeColumn(typedColumn)) {
                        appendTypedWhere(false, typedColumn);
                    }
                } else {
                    if(!isDuplicatedTypeColumn(typedColumn)) {
                        buffer.append(SPACE).append(WHERE).append(SPACE);
                        appendTypedWhere(true, typedColumn);
                    }
                }
            } else {
                String keyspace = this.nameInSource(this.letStack.get(this.letStack.size() - 1).getTableAlias());
                String unrelatedType = keyspace + SOURCE_SEPARATOR + buildTypedWhere(this.typedName, this.typedValue);
                if(obj.getWhere() != null) {
                    buffer.append(SPACE).append(WHERE).append(SPACE);
                    append(obj.getWhere());
                    if(!isDuplicatedTypeColumn(this.typedName)){
                        buffer.append(SPACE).append(Reserved.AND).append(SPACE).append(unrelatedType);
                    }
                } else {
                    if(!isDuplicatedTypeColumn(this.typedName)) {
                        buffer.append(SPACE).append(WHERE).append(SPACE);
                        buffer.append(unrelatedType);
                    }
                }
            }
            
        } else {
            if(obj.getWhere() != null) {
                buffer.append(SPACE).append(WHERE).append(SPACE);
                append(obj.getWhere());
            }
        }
        
    }

    private void appendTypedWhere(boolean and, List<CBColumn> typedColumn) {
        for(CBColumn column : typedColumn) {
            if(column.hasTypedWhere()) {
                if(and) {
                    buffer.append(column.getTypedWhere());
                    and = false;
                } else {
                    buffer.append(SPACE).append(Reserved.AND).append(SPACE).append(column.getTypedWhere());
                }
            }
        }
    }
    
    private boolean isDuplicatedTypeColumn(List<CBColumn> typedColumn) {
        boolean result = false;
        for(CBColumn column : typedColumn) {
            if(isDuplicatedTypeColumn(column)) {
                result = true;
                break;
            }
        } 
        return result;
    }

    private boolean isDuplicatedTypeColumn(CBColumn typed) {
        if(typed.isPK() || typed.isIdx()) {
            return false;
        }
        boolean result = false;
        for(CBColumn column : this.tmpStack) {
            if(column.isPK() || column.isIdx()) {
                continue;
            }
            if(column.getNameInSource().equals(typed.getNameInSource())) {
                result = true;
                break;
            }
        }
        return result;
    }
    
    private boolean isDuplicatedTypeColumn(String typedName) {
        boolean result = false;
        for(CBColumn column : this.tmpStack) {
            if(column.getLeafName().equals(this.trimWave(typedName))) {
                result = true;
                break;
            }
        }
        return result;
    }

    @Override
    public void visit(NamedTable obj) {
        
        String tableNameInSource = obj.getMetadataObject().getNameInSource();
        String alias = getTableAliasGenerator().generate();
        if(this.isArrayTable) {
            String baseName = tableNameInSource;
            String newAlias;
            for(int i = this.letStack.size() ; i > 0 ; i --) {
                
                CBColumn column = this.letStack.get(i -1);
                String nameReference = column.getNameReference();
                StringBuilder letValueReference = new StringBuilder();
                letValueReference.append(buildEQ(nameReference));
                
                if(column.isPK()) {
                    letValueReference.append(buildMeta(alias));
                    column.setValueReference(letValueReference.toString());
                    column.setTableAlias(alias);
                    continue;
                } else if (column.isIdx()) {
                    letValueReference.append(UNNEST_POSITION);
                    letValueReference.append(LPAREN).append(nameInSource(alias)).append(RPAREN);
                    column.setValueReference(letValueReference.toString());
                    
                    newAlias = tableAliasGenerator.generate();
                    baseName = baseName.substring(0, baseName.length() - SQUARE_BRACKETS.length());
                    StringBuilder unnestBuilder = new StringBuilder();
                    unnestBuilder.append(UNNEST).append(SPACE);
                    unnestBuilder.append(this.nameInSource(newAlias));
                    if(!baseName.endsWith(SQUARE_BRACKETS)) { // the dim 1 array has a attribute name under keyspace
                        String dimArrayAttrName = baseName.substring(baseName.lastIndexOf(SOURCE_SEPARATOR) + 1, baseName.length());
                        unnestBuilder.append(SOURCE_SEPARATOR).append(dimArrayAttrName);
                    }
                    unnestBuilder.append(SPACE).append(this.nameInSource(alias));
                    column.setUnnest(unnestBuilder.toString());
                    alias = newAlias ;
                    continue;
                }
                
                letValueReference.append(this.nameInSource(alias));
                if(column.hasLeaf()) {
                    letValueReference.append(SOURCE_SEPARATOR).append(this.nameInSource(column.getLeafName()));
                }
                column.setValueReference(letValueReference.toString());
                column.setTableAlias(alias);
            }
            String keyspace = baseName.substring(0, baseName.indexOf(SOURCE_SEPARATOR));
            buffer.append(keyspace);
            buffer.append(SPACE);
            buffer.append(nameInSource(alias));
            
            for(int i = 0 ; i < this.letStack.size() ; i++) {
                CBColumn column = this.letStack.get(i);
                if(column.hasUnnest()) {
                    buffer.append(SPACE);
                    buffer.append(column.getUnnest());
                }
            }
            
        } else {  
            for(int i = this.letStack.size() ; i > 0 ; i --) {
                CBColumn column = this.letStack.get(i -1);
                String nameReference = column.getNameReference();
                StringBuilder letValueReference = new StringBuilder();
                letValueReference.append(buildEQ(nameReference));
                
                if(column.isPK()){
                    buildMeta(alias);
                    letValueReference.append(buildMeta(alias));
                    column.setValueReference(letValueReference.toString());
                    column.setTableAlias(alias);
                    continue;
                }
                
                letValueReference.append(this.nameInSource(alias));
                String nameInSource = column.getNameInSource();
                if(nameInSource != null) {
                    nameInSource = nameInSource.substring(nameInSource.indexOf(SOURCE_SEPARATOR) + 1, nameInSource.length());
                    letValueReference.append(SOURCE_SEPARATOR).append(nameInSource);
                }
                column.setValueReference(letValueReference.toString());
                column.setTableAlias(alias);
            }

            buffer.append(tableNameInSource); // if a table not array table, the table name in source is keyspace name
            buffer.append(SPACE);
            buffer.append(nameInSource(alias));
        }
    }

    private String buildEQ(String nameReference) {
        StringBuilder sb = new StringBuilder();
        sb.append(this.nameInSource(nameReference));
        sb.append(SPACE).append(EQ).append(SPACE);
        return sb.toString();
    }
    
    private String buildTypedWhere(String typedName, String typedValue) {
        StringBuilder sb = new StringBuilder();
        sb.append(typedName).append(SPACE).append(EQ).append(SPACE);
        sb.append(typedValue);
        return sb.toString();
    }
    
    protected String buildMeta(String alias) {
        StringBuilder sb = new StringBuilder();
        sb.append("META").append(LPAREN).append(nameInSource(alias)).append(RPAREN).append(".id"); //$NON-NLS-1$ //$NON-NLS-2$
        return sb.toString();
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
        if(!isUnrelatedColumns) {
            buffer.append(ORDER).append(Tokens.SPACE).append(BY).append(Tokens.SPACE);
        }
        append(obj.getSortSpecifications());
        recordColumnName = true;
    }
    
    @Override
    public void visit(Comparison obj) {
        recordColumnName = false;
        append(obj.getLeftExpression());
        if(!isUnrelatedColumns) {
            buffer.append(Tokens.SPACE);
            buffer.append(obj.getOperator());
            buffer.append(Tokens.SPACE);
            appendRightComparison(obj);
        }
        recordColumnName = true;
    }

    @Override
    public void visit(DerivedColumn obj) {
        if(recordColumnName) {
            selectColumnReferences.add(obj.getAlias());
        }
        append(obj.getExpression());
    }

    @Override
    public void visit(ColumnReference obj) {
        
        if(obj.getTable() != null) {
            
            if(isUnrelatedColumns  && this.columnMap.get(obj.getName()) != null) {
                return;
            }
            
            if(!isUnrelatedColumns && !recordColumnName && this.columnMap.get(obj.getName()) != null) {
                String aliasName = this.columnMap.get(obj.getName()).getNameReference();
                buffer.append(this.nameInSource(aliasName));
                this.tmpStack.add(this.columnMap.get(obj.getName()));
                return;
            } 
            
            retrieveTableProperty(obj.getTable());

            CBColumn column = formCBColumn(obj);
            
            if(this.typedName != null && this.typedValue != null && column.getLeafName().equals(trimWave(this.typedName))) {
                String typedWhere = buildTypedWhere(nameInSource(column.getNameReference()), this.typedValue);
                column.setTypedWhere(typedWhere);
            }
            
            if(recordColumnName) {
                this.letStack.add(column);
                this.columnMap.put(obj.getName(), column);
                this.selectColumns.add(column.getNameReference());
                buffer.append(this.nameInSource(column.getNameReference()));
            } else if(isUnrelatedColumns && !recordColumnName && letStack.size() > 0){
                String tableAlias = letStack.get(letStack.size() -1).getTableAlias();
                column.setTableAlias(tableAlias);
                this.unrelatedStack.add(column);
                this.columnMap.put(obj.getName(), column);
            }
        } else {
            super.visit(obj);
        }
    }

    protected CBColumn formCBColumn (ColumnReference obj) {
        boolean isPK = false;
        boolean isIdx = false;
        String leafName = ""; //$NON-NLS-1$
        
        if(isPKColumn(obj)) {
            isPK = true;
        } else if(isIDXColumn(obj)) {
            isIdx = true;
        } else if(obj.getMetadataObject().getNameInSource() != null && !obj.getMetadataObject().getNameInSource().endsWith(SQUARE_BRACKETS)){
            String nameInSource = obj.getMetadataObject().getNameInSource();
            leafName = nameInSource.substring(nameInSource.lastIndexOf(SOURCE_SEPARATOR) + 1, nameInSource.length());
            leafName = this.trimWave(leafName);
        }
        
        String colExpr = this.getColumnAliasGenerator().generate() + UNDERSCORE + obj.getName();

        return new CBColumn(isPK, isIdx, colExpr, leafName, obj.getMetadataObject().getSourceName());
    }
    
    protected void retrieveTableProperty(NamedTable table) {
        
        if(table == null) {
            return;
        }
        
        if(!isArrayTable && table.getMetadataObject().getProperty(IS_ARRAY_TABLE, false).equals(TRUE_VALUE)) {
            this.isArrayTable = true;
        }

        if(this.typedName == null && this.typedValue == null) {
            String typedNamePair = table.getMetadataObject().getProperty(NAMED_TYPE_PAIR, false);
            if(typedNamePair != null && typedNamePair.length() > 0) {
                String[] pair = typedNamePair.split(COLON);
                this.typedName = pair[0];
                this.typedValue = pair[1];
            }
        }
    }

    private boolean isIDXColumn(ColumnReference obj) {
        return obj.getName().endsWith(IDX_SUFFIX) && obj.getMetadataObject().getNameInSource() == null;
    }

    private boolean isPKColumn(ColumnReference obj) {
        return obj.getName().equals(DOCUMENTID) && obj.getMetadataObject().getNameInSource() == null;
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
            buffer.append(obj.getName()).append(LPAREN);
            append(obj.getParameters());
            buffer.append(RPAREN);
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
            buffer.append(LIMIT).append(SPACE);
            buffer.append(limit.getRowLimit()).append(SPACE);
            buffer.append(OFFSET).append(SPACE);
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

    public AliasGenerator getColumnAliasGenerator() {
        if(this.columnAliasGenerator == null) {
            this.columnAliasGenerator = new AliasGenerator(N1QL_COLUMN_ALIAS_PREFIX);
        }
        return columnAliasGenerator;
    }

    public AliasGenerator getTableAliasGenerator() {
        if(this.tableAliasGenerator == null) {
            this.tableAliasGenerator = new AliasGenerator(N1QL_TABLE_ALIAS_PREFIX);
        }
        return tableAliasGenerator;
    }

    @Override
    public void visit(Call call) {
        
        String procName = call.getProcedureName();
        String keyspace = null;
        if(procName.equalsIgnoreCase(GETDOCUMENTS) || procName.equalsIgnoreCase(GETDOCUMENT)) {
            keyspace = (String) call.getArguments().get(1).getArgumentValue().getValue();
        } 
        
        if(call.getProcedureName().equalsIgnoreCase(GETDOCUMENTS)) {
            appendKeyspace(keyspace);
            appendN1QLWhere(call);
            return;
        } else if(call.getProcedureName().equalsIgnoreCase(GETDOCUMENT)) {
            appendKeyspace(keyspace);
            appendN1QLPK(call);
            return;
        } 
    }
    
    @Override
    protected String escapeString(String str, String quote) {
        return StringUtil.replaceAll(str, quote, "\\u0027"); //$NON-NLS-1$
    }

    private void appendKeyspace(String keyspace) {
        buffer.append(SELECT).append(SPACE);
        buffer.append(RESULT).append(SPACE); 
        buffer.append(Reserved.FROM).append(SPACE);
        buffer.append(nameInSource(keyspace)).append(SPACE);
        buffer.append(Reserved.AS).append(SPACE).append(RESULT).append(SPACE);
    }
    
    private void appendN1QLWhere(Call call) {
        buffer.append(Reserved.WHERE).append(SPACE);
        buffer.append("META").append(LPAREN).append(RPAREN).append(".id").append(SPACE); //$NON-NLS-1$ //$NON-NLS-2$
        buffer.append(Reserved.LIKE).append(SPACE);
        append(call.getArguments().get(0));
    }
    
    private void appendN1QLPK(Call call) {
        buffer.append("USE PRIMARY KEYS").append(SPACE); //$NON-NLS-1$
        append(call.getArguments().get(0));
    }
    
    protected String nameInSource(String path) {
        if(path.startsWith(WAVE) && path.endsWith(WAVE)) {
            return path;
        }
        return WAVE + path + WAVE; 
    }
    
    protected String trimWave(String value) {
        String results = value;
        if(results.startsWith(WAVE)) {
            results = results.substring(1);
        }
        if(results.endsWith(WAVE)) {
            results = results.substring(0, results.length() - 1);
        }
        return results;
    }
    
    private class AliasGenerator {
        
        private final String prefix;
        
        private Integer aliasCounter;
        
        AliasGenerator(String prefix) {
            this.prefix = prefix;
            this.aliasCounter = Integer.valueOf(1);
        }
        
        public String generate() {  
            int index = this.aliasCounter.intValue(); 
            String alias = this.prefix + index;
            this.aliasCounter = Integer.valueOf(this.aliasCounter.intValue() + 1);
            return alias;
        }
    }
    
    protected class CBColumn {
        
        private boolean isPK;
        private boolean isIdx;
        private String nameReference;
        private String leafName;
        private String nameInSource;
        private String valueReference;
        private String unnest;
        private String tableAlias;
        private String typedWhere;

        public CBColumn(boolean isPK, boolean isIdx, String nameReference, String leafName, String nameInSource) {
            this.isPK = isPK;
            this.isIdx = isIdx;
            this.nameReference = nameReference;
            this.leafName = leafName;
            this.nameInSource = nameInSource;
        }

        public boolean isPK() {
            return isPK;
        }

        public boolean isIdx() {
            return isIdx;
        }

        public String getNameReference() {
            return nameReference;
        }
        
        public boolean hasLeaf() {
            return this.leafName != null && this.leafName.length() > 0;
        }

        public String getLeafName() {
            return leafName;
        }

        public String getNameInSource() {
            return nameInSource;
        }

        public String getValueReference() {
            return valueReference;
        }

        public void setValueReference(String valueReference) {
            this.valueReference = valueReference;
        }
        
        boolean hasUnnest() {
            return this.unnest != null && this.unnest.length() > 0 ;
        }

        public String getUnnest() {
            return unnest;
        }

        public void setUnnest(String unnest) {
            this.unnest = unnest;
        }
        
        public String getTableAlias() {
            return tableAlias;
        }

        public void setTableAlias(String tableAlias) {
            this.tableAlias = tableAlias;
        }

        public boolean hasTypedWhere() {
            return this.typedWhere != null && this.typedWhere.length() > 0;
        }

        public String getTypedWhere() {
            return typedWhere;
        }

        public void setTypedWhere(String typedWhere) {
            this.typedWhere = typedWhere;
        }
    }
}
