/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.translator.couchbase;

import static org.teiid.language.SQLConstants.Reserved.*;
import static org.teiid.language.SQLConstants.Tokens.*;
import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.*;
import static org.teiid.translator.couchbase.CouchbaseProperties.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.core.util.StringUtil;
import org.teiid.language.*;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.visitor.CollectorVisitor;
import org.teiid.language.visitor.SQLStringVisitor;

public class N1QLVisitor extends SQLStringVisitor{

    protected CouchbaseExecutionFactory ef;

    private List<String> selectColumns = new ArrayList<>();
    private Map<String, CBColumn> columnMap = new HashMap<>();

    private AliasGenerator columnAliasGenerator;
    private AliasGenerator tableAliasGenerator;

    protected boolean isArrayTable = false;
    private List<CBColumn> letStack = new ArrayList<>();

    protected String typedName = null;
    protected String typedValue = null;

    private String topTableAlias;

    private int placeHolderIndex = 1;

    public N1QLVisitor(CouchbaseExecutionFactory ef) {
        this.ef = ef;
    }

    @Override
    protected void appendSetQuery(SetQuery parent, QueryExpression obj,
            boolean right) {
        if (right) {
            //because we hold a lot of state, it's easier to construct a new visitor for the right
            //if join is supported we need different behavior
            N1QLVisitor rightVisitor = new N1QLVisitor(this.ef);
            rightVisitor.append(obj);
            if(shouldNestSetChild(parent, obj, right)) {
                buffer.append(Tokens.LPAREN);
                buffer.append(rightVisitor.toString());
                buffer.append(Tokens.RPAREN);
            } else {
                buffer.append(rightVisitor.toString());
            }
            return;
        }
        super.appendSetQuery(parent, obj, right);
    }

    private void appendQueryExpressionEnd(QueryExpression obj) {
        if (obj.getOrderBy() != null) {
            buffer.append(Tokens.SPACE);
            append(obj.getOrderBy());
        }

        if (obj.getLimit() != null) {
            buffer.append(Tokens.SPACE);
            append(obj.getLimit());
        }
    }

    @Override
    public void visit(Select obj) {
        Collection<ColumnReference> elements = CollectorVisitor.collectElements(obj);
        defineColumns(elements);

        //do the from first as that sets values on the CBColumns for later
        if (obj.getFrom() != null && !obj.getFrom().isEmpty()) {
            buffer.append(Tokens.SPACE).append(FROM).append(Tokens.SPACE);
            append(obj.getFrom());
        }
        String from = buffer.toString();
        buffer.setLength(0);

        buffer.append(SELECT).append(Tokens.SPACE);
        if (obj.isDistinct()) {
            buffer.append(DISTINCT).append(Tokens.SPACE);
        }

        append(obj.getDerivedColumns());

        buffer.append(from);

        appendLet();

        appendWhere(obj);

        if (obj.getGroupBy() != null) {
            buffer.append(Tokens.SPACE);
            append(obj.getGroupBy());
        }

        if (obj.getHaving() != null) {
            buffer.append(Tokens.SPACE).append(HAVING).append(Tokens.SPACE);
            append(obj.getHaving());
        }

        appendQueryExpressionEnd(obj);
    }

    private boolean appendLet() {
        boolean comma = false;
        for(int i = 0 ; i < letStack.size() ; i++) {
            CBColumn cbColumn = letStack.get(i);
            if (!cbColumn.isIdx()) {
                continue;
            }
            if (comma) {
                buffer.append(COMMA).append(SPACE);
            } else {
                buffer.append(SPACE).append(LET).append(SPACE);
                comma = true;
            }
            buffer.append(nameInSource(cbColumn.getNameReference()));
            buffer.append(SPACE).append(EQ).append(SPACE);
            buffer.append(cbColumn.getValueReference());
        }
        return comma;
    }

    private void defineColumns(Collection<ColumnReference> elements) {
        for (ColumnReference obj : elements) {
            if(this.columnMap.get(obj.getName()) != null) {
                continue;
            }
            if (obj.getTable() == null) {
                //this is a reference to a select alias
                continue;
            }
            CBColumn column = formCBColumn(obj);

            this.letStack.add(column);
            this.columnMap.put(obj.getName(), column);
        }
    }

    @Override
    public void visit(AndOr obj) {
        appendNestedCondition(obj, obj.getLeftCondition());
        buffer.append(Tokens.SPACE).append(obj.getOperator().toString()).append(Tokens.SPACE);
        appendNestedCondition(obj, obj.getRightCondition());
    }

    private void appendWhere(Select obj) {

        if(this.typedName != null && this.typedValue != null) {
            String typedWhere = null;

            for(CBColumn column : this.letStack) {
                if(column.getLeafName().equals(trimWave(this.typedName))) {
                    typedWhere = buildTypedWhere(nameInSource(column.isIdx()?column.getNameReference():column.getValueReference()), this.typedValue);
                    break;
                }
            }

            if (typedWhere == null) {
                String keyspace = nameInSource(this.topTableAlias);
                if(this.letStack.size() > 0) {
                    keyspace = nameInSource(this.letStack.get(this.letStack.size() - 1).getTableAlias());
                }
                typedWhere = keyspace + SOURCE_SEPARATOR + buildTypedWhere(this.typedName, this.typedValue);
            }
            buffer.append(SPACE).append(WHERE).append(SPACE);
            if(obj.getWhere() != null) {
                append(obj.getWhere());
                //TODO: detect duplicates / conflicts
                buffer.append(SPACE).append(Reserved.AND).append(SPACE);
            }
            buffer.append(typedWhere);
        } else if(obj.getWhere() != null) {
            buffer.append(SPACE).append(WHERE).append(SPACE);
            append(obj.getWhere());
        }

    }

    @Override
    public void visit(NamedTable obj) {

        retrieveTableProperty(obj);

        String tableNameInSource = obj.getMetadataObject().getSourceName();
        String alias = getTableAliasGenerator().generate();
        this.topTableAlias = alias;
        if(this.isArrayTable) {
            String baseName = tableNameInSource;
            String newAlias;
            for(int i = this.letStack.size() ; i > 0 ; i --) {

                CBColumn column = this.letStack.get(i -1);
                StringBuilder letValueReference = new StringBuilder();

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
                    unnestBuilder.append(nameInSource(newAlias));
                    if(!baseName.endsWith(SQUARE_BRACKETS)) { // the dim 1 array has a attribute name under keyspace
                        String dimArrayAttrName = baseName.substring(baseName.lastIndexOf(SOURCE_SEPARATOR) + 1, baseName.length());
                        unnestBuilder.append(SOURCE_SEPARATOR).append(dimArrayAttrName);
                    }
                    unnestBuilder.append(SPACE).append(nameInSource(alias));
                    column.setUnnest(unnestBuilder.toString());
                    alias = newAlias ;
                    continue;
                }

                letValueReference.append(nameInSource(alias));
                if(column.hasLeaf()) {
                    letValueReference.append(SOURCE_SEPARATOR).append(nameInSource(column.getLeafName()));
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
                StringBuilder letValueReference = new StringBuilder();

                if(column.isPK()){
                    buildMeta(alias);
                    letValueReference.append(buildMeta(alias));
                    column.setValueReference(letValueReference.toString());
                    column.setTableAlias(alias);
                    continue;
                }

                letValueReference.append(nameInSource(alias));
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
    public void visit(DerivedColumn obj) {
        String name = obj.getAlias();
        if (name == null) {
            name = "c" + placeHolderIndex++; //$NON-NLS-1$
        }

        append(obj.getExpression());
        buffer.append(SPACE).append(name);
        selectColumns.add(name);
    }

    @Override
    public void visit(ColumnReference obj) {
        if(obj.getTable() != null) {
            CBColumn column = this.columnMap.get(obj.getName());
            String aliasName = column.isIdx()?column.getNameReference():column.getValueReference();
            buffer.append(nameInSource(aliasName));
        } else {
            buffer.append(obj.getName());
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
            String nameInSource = obj.getMetadataObject().getSourceName();
            leafName = nameInSource.substring(nameInSource.lastIndexOf(SOURCE_SEPARATOR) + 1, nameInSource.length());
            leafName = trimWave(leafName);
        }

        String colExpr = this.getColumnAliasGenerator().generate() + UNDERSCORE + obj.getName();

        return new CBColumn(isPK, isIdx, colExpr, leafName, obj.getMetadataObject().getSourceName());
    }

    protected void retrieveTableProperty(NamedTable table) {

        if(!isArrayTable && table.getMetadataObject().getProperty(IS_ARRAY_TABLE, false) != null && table.getMetadataObject().getProperty(IS_ARRAY_TABLE, false).equals(TRUE_VALUE)) {
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
        return obj.getMetadataObject().getNameInSource() == null && StringUtil.endsWithIgnoreCase(obj.getName(), IDX_SUFFIX);
    }

    protected boolean isPKColumn(ColumnReference obj) {
        return obj.getMetadataObject().getNameInSource() == null && obj.getName().equalsIgnoreCase(DOCUMENTID);
    }

    @Override
    public void visit(Function obj) {

        String functionName = obj.getName();
        if (functionName.equalsIgnoreCase(NonReserved.TRIM)){
            buffer.append(obj.getName()).append(LPAREN);
            append(obj.getParameters());
            buffer.append(RPAREN);
            return;
        }
        if (this.ef.getFunctionModifiers().containsKey(functionName)) {
            List<?> parts =  this.ef.getFunctionModifiers().get(functionName).translate(obj);
            if (parts != null) {
                for (Object part : parts) {
                    if (part instanceof LanguageObject) {
                        append((LanguageObject)part);
                    } else {
                        buffer.append(part);
                    }
                }
                return;
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

    protected AliasGenerator getColumnAliasGenerator() {
        if(this.columnAliasGenerator == null) {
            this.columnAliasGenerator = new AliasGenerator(N1QL_COLUMN_ALIAS_PREFIX);
        }
        return columnAliasGenerator;
    }

    protected AliasGenerator getTableAliasGenerator() {
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

    protected static class CBColumn {

        private boolean isPK;
        private boolean isIdx;
        private String nameReference;
        private String leafName;
        private String nameInSource;
        private String valueReference;
        private String unnest;
        private String tableAlias;

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

    }
}
