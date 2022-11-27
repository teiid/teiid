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
package org.teiid.translator.infinispan.hotrod;

import static org.teiid.language.SQLConstants.Reserved.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.infinispan.api.InfinispanPlugin;
import org.teiid.infinispan.api.MarshallerBuilder;
import org.teiid.infinispan.api.ProtobufMetadataProcessor;
import org.teiid.language.*;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.Join.JoinType;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.document.DocumentNode;

public class IckleConversionVisitor extends SQLStringVisitor {
    protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
    protected RuntimeMetadata metadata;
    protected List<Expression> projectedExpressions = new ArrayList<>();
    protected NamedTable parentTable;
    protected NamedTable queriedTable;
    private Integer rowLimit;
    private Integer rowOffset;
    private boolean includePK;
    protected boolean avoidProjection = false;
    private DocumentNode rootNode;
    private DocumentNode joinedNode;
    private LinkedHashMap<String, Class<?>> projectedDocumentAttributes = new LinkedHashMap<>();
    private AtomicInteger aliasCounter = new AtomicInteger();
    protected boolean nested;

    public IckleConversionVisitor(RuntimeMetadata metadata, boolean includePK) {
        this.metadata = metadata;
        this.includePK = includePK;
        this.shortNameOnly = true;
    }

    public Table getParentTable() {
        return parentTable.getMetadataObject();
    }

    public NamedTable getParentNamedTable() {
        return parentTable;
    }

    public Table getQueryTable() {
        return this.queriedTable.getMetadataObject();
    }

    public NamedTable getQueryNamedTable() {
        return this.queriedTable;
    }

    public boolean isNestedOperation() {
        return this.nested;
    }

    @Override
    public void visit(NamedTable obj) {
        this.queriedTable = obj;
        if (obj.getCorrelationName() == null) {
            obj.setCorrelationName(obj.getMetadataObject().getName().toLowerCase()+"_"+aliasCounter.getAndIncrement());
        }

        if (this.rootNode == null) {
            String messageName = null;
            String aliasName = null;
            String mergedTableName = ProtobufMetadataProcessor.getMerge(obj.getMetadataObject());
            if (mergedTableName == null) {
                aliasName = obj.getCorrelationName();
                messageName = getMessageName(obj.getMetadataObject());
                this.parentTable = obj;
                this.rootNode = new DocumentNode(obj.getMetadataObject(), true);
                this.joinedNode = this.rootNode;

                // check to see if there is one-2-one rows
                Set<String> tags = new HashSet<>();
                for (Column column:obj.getMetadataObject().getColumns()) {
                    if (ProtobufMetadataProcessor.getParentTag(column) != -1) {
                        String childMessageName = ProtobufMetadataProcessor.getMessageName(column);
                        if (!tags.contains(childMessageName)) {
                          tags.add(childMessageName);
                          //TODO: DocumentNode needs to be refactored to just take name, not table
                          Table t = new Table();
                          t.setName(childMessageName);
                          this.joinedNode = this.rootNode.joinWith(JoinType.INNER_JOIN,
                                    new DocumentNode(t, false));
                        }
                    }
                }

            } else {
                try {
                    Table mergedTable = this.metadata.getTable(mergedTableName);
                    messageName = getMessageName(mergedTable);
                    aliasName = mergedTable.getName().toLowerCase()+"_"+aliasCounter.getAndIncrement();
                    this.parentTable = new NamedTable(mergedTable.getName(), aliasName, mergedTable);
                    this.rootNode = new DocumentNode(mergedTable, true);
                    this.joinedNode = this.rootNode.joinWith(JoinType.INNER_JOIN,
                            new DocumentNode(obj.getMetadataObject(), true));
                    this.nested = true;
                } catch (TranslatorException e) {
                    this.exceptions.add(e);
                }
            }

            buffer.append(messageName);
            if (aliasName != null) {
                buffer.append(Tokens.SPACE);
                buffer.append(aliasName);
            }

            if (this.includePK) {
                KeyRecord pk = this.parentTable.getMetadataObject().getPrimaryKey();
                if (pk != null) {
                    for (Column column : pk.getColumns()) {
                        projectedExpressions.add(new ColumnReference(obj, column.getName(), column, column.getJavaType()));
                    }
                }
            }
        }
    }

    private String getMessageName(Table obj) {
        String messageName;
        messageName = ProtobufMetadataProcessor.getMessageName(obj);
        if (messageName == null) {
            messageName = obj.getName();
        }
        return messageName;
    }

    public boolean isPartOfPrimaryKey(String columnName) {
        KeyRecord pk = getParentTable().getPrimaryKey();
        if (pk != null) {
            for (Column column:pk.getColumns()) {
                if (column.getName().equals(columnName)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void visit(Join obj) {
        Condition cond = null;
        if (obj.getLeftItem() instanceof Join) {
            cond = obj.getCondition();
            append(obj.getLeftItem());
            Table right = ((NamedTable)obj.getRightItem()).getMetadataObject();
            this.joinedNode.joinWith(obj.getJoinType(), new DocumentNode(right, true));
        }
        else if (obj.getRightItem() instanceof Join) {
            cond = obj.getCondition();
            append(obj.getRightItem());
            Table left = ((NamedTable)obj.getLeftItem()).getMetadataObject();
            this.joinedNode.joinWith(obj.getJoinType(), new DocumentNode(left, true));
        }
        else {
            cond = obj.getCondition();
            append(obj.getLeftItem());
            this.queriedTable = (NamedTable)obj.getRightItem();
            Table right = ((NamedTable)obj.getRightItem()).getMetadataObject();
            this.joinedNode.joinWith(obj.getJoinType(), new DocumentNode(right, true));
        }

        if (cond != null) {
            append(cond);
        }
    }

    @Override
    public void visit(Limit obj) {
        if (obj.getRowOffset() != 0) {
            this.rowOffset = new Integer(obj.getRowOffset());
        }
        if (obj.getRowLimit() != 0) {
            this.rowLimit = new Integer(obj.getRowLimit());
        }
    }


    @Override
    public void visit(Select obj) {
        buffer.append(SQLConstants.Reserved.FROM).append(Tokens.SPACE);
        visitNodes(obj.getFrom());

        if (obj.getWhere() != null) {
            buffer.append(Tokens.SPACE);
            buffer.append(SQLConstants.Reserved.WHERE).append(Tokens.SPACE);
            visitNode(obj.getWhere());
        }

        if (obj.getGroupBy() != null) {
            buffer.append(Tokens.SPACE);
            append(obj.getGroupBy());
        }

        if (obj.getHaving() != null) {
            buffer.append(Tokens.SPACE)
                  .append(HAVING)
                  .append(Tokens.SPACE);
            append(obj.getHaving());
        }

        if (obj.getOrderBy() != null) {
            buffer.append(Tokens.SPACE);
            visitNode(obj.getOrderBy());
            if (this.nested) {
                this.exceptions.add(new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25010)));
            }
        }

        if (obj.getLimit() != null) {
            buffer.append(Tokens.SPACE);
            visitNode(obj.getLimit());
        }

        visitNodes(obj.getDerivedColumns());
    }

    @Override
    public void visit(Comparison obj) {
        if (obj.getOperator() == Operator.EQ && obj.getLeftExpression() instanceof ColumnReference
                && obj.getRightExpression() instanceof ColumnReference) {
            // this typically is join.
            Column left = ((ColumnReference)obj.getLeftExpression()).getMetadataObject();
            Column right = ((ColumnReference)obj.getRightExpression()).getMetadataObject();
            if (getQualifiedName(left).equals(getQualifiedName(right))) {
                return;
            }
        }
        super.visit(obj);
    }

    @Override
    public void visit(ColumnReference obj) {
        buffer.append(getQualifiedName(obj.getMetadataObject()));
    }

    @Override
    public void visit(DerivedColumn obj) {
        if (obj.getExpression() instanceof ColumnReference) {
            Column column = ((ColumnReference)obj.getExpression()).getMetadataObject();
            if (!column.isSelectable()) {
                this.exceptions.add(new TranslatorException(InfinispanPlugin.Util
                        .gs(InfinispanPlugin.Event.TEIID25001, column.getName())));
            }
            try {
                column = normalizePseudoColumn(column, this.metadata);
            } catch (TranslatorException e1) {
                this.exceptions.add(e1);
            }
            if (!this.includePK || !isPartOfPrimaryKey(column.getName())) {
                if (column.getParent().equals(this.parentTable.getMetadataObject())){
                    this.projectedExpressions.add(new ColumnReference(this.parentTable, column.getName(), column, column.getJavaType()));
                } else {
                    this.projectedExpressions.add(new ColumnReference(this.queriedTable, column.getName(), column, column.getJavaType()));
                }
            }
            boolean nested = false;
            if (ProtobufMetadataProcessor.getParentTag(column) != -1
                    || ProtobufMetadataProcessor.getParentTag((Table) column.getParent()) != -1) {
                this.avoidProjection = true;
                nested = true;
            }
            try {
                this.projectedDocumentAttributes.put(
                        MarshallerBuilder.getDocumentAttributeName(column, nested, this.metadata),
                        column.getJavaType());
            } catch (TranslatorException e) {
                this.exceptions.add(e);
            }
        }
        else if (obj.getExpression() instanceof Function) {
            if (!this.parentTable.equals(this.queriedTable)) {
                this.exceptions.add(new TranslatorException(InfinispanPlugin.Event.TEIID25008,
                        InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25008)));
            }
            AggregateFunction func = (AggregateFunction)obj.getExpression();
            this.projectedExpressions.add(func);
            // Aggregate functions can not be part of the implicit query projection when the complex object is involved
            // thus not adding to projectedDocumentAttributes. i.e. sum(g2.g3.e1) is not supported by Infinispan AFAIK.
            this.projectedDocumentAttributes.put(obj.getAlias(), func.getType());
        }
        else {
            this.exceptions.add(new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25002, obj)));
        }
    }

    static Column normalizePseudoColumn(Column column, RuntimeMetadata metadata) throws TranslatorException {
        String pseudo = ProtobufMetadataProcessor.getPseudo(column);
        if (pseudo != null) {
            Table columnParent = (Table)column.getParent();
            Table pseudoColumnParent = metadata.getTable(
                    ProtobufMetadataProcessor.getMerge(columnParent));
            return pseudoColumnParent.getColumnByName(getRecordName(column));
        }
        return column;
    }

    public String getQuery() {
        StringBuilder sb = new StringBuilder();
        if (!this.avoidProjection) {
            addSelectedColumns(sb);
            sb.append(Tokens.SPACE);
        }
        sb.append(super.toString());
        return  sb.toString();
    }

    String getQualifiedName(Column column) {
        String aliasName = this.parentTable.getCorrelationName();
        String nis = getName(column);
        String parentName = ProtobufMetadataProcessor.getParentColumnName(column);
        if (parentName == null && !ProtobufMetadataProcessor.isPseudo(column)) {
            parentName = ProtobufMetadataProcessor.getParentColumnName((Table)column.getParent());
        }
        if (parentName != null) {
            nis = parentName + Tokens.DOT + nis;
        }
        if (aliasName != null) {
            return aliasName + Tokens.DOT + nis;
        }
        return nis;
    }

    StringBuilder addSelectedColumns(StringBuilder sb) {
        sb.append(SQLConstants.Reserved.SELECT).append(Tokens.SPACE);

        boolean first = true;
        for (Expression expr : this.projectedExpressions) {
            if (!first) {
                sb.append(Tokens.COMMA).append(Tokens.SPACE);
            }
            if (expr instanceof ColumnReference) {
                Column column = ((ColumnReference) expr).getMetadataObject();
                String nis = getQualifiedName(column);
                sb.append(nis);
            } else if (expr instanceof Function) {
                Function func = (Function) expr;
                sb.append(func.getName()).append(Tokens.LPAREN);
                if (func.getParameters().isEmpty() && SQLConstants.NonReserved.COUNT.equalsIgnoreCase(func.getName())) {
                    sb.append(Tokens.ALL_COLS);
                } else {
                    ColumnReference columnRef = (ColumnReference) func.getParameters().get(0);
                    Column column = columnRef.getMetadataObject();
                    String nis = getQualifiedName(column);
                    sb.append(nis);
                }
                sb.append(Tokens.RPAREN);
            }
            first = false;
        }
        return sb;
    }

    public Integer getRowLimit() {
        return rowLimit;
    }

    public Integer getRowOffset() {
        return rowOffset;
    }

    @Override
    protected boolean useAsInGroupAlias(){
        return false;
    }

    public Map<String, Class<?>> getProjectedDocumentAttributes() throws TranslatorException {
        return projectedDocumentAttributes;
    }

    RuntimeMetadata getMetadata() {
        return metadata;
    }

    DocumentNode getDocumentNode() {
        return this.rootNode;
    }
}
