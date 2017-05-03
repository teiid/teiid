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
package org.teiid.translator.infinispan.hotrod;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.teiid.infinispan.api.InfinispanDocument;
import org.teiid.infinispan.api.TableWireFormat;
import org.teiid.language.*;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;

public class InfinispanUpdateVisitor extends IckleConversionVisitor {
    protected enum OperationType {INSERT, UPDATE, DELETE, UPSERT};
    protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
    private OperationType operationType;
    private InfinispanDocument insertPayload;
    private Map<String, Object> updatePayload = new HashMap<>();
    private Object identity;
    private Condition whereClause;

    public InfinispanUpdateVisitor(RuntimeMetadata metadata) {
        super(metadata, true);
    }

    public Object getIdentity() {
        return identity;
    }

    public OperationType getOperationType() {
        return this.operationType;
    }

    public InfinispanDocument getInsertPayload() {
        return insertPayload;
    }

    public Map<String, Object> getUpdatePayload() {
        return updatePayload;
    }

    @Override
    public void visit(Insert obj) {
        this.operationType = OperationType.INSERT;
        if (obj.isUpsert()) {
            this.operationType = OperationType.UPSERT;
        }
        visitNode(obj.getTable());

        Column pkColumn = getPrimaryKey();
        if (pkColumn == null) {
            this.exceptions.add(new TranslatorException(
                    InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25013, getParentTable().getName())));
            return;
        }

        // table that insert issued for
        Table table = obj.getTable().getMetadataObject();
        try {
            // create the top table parent document, where insert is actually being done at
            InfinispanDocument targetDocument = buildTargetDocument(table, true);

            // build the payload object from insert
            int elementCount = obj.getColumns().size();
            for (int i = 0; i < elementCount; i++) {
                ColumnReference columnReference = obj.getColumns().get(i);
                Column column = columnReference.getMetadataObject();
                this.projectedExpressions.add(columnReference);

                List<Expression> values = ((ExpressionValueSource)obj.getValueSource()).getValues();
                Expression expr = values.get(i);
                Object value = resolveExpressionValue(expr);

                updateDocument(targetDocument, column, value);

                if (column.equals(pkColumn) || pkColumn.equals(normalizePseudoColumn(column))) {
                    this.identity = value;
                }
            }
            this.insertPayload = targetDocument;
        } catch (NumberFormatException e) {
            this.exceptions.add(new TranslatorException(e));
        } catch (TranslatorException e) {
            this.exceptions.add(new TranslatorException(e));
        }

        if (this.identity == null) {
            this.exceptions.add(new TranslatorException(
                    InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25004, getParentTable().getName())));
        }
    }

    @SuppressWarnings("unchecked")
    private void updateDocument(InfinispanDocument parentDocument, Column column, Object value)
            throws TranslatorException {
        boolean complexObject = this.nested;
        InfinispanDocument targetDocument = parentDocument;

        int parentTag = ProtobufMetadataProcessor.getParentTag(column);
        if (parentTag != -1) {
            // this is in one-2-one case. Dummy child will be there due to buildTargetDocument logic.
            String messageName = ProtobufMetadataProcessor.getMessageName(column);
            InfinispanDocument child = (InfinispanDocument)parentDocument.getChildDocuments(messageName).get(0);
            targetDocument = child;
            complexObject = true;
        } else if (this.nested){
            Table table = (Table)column.getParent();
            String messageName = ProtobufMetadataProcessor.getMessageName(table);
            InfinispanDocument child = (InfinispanDocument)parentDocument.getChildDocuments(messageName).get(0);
            targetDocument = child;
            complexObject = true;
        }

        if (!ProtobufMetadataProcessor.isPseudo(column)) {
            if (value instanceof List) {
                List<Object> l = (List<Object>)value;
                for(Object o : l) {
                    targetDocument.addArrayProperty(getName(column), o);
                }
            } else {
                targetDocument.addProperty(getName(column), value);
            }
            String attrName = MarshallerBuilder.getDocumentAttributeName(column, complexObject, this.metadata);
            this.updatePayload.put(attrName, value);
        }
    }

    private InfinispanDocument buildTargetDocument(Table table, boolean addDefaults) throws TranslatorException {
        TreeMap<Integer, TableWireFormat> wireMap = MarshallerBuilder.getWireMap(getParentTable(), metadata);
        String messageName = ProtobufMetadataProcessor.getMessageName(getParentTable());
        InfinispanDocument parentDocument = new InfinispanDocument(messageName, wireMap, null);

        // if there are any one-2-one relation build them and add defaults
        addDefaults(parentDocument, getParentTable(), addDefaults);

        // now create the document at child node, this is one-2-many case
        if (!table.equals(getParentTable())) {
            messageName = ProtobufMetadataProcessor.getMessageName(table);
            int parentTag = ProtobufMetadataProcessor.getParentTag(table);
            TableWireFormat twf = wireMap.get(TableWireFormat.buildNestedTag(parentTag));
            this.nested = true;
            InfinispanDocument child = new InfinispanDocument(messageName, twf.getNestedWireMap(), parentDocument);
            addDefaults(child, table, addDefaults);
            parentDocument.addChildDocument(messageName, child);
        }
        return parentDocument;
    }

    private void addDefaults(InfinispanDocument parentDocument, Table table, boolean addDefaults)
            throws TranslatorException {
        for (Column column : table.getColumns()) {
            int parentTag = ProtobufMetadataProcessor.getParentTag(column);
            if (parentTag != -1) {
                String messageName = ProtobufMetadataProcessor.getMessageName(column);
                List<?> children = parentDocument.getChildDocuments(messageName);
                InfinispanDocument child = null;
                if (children == null || children.isEmpty()) {
                    TableWireFormat twf = parentDocument.getWireMap().get(TableWireFormat.buildNestedTag(parentTag));
                    child = new InfinispanDocument(messageName, twf.getNestedWireMap(), parentDocument);
                    parentDocument.addChildDocument(messageName, child);
                } else {
                    child = (InfinispanDocument)children.get(0);
                }
                if (addDefaults && column.getDefaultValue() != null) {
                    child.addProperty(getName(column), column.getDefaultValue());
                }
            } else {
                if (addDefaults && column.getDefaultValue() != null) {
                    parentDocument.addProperty(getName(column), column.getDefaultValue());
                }
            }
        }
    }

    public Column getPrimaryKey() {
        Column pkColumn = null;
        if (getParentTable().getPrimaryKey() != null) {
            pkColumn = getParentTable().getPrimaryKey().getColumns().get(0);
        }
        return pkColumn;
    }

    private Object resolveExpressionValue(Expression expr) {
        Object value = null;
        if (expr instanceof Literal) {
            value = ((Literal)expr).getValue();
        }
        else if (expr instanceof org.teiid.language.Array) {
            org.teiid.language.Array contents = (org.teiid.language.Array)expr;
            List<Expression> arrayExprs = contents.getExpressions();
            List<Object> values = new ArrayList<Object>();
            for (Expression exp:arrayExprs) {
                if (exp instanceof Literal) {
                    values.add(((Literal)exp).getValue());
                }
                else {
                    this.exceptions.add(new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25003)));
                }
            }
            value = values;
        }
        else {
            this.exceptions.add(new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25003)));
        }
        return value;
    }


    @Override
    public void visit(Update obj) {
        this.operationType = OperationType.UPDATE;
        append(obj.getTable());
        if (obj.getWhere() != null) {
            buffer.append(Tokens.SPACE).append(SQLConstants.Reserved.WHERE).append(Tokens.SPACE);
            append(obj.getWhere());
            // Can't use the original where string because it is designed for the document model querying
            this.whereClause = obj.getWhere();
        }

        // table that update issued for
        Table table = obj.getTable().getMetadataObject();
        if (!table.equals(getParentTable())) {
            this.nested = true;
        }

        // read the properties
        try {
            InfinispanDocument targetDocument = buildTargetDocument(table, false);
            int elementCount = obj.getChanges().size();
            for (int i = 0; i < elementCount; i++) {
                Column column = obj.getChanges().get(i).getSymbol().getMetadataObject();
                Expression expr = obj.getChanges().get(i).getValue();
                Object value = resolveExpressionValue(expr);
                //this.updatePayload.put(getName(column), value);
                updateDocument(targetDocument, column, value);
            }
            this.insertPayload = targetDocument;
        } catch (TranslatorException e) {
            this.exceptions.add(e);
        }
    }

    @Override
    public void visit(Delete obj) {
        this.operationType = OperationType.DELETE;
        append(obj.getTable());

        // table that update issued for
        Table table = obj.getTable().getMetadataObject();
        if (!table.equals(getParentTable())) {
            this.nested = true;
        }

        if (obj.getWhere() != null) {
            buffer.append(Tokens.SPACE).append(SQLConstants.Reserved.WHERE).append(Tokens.SPACE);
            append(obj.getWhere());
            this.whereClause = obj.getWhere();
        }
    }

    public List<String> getProjectedColumnNames() {
        ArrayList<String> names = new ArrayList<>();
        for (Expression expr: this.projectedExpressions) {
            if (expr instanceof ColumnReference) {
                names.add(((ColumnReference)expr).getMetadataObject().getName());
            } else if (expr instanceof Function) {
                names.add(((Function)expr).getName());
            }
        }
        return names;
    }

    public String getUpdateQuery() {
        StringBuilder sb = new StringBuilder();
        sb.append(SQLConstants.Reserved.FROM);
        sb.append(Tokens.SPACE).append(super.toString());
        return  sb.toString();
    }

    public String getDeleteQuery() {
        StringBuilder sb = new StringBuilder();
        if (!isNestedOperation()) {
            addSelectedColumns(sb);
            sb.append(Tokens.SPACE);
        }
        sb.append(SQLConstants.Reserved.FROM);
        sb.append(Tokens.SPACE).append(super.toString());
        return  sb.toString();
    }

    Condition getWhereClause() {
        return whereClause;
    }
}
