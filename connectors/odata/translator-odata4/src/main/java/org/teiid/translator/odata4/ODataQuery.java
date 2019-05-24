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
package org.teiid.translator.odata4;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Condition;
import org.teiid.language.Join.JoinType;
import org.teiid.language.LanguageUtil;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.document.DocumentNode;
import org.teiid.translator.odata4.ODataDocumentNode.ODataDocumentType;

public class ODataQuery {
    protected ODataExecutionFactory executionFactory;
    protected RuntimeMetadata metadata;
    protected ODataDocumentNode rootDocument;
    protected DocumentNode joinNode;

    protected ArrayList<ODataDocumentNode> complexTables = new ArrayList<ODataDocumentNode>();
    protected ArrayList<ODataDocumentNode> expandTables = new ArrayList<ODataDocumentNode>();

    public ODataQuery(ODataExecutionFactory executionFactory, RuntimeMetadata metadata) {
        this.executionFactory = executionFactory;
        this.metadata = metadata;
    }

    public void addRootDocument(Table table) throws TranslatorException {
        ODataDocumentNode node = null;

        if (this.rootDocument == null) {
            if (ODataMetadataProcessor.isEntitySet(table)) {
                node = new ODataDocumentNode(table, ODataDocumentType.PRIMARY,
                        ODataMetadataProcessor.isCollection(table));
                this.rootDocument = node;
                this.joinNode = node;
            } else {
                // add the complex or expand tables
                Table parentTable = ODataMetadataProcessor.getComplexTableParentTable(this.metadata, table);
                if (parentTable == null) {
                    throw new TranslatorException(ODataPlugin.Event.TEIID17028,
                            ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17028, table.getName()));
                }
                addRootDocument(parentTable);
                // if this is not complex/navigation we already added
                // this as the parent document; no need to join
                if (ODataMetadataProcessor.isComplexType(table)
                        || ODataMetadataProcessor.isNavigationType(table)) {
                    joinChildDocument(table, JoinType.INNER_JOIN);
                }
            }
        } else {
            joinChildDocument(table, JoinType.INNER_JOIN);
        }
    }

    private ODataDocumentNode addComplexOrNavigation(Table table) {
        ODataDocumentNode node;
        if (ODataMetadataProcessor.isComplexType(table)) {
            node = new ODataDocumentNode(table, ODataDocumentType.COMPLEX,
                    ODataMetadataProcessor.isCollection(table));
            this.complexTables.add(node);
        } else if (ODataMetadataProcessor.isNavigationType(table)){
            node = new ODataDocumentNode(table, ODataDocumentType.EXPAND,
                    ODataMetadataProcessor.isCollection(table));
            this.expandTables.add(node);
        } else {
            node = new ODataDocumentNode(table, ODataDocumentType.EXPAND,
                    ODataMetadataProcessor.isCollection(table));
            this.expandTables.add(node);
        }
        return node;
    }

    private void joinChildDocument(Table table, JoinType joinType) throws TranslatorException {
        ODataDocumentNode node = addComplexOrNavigation(table);
        this.joinNode = this.joinNode.joinWith(joinType, node);
    }

    public Condition addNavigation(Condition obj, JoinType joinType, Table right)
            throws TranslatorException {
        joinChildDocument(right, joinType);
        return parseKeySegmentFromCondition(obj);
    }

    public Condition addNavigation(Condition obj, JoinType joinType, Table left, Table right)
            throws TranslatorException {
        addRootDocument(left);
        joinChildDocument(right, joinType);
        return parseKeySegmentFromCondition(obj);
    }

    public DocumentNode getRootDocument() {
        return this.rootDocument;
    }

    protected String processFilter(Condition condition) throws TranslatorException {
        List<Condition> crits = LanguageUtil.separateCriteriaByAnd(condition);
        if (!crits.isEmpty()) {
            for(Iterator<Condition> iter = crits.iterator(); iter.hasNext();) {
                Condition crit = iter.next();
                ODataFilterVisitor visitor = new ODataFilterVisitor(this.executionFactory, this.metadata, this);
                visitor.appendFilter(crit);
            }
        }
        StringBuilder sb = new StringBuilder();
        if (this.rootDocument.getFilter() != null) {
            sb.append(this.rootDocument.getFilter());
        }
        for (ODataDocumentNode use:this.complexTables) {
            if (use.getFilter() != null) {
                if (sb.length() > 0) {
                    sb.append(" and ");
                }
                sb.append(use.getFilter());
            }
        }
        return sb.length() == 0?null:sb.toString();
    }

    protected Condition parseKeySegmentFromCondition(Condition obj)
            throws TranslatorException {
        List<Condition> crits = LanguageUtil.separateCriteriaByAnd(obj);
        if (!crits.isEmpty()) {
            boolean modified = false;
            for(Iterator<Condition> iter = crits.iterator(); iter.hasNext();) {
                Condition crit = iter.next();
                if (crit instanceof Comparison) {
                    Comparison left = (Comparison) crit;
                    boolean leftAdded = parseKeySegmentFromComparison(left);
                    if (leftAdded) {
                        iter.remove();
                        modified = true;
                    }
                }
            }
            if (modified) {
                return LanguageUtil.combineCriteria(crits);
            }
        }
        return obj;
    }

    private boolean parseKeySegmentFromComparison(Comparison obj) throws TranslatorException {
        if (obj.getOperator().equals(Comparison.Operator.EQ)) {
            if (obj.getLeftExpression() instanceof ColumnReference
                    && obj.getRightExpression() instanceof ColumnReference) {
                Column left = ((ColumnReference)obj.getLeftExpression()).getMetadataObject();
                Column right = ((ColumnReference)obj.getRightExpression()).getMetadataObject();

                if (isJoinOrPkColumn(left) && isJoinOrPkColumn(right)) {
                    // in odata the navigation from parent to child implicit by their keys
                    return true;
                }
            }
        }
        return false;
    }

    ODataDocumentNode getSchemaElement(Table table) {
        if (this.rootDocument != null && this.rootDocument.getTable().equals(table)) {
            return this.rootDocument;
        }
        for (ODataDocumentNode schemaElement:this.complexTables) {
            if (schemaElement.getTable().equals(table)) {
                return schemaElement;
            }
        }
        for (ODataDocumentNode schemaElement:this.expandTables) {
            if (schemaElement.getTable().equals(table)) {
                return schemaElement;
            }
        }
        return null;
    }

    private boolean isJoinOrPkColumn(Column column) {
        Table table = (Table)column.getParent();
        boolean isKey = (table.getPrimaryKey().getColumnByName(column.getName()) != null);
        if (!isKey) {
            for(ForeignKey fk:table.getForeignKeys()) {
                if (fk.getColumnByName(column.getName()) != null) {
                    isKey = true;
                }
            }
        }
        return isKey;
    }
}
