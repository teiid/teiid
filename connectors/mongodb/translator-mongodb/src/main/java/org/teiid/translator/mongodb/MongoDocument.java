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
package org.teiid.translator.mongodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.mongodb.MergeDetails.Association;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.DBRef;

class MongoDocument {
    private RuntimeMetadata metadata;
    private Table table;
    private MergeDetails mergeKey;
    private List<MergeDetails> embeddedKeys = new ArrayList<MergeDetails>();
    private LinkedHashMap<List<String>, MergeDetails> foreignKeys = new LinkedHashMap<List<String>, MergeDetails>();
    private ArrayList<MergeDetails> copyto = new ArrayList<MergeDetails>();
    private MongoDocument mergeDocument;
    private HashMap<String, MongoDocument> relatedDocs = new HashMap<String, MongoDocument>();
    private String documentAlias;

    public MongoDocument(Table table, RuntimeMetadata metadata) throws TranslatorException {
        this.table = table;
        this.metadata = metadata;

        if (isEmbeddable() && isMerged()) {
            throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18013, table.getName()));
        }

        build();
    }

    public Table getTable() {
        return this.table;
    }

    public Table getTargetTable() throws TranslatorException {
        if (isMerged()) {
            Table merge = getMergeTable();
            MongoDocument mergeDoc = getDocument(merge.getName());
            if (mergeDoc.isMerged()) {
                return mergeDoc.getTargetTable();
            }
            return merge;
        }
        return getTable();
    }

    public MongoDocument getTargetDocument() throws TranslatorException {
        if (isMerged()) {
            return getMergeDocument().getTargetDocument();
        }
        return this;
    }

    public boolean isEmbeddable() {
        return isEmbeddable(this.table);
    }

    public static boolean isEmbeddable(Table tbl) {
        return Boolean.parseBoolean(tbl.getProperty(MongoDBMetadataProcessor.EMBEDDABLE, false));
    }

    public boolean isMerged() {
        return this.table.getProperty(MongoDBMetadataProcessor.MERGE, false) != null;
    }

    public Table getMergeTable() throws TranslatorException {
        String tblName = this.table.getProperty(MongoDBMetadataProcessor.MERGE, false);
        if (tblName == null) {
            return null;
        }
        Table mergeTable = this.metadata.getTable(this.table.getParent().getName(), tblName);
        return mergeTable;
    }

    public MongoDocument getMergeDocument() throws TranslatorException {
        if (this.mergeDocument != null) {
            return this.mergeDocument;
        }

        Table mergeTable = getMergeTable();
        if (mergeTable != null) {
            this.mergeDocument = new MongoDocument(mergeTable, this.metadata);
        }
        return this.mergeDocument;
    }

    public Association getMergeAssociation() {
        return this.mergeKey.getAssociation();
    }

    public boolean hasEmbeddedDocuments() {
        return !this.embeddedKeys.isEmpty();
    }

    public List<String> getEmbeddedDocumentNames(){
        ArrayList<String> names = new ArrayList<String>();
        for (MergeDetails ref:this.embeddedKeys) {
            names.add(ref.getName());
        }
        return names;
    }

    private void build() throws TranslatorException {
        buildForeignKeyReferences();
        buildEmbeddableIntoReferences();
        buildEmbeddedReferences();
        buildMergeKey();
    }

    private void buildEmbeddableIntoReferences() {
        // if this table is marked as "embeddable", figure out all the tables it is
        // copied in.
        if (isEmbeddable()) {
            for (Table t:this.table.getParent().getTables().values()) {
                for (ForeignKey fk:t.getForeignKeys()) {
                    if (fk.getReferenceKey().getParent().equals(this.table)){
                        MergeDetails key = new MergeDetails(this);
                        key.setName(this.table.getName());
                        key.setParentTable(t.getName());
                        key.setEmbeddedTable(this.table.getName());
                        key.setColumns(MongoDBSelectVisitor.getColumnNames(fk.getColumns()));
                        key.setReferenceColumns(fk.getReferenceColumns());
                        key.setAssociation(Association.ONE);
                        this.copyto.add(key);
                    }
                }
            }
        }
    }

    private void buildEmbeddedReferences() throws TranslatorException {
        for (ForeignKey fk:this.table.getForeignKeys()) {
            Table referenceTable = fk.getReferenceKey().getParent();
            MongoDocument refereceDoc = new MongoDocument(referenceTable, this.metadata);
            if (refereceDoc.isEmbeddable()) {

                // if this table itself is merged into embedded; then skip it
                if (isMerged() && getMergeTable().getName().equals(referenceTable.getName())) {
                    // avoid self inclusion
                    continue;
                }

                MergeDetails key = new MergeDetails(this);
                key.setName(fk.getReferenceTableName());
                key.setParentTable(this.table.getName());
                key.setReferenceColumns(MongoDBSelectVisitor.getColumnNames(fk.getColumns()));
                key.setColumns(fk.getReferenceColumns());
                key.setEmbeddedTable(fk.getReferenceTableName());

                // if the primary is reference, then it needs to built as such during the fetch
                if (MongoDBSelectVisitor.isPartOfForeignKey(referenceTable, fk.getReferenceColumns().get(0))) {
                    key.setIdReference(MongoDBSelectVisitor.getForeignKeyRefTable(referenceTable, fk.getReferenceColumns().get(0)));
                }
                this.embeddedKeys.add(key);
            }
        }
    }

    private void buildForeignKeyReferences() {
        for (ForeignKey fk:this.table.getForeignKeys()) {
            MergeDetails key = new MergeDetails(this);
            key.setParentTable(fk.getReferenceTableName());
            key.setEmbeddedTable(this.table.getName());
            key.setName(fk.getName());
            key.setColumns(MongoDBSelectVisitor.getColumnNames(fk.getColumns()));
            key.setReferenceColumns(fk.getReferenceColumns());
            this.foreignKeys.put(MongoDBSelectVisitor.getColumnNames(fk.getColumns()), key);
        }
    }

    private void buildMergeKey() throws TranslatorException {
        if (!isMerged()) {
            return;
        }
        Table mergeTable = getMergeTable();
        for (ForeignKey fk:this.table.getForeignKeys()) {
            if (fk.getReferenceKey().getParent().equals(mergeTable)) {
                MergeDetails key = new MergeDetails(this);
                key.setName(this.table.getName());
                key.setParentTable(mergeTable.getName());
                key.setColumns(MongoDBSelectVisitor.getColumnNames(fk.getColumns()));
                key.setReferenceColumns(fk.getReferenceColumns());
                key.setEmbeddedTable(this.table.getName());
                key.setAssociation(Association.MANY);

                // check to see if the parent table has relation to this table, if yes
                // then it is one-to-one, other wise many-to-one
                for (ForeignKey fk1:mergeTable.getForeignKeys()) {
                    if (fk1.getReferenceKey().getParent().equals(this.table)) {
                        key.setAssociation(Association.ONE);
                        break;
                    }
                }

                // or for 1 to 1 to be true, fk columns are same as PK columns
                if (this.table.getPrimaryKey() != null && sameKeys(MongoDBSelectVisitor.getColumnNames(fk.getColumns()), MongoDBSelectVisitor.getColumnNames(this.table.getPrimaryKey().getColumns()))) {
                    key.setAssociation(Association.ONE);
                }
                this.mergeKey = key;
                break;
            }
        }
    }

    private boolean sameKeys(List<String> columns1, List<String> columns2) {
        if (columns1.size() != columns2.size()) {
            return false;
        }
        for (String name : columns1) {
            if (!columns2.contains(name)) {
                return false;
            }
        }
        return true;
    }

    public void updateReferenceColumnValue(String tableName, String columnName, Object value ) {
        Iterator<Entry<List<String>, MergeDetails>> it = this.foreignKeys.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<List<String>, MergeDetails> pairs = it.next();
            List<String> keys = pairs.getKey();
            MergeDetails ref = pairs.getValue();
            if (keys.contains(columnName) && ref.getEmbeddedTable().equals(tableName)) {
                ref.setId(columnName, value);
            }
        }

        // parent table selection query.
        if (this.mergeKey != null
                && this.mergeKey.getColumns().contains(columnName)
                && this.mergeKey.getEmbeddedTable().equals(tableName)) {

            for (int i = 0; i < this.mergeKey.getColumns().size(); i++) {
                String column = this.mergeKey.getColumns().get(i);
                if (column.equals(columnName)) {
                    String referenceColumn = this.mergeKey.getReferenceColumns().get(i);
                    this.mergeKey.setId(referenceColumn, value);
                }
            }
        }

        // child table selection query
        if (!this.embeddedKeys.isEmpty()) {
            for (MergeDetails ref:this.embeddedKeys) {
                if (ref.getReferenceColumns().contains(columnName) && ref.getParentTable().equals(tableName)) {
                    for (int i = 0; i < ref.getReferenceColumns().size(); i++) {
                        String column = ref.getReferenceColumns().get(i);
                        if (column.equals(columnName)) {
                            String referenceColumn = ref.getColumns().get(i);
                            ref.setId(referenceColumn, value);
                        }
                    }
                }
            }
        }
    }

    public MergeDetails getFKReference(String columnName) {
        Iterator<Entry<List<String>, MergeDetails>> it = this.foreignKeys.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<List<String>, MergeDetails> pairs = it.next();
            List<String> keys = pairs.getKey();
            MergeDetails ref = pairs.getValue();
            if (keys.contains(columnName)) {
                return ref;
            }
        }
        return null;
    }

    public DBObject getEmbeddedDocument(DB mongoDB, String docName) {
        for (MergeDetails ref:this.embeddedKeys) {
            if (ref.getName().equals(docName)) {
                DBRef dbRef = ref.getDBRef(mongoDB, false);
                if (dbRef != null) {
                    return mongoDB.getCollection(dbRef.getCollectionName()).findOne(new BasicDBObject("_id", dbRef.getId())); //$NON-NLS-1$
                }
            }
        }
        return null;
    }

    public String getColumnName(String columnName) throws TranslatorException {
        String originalColumnName = columnName;
        boolean primaryKey = false;

        if (isPartOfPrimaryKey(originalColumnName)) {
            columnName = "_id"; //$NON-NLS-1$
            if (hasCompositePrimaryKey()) {
                columnName = "_id."+originalColumnName; //$NON-NLS-1$
            }
            primaryKey = true;
        }

        if (isMerged()) {
            if (primaryKey && pkExistsInParent(this)) {
                if (this.documentAlias != null) {
                    return this.documentAlias+"."+columnName; //$NON-NLS-1$
                }
                return columnName;
            }

            // even if the key is part of foreign key, then this
            // key does not exist in child document.
            if (isPartOfForeignKey(originalColumnName)) {
                return getMergeDocument().getColumnName(this.mergeKey.getParentColumnName(originalColumnName));
            }
            return this.getDocumentName()+"."+columnName; //$NON-NLS-1$
        }
        else if (isEmbeddable()) {

        }
        return columnName;
    }

    public String getDocumentName() throws TranslatorException {
        return this.documentAlias != null ? this.documentAlias:getQualifiedName(false);
    }

    // if a table is ONE-2-ONE all the way to target document, then PK exists only on top
    public boolean pkExistsInParent(MongoDocument document) throws TranslatorException {
        while(document.isMerged()) {
            if (document.getMergeKey().getAssociation() == Association.ONE) {
                document = document.getMergeDocument();
            }
            else {
                return false;
            }
        }
        return true;
    }

    /**
     * References that are going OUT
     * @return
     */
    public List<MergeDetails> getEmbeddedIntoReferences(){
        return this.copyto;
    }

    MergeDetails getMergeKey() {
        return this.mergeKey;
    }

    /**
     * references that are coming IN
     * @return
     */
    List<MergeDetails> getEmbeddedReferences(){
        return this.embeddedKeys;
    }

    public boolean embeds(MongoDocument right) throws TranslatorException {
        if (equals(right)) {
            return false;
        }

        for (MergeDetails ref:this.embeddedKeys) {
            if (ref.getEmbeddedTable().equals(right.getTable().getName())) {
                return true;
            }
        }

        for (MergeDetails ref:right.getEmbeddedIntoReferences()) {
            if (ref.getParentTable().equals(getTable().getName())) {
                return true;
            }
        }
        return nestedEmbedded(right);
    }

    public boolean merges(MongoDocument right) throws TranslatorException {

        if (equals(right)) {
            return false;
        }

        if (right.isMerged()) {
            if (right.mergeKey.getParentTable().equals(getTable().getName())) {
                return true;
            }
        }
        return nestedMerge(right);
    }

    public boolean contains(MongoDocument right) throws TranslatorException {
        return (embeds(right) || merges(right));
    }

    /**
     * Check if it is grand kids. Multiple nesting..
     * @param right
     * @return
     */
    private boolean nestedEmbedded(MongoDocument right) throws TranslatorException {

        for (MergeDetails ref:this.embeddedKeys) {
            MongoDocument parent = getDocument(ref.getEmbeddedTable());
            if (parent.embeds(right)) {
                return true;
            }
        }

        for (MergeDetails ref:right.getEmbeddedIntoReferences()) {
            MongoDocument parent = getDocument(ref.getParentTable());
            if (parent.embeds(right)) {
                return true;
            }
        }
        return false;
    }

    private boolean nestedMerge(MongoDocument right) throws TranslatorException {
        if (right.isMerged()) {
            MongoDocument parent = getDocument(right.mergeKey.getParentTable());
            if (parent.merges(right)) {
                return true;
            }
        }
        return false;
    }

    public String getQualifiedName(boolean positional) throws TranslatorException {
        MongoDocument document = this;
        String tableName = getTable().getName();
        // check if document nested merge, i.e parent is also merged document
        while (document.isMerged()) {
            document = document.getMergeDocument();
            if (document.isMerged()) {
                if (positional) {
                    if (document.mergeKey.getAssociation() == Association.ONE) {
                        tableName = document.getTable().getName()+"."+tableName; //$NON-NLS-1$
                    }
                    else {
                        tableName = document.getTable().getName()+".$."+tableName; //$NON-NLS-1$
                    }
                }
                else {
                    tableName = document.getTable().getName()+"."+tableName; //$NON-NLS-1$
                }
            }
        }
        return tableName;
    }

    private MongoDocument getDocument(String tblName) throws TranslatorException {
        if (this.relatedDocs.get(tblName) != null) {
            return this.relatedDocs.get(tblName);
        }

        Table tbl = this.metadata.getTable(this.table.getParent().getName(), tblName);
        MongoDocument doc = new MongoDocument(tbl, this.metadata);
        this.relatedDocs.put(tblName, doc);

        return doc;
    }

    public MergeDetails getEmbeddedDocumentReferenceKey(MongoDocument right) throws TranslatorException {
        if (equals(right)) {
            return null;
        }

        for (MergeDetails ref:this.embeddedKeys) {
            if (ref.getEmbeddedTable().equals(right.getTable().getName())) {
                return ref.clone();
            }
        }

        for (MergeDetails ref:right.getEmbeddedIntoReferences()) {
            if (ref.getParentTable().equals(getTable().getName())) {
                return ref.clone();
            }
        }
        return getNestedEmbeddedDocumentReferenceKey(right);
    }

    private MergeDetails getNestedEmbeddedDocumentReferenceKey(MongoDocument right) throws TranslatorException {

        for (MergeDetails ref:this.embeddedKeys) {
            MongoDocument parent = getDocument(ref.getEmbeddedTable());
            if (parent.contains(right)) {
                MergeDetails key = parent.getEmbeddedDocumentReferenceKey(right);
                key.setName(parent.getTable().getName()+"."+key.getName()); //$NON-NLS-1$
                key.setNested(true);
                return key;
            }
        }

        for (MergeDetails ref:right.getEmbeddedIntoReferences()) {
            MongoDocument parent = getDocument(ref.getParentTable());
            if (parent.contains(right)) {
                MergeDetails key = parent.getEmbeddedDocumentReferenceKey(right);
                key.setName(parent.getTable().getName()+"."+key.getName()); //$NON-NLS-1$
                key.setNested(true);
                return key;
            }
        }
        return null;
    }

    public boolean isPartOfPrimaryKey(String columnName) {
        KeyRecord pk = this.table.getPrimaryKey();
        if (pk != null) {
            for (Column column:pk.getColumns()) {
                if (column.getName().equals(columnName)) {
                    return true;
                }
            }
        }
        return false;
    }

    boolean hasCompositePrimaryKey() {
        KeyRecord pk = this.table.getPrimaryKey();
        return pk.getColumns().size() > 1;
    }

    boolean isPartOfForeignKey(String columnName) {
        for (ForeignKey fk : this.table.getForeignKeys()) {
            for (Column column : fk.getColumns()) {
                if (column.getName().equals(columnName)) {
                    return true;
                }
            }
        }
        return false;
    }

    boolean isCompositeForeignKey(String columnName) {
        for (ForeignKey fk : this.table.getForeignKeys()) {
            for (Column column : fk.getColumns()) {
                if (column.getName().equals(columnName)) {
                    return fk.getColumns().size() > 1;
                }
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.table == null) ? 0 : this.table.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof MongoDocument)) {
            return false;
        }

        MongoDocument other = (MongoDocument) obj;
        if (getTable().getName().equals(other.getTable().getName())) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return getTable().getName();
    }

    public void setAlias(String alias) {
        this.documentAlias = alias;
    }

    public String getAlias() {
        return this.documentAlias;
    }
}
