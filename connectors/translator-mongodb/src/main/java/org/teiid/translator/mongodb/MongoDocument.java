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
package org.teiid.translator.mongodb;

import java.util.*;
import java.util.Map.Entry;

import org.teiid.metadata.*;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.mongodb.MutableDBRef.Association;

import com.mongodb.*;

class MongoDocument {
	private RuntimeMetadata metadata;
	private Table table;
	private MutableDBRef mergeKey;
	private List<MutableDBRef> pullKeys = new ArrayList<MutableDBRef>();
	private LinkedHashMap<List<String>, MutableDBRef> foreignKeys = new LinkedHashMap<List<String>, MutableDBRef>();
	private ArrayList<MutableDBRef> copyto = new ArrayList<MutableDBRef>();
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
		return !this.pullKeys.isEmpty();
	}

	public List<String> getEmbeddedDocumentNames(){
		ArrayList<String> names = new ArrayList<String>();
		for (MutableDBRef ref:this.pullKeys) {
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
        				MutableDBRef key = new MutableDBRef();
        				key.setName(this.table.getName());
        				key.setParentTable(t.getName());
        				key.setEmbeddedTable(this.table.getName());
        				key.setColumns(MongoDBSelectVisitor.getColumnNames(fk.getColumns()));
        				key.setReferenceColumns(fk.getReferenceColumns());
        				key.setAssociation(Association.ONE);
        				setReferenceName(key, t, MongoDBSelectVisitor.getColumnNames(fk.getColumns()));
        				this.copyto.add(key);
        			}
        		}
        	}
		}
	}

	private void setReferenceName(MutableDBRef key, Table table, List<String> columnNames) {
		boolean ispartofPK = false;
		for(String column:columnNames) {
			if (MongoDBSelectVisitor.isPartOfPrimaryKey(table, column)) {
				ispartofPK = true;
			}
		}

		if (ispartofPK) {
			key.setReferenceName("_id"); //$NON-NLS-1$
		}
		else {
			key.setReferenceName(columnNames.get(0));
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

				MutableDBRef key = new MutableDBRef();
				key.setName(fk.getReferenceTableName());
				key.setParentTable(this.table.getName());
				key.setColumns(MongoDBSelectVisitor.getColumnNames(fk.getColumns()));
				key.setReferenceColumns(fk.getReferenceColumns());
				key.setEmbeddedTable(fk.getReferenceTableName());
				// if the primary is reference, then it needs to built as such during the fetch
				if (MongoDBSelectVisitor.isPartOfForeignKey(referenceTable, fk.getReferenceColumns().get(0))) {
					key.setIdReference(MongoDBSelectVisitor.getForeignKeyRefTable(referenceTable, fk.getReferenceColumns().get(0)));
				}
				setReferenceName(key, this.table, MongoDBSelectVisitor.getColumnNames(fk.getColumns()));
				this.pullKeys.add(key);
			}
    	}
	}

	private void buildForeignKeyReferences() throws TranslatorException {
    	for (ForeignKey fk:this.table.getForeignKeys()) {
			MutableDBRef key = new MutableDBRef();
			key.setParentTable(fk.getReferenceTableName());
			key.setEmbeddedTable(this.table.getName());
			key.setName(fk.getName());
			key.setColumns(MongoDBSelectVisitor.getColumnNames(fk.getColumns()));
			key.setReferenceColumns(fk.getReferenceColumns());
			Table refTable = this.metadata.getTable(this.table.getParent().getName(), fk.getReferenceTableName());
			setReferenceName(key, refTable, MongoDBSelectVisitor.getColumnNames(fk.getColumns()));
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
				MutableDBRef key = new MutableDBRef();
				key.setName(this.table.getName());
				key.setParentTable(mergeTable.getName());
				key.setColumns(MongoDBSelectVisitor.getColumnNames(fk.getColumns()));
				key.setReferenceColumns(fk.getReferenceColumns());
				key.setEmbeddedTable(this.table.getName());
				key.setAssociation(Association.MANY);
				setReferenceName(key, mergeTable, MongoDBSelectVisitor.getColumnNames(fk.getColumns()));

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
		Iterator<Entry<List<String>, MutableDBRef>> it = this.foreignKeys.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry<List<String>, MutableDBRef> pairs = it.next();
	        List<String> keys = pairs.getKey();
	        MutableDBRef ref = pairs.getValue();
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
		if (!this.pullKeys.isEmpty()) {
			for (MutableDBRef ref:this.pullKeys) {
				if (ref.getColumns().contains(columnName) && ref.getParentTable().equals(tableName)) {
					for (int i = 0; i < ref.getColumns().size(); i++) {
						String column = ref.getColumns().get(i);
						if (column.equals(columnName)) {
							String referenceColumn = ref.getReferenceColumns().get(i);
							ref.setId(referenceColumn, value);
						}
					}
				}
			}
		}
	}

	public MutableDBRef getFKReference(String columnName) {
		Iterator<Entry<List<String>, MutableDBRef>> it = this.foreignKeys.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry<List<String>, MutableDBRef> pairs = it.next();
	        List<String> keys = pairs.getKey();
	        MutableDBRef ref = pairs.getValue();
	        if (keys.contains(columnName)) {
	        	return ref;
	        }
	    }
	    return null;
	}

	public DBObject getEmbeddedDocument(DB mongoDB, String docName) {
		for (MutableDBRef ref:this.pullKeys) {
			if (ref.getName().equals(docName)) {
				DBRef dbRef = ref.getDBRef(mongoDB, false);
				if (dbRef != null) {
					return mongoDB.getCollection(dbRef.getRef()).findOne(new BasicDBObject("_id", dbRef.getId())); //$NON-NLS-1$
				}
			}
		}
		return null;
	}

	static class MergeDetails {
		DBObject match;
		boolean nested;
		BasicDBObject update;
		Association association;
		String embeddedDocument;
	}
	
	// for nested merge, format is
	// > db.customer.update({"_id":1, "rental._id":2}, {$push:{"rental.$.payment":{"foo":"bar"}}})
	public MergeDetails getMergeParentCriteria(DB mongo, DBObject match, String embedTable, BasicDBObject insert, boolean nested) throws TranslatorException {
		MongoDocument targetDocument = getDocument(this.mergeKey.getParentTable());
		MergeDetails md = new MergeDetails();
		md.association = getMergeAssociation();
		
		if (targetDocument.isMerged()) {
			// this is the case of nested merge
			if (match == null) {
				match = new BasicDBObject(this.mergeKey.getParentTable()+"._id", this.mergeKey.getDBRef(mongo, true).getId()); //$NON-NLS-1$
				embedTable = "$"; //$NON-NLS-1$
			}
			else {
				DBCollection collection = mongo.getCollection(this.mergeKey.getParentTable());
				DBObject result = collection.findOne(match);
				match = new BasicDBObject(this.mergeKey.getParentTable()+"._id", result.get("_id")); //$NON-NLS-1$ //$NON-NLS-2$
			}
			return targetDocument.getMergeParentCriteria(mongo, match, embedTable+"."+getTable().getName(), insert, true); //$NON-NLS-1$
		}

		if (match == null) {
			DBRef dbRef = this.mergeKey.getDBRef(mongo, true);
			if (dbRef != null) {
				match = new BasicDBObject("_id", dbRef.getId()); //$NON-NLS-1$
				//throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18015, this.mergeKey.getParentTable(), this.mergeKey.getId(), this.mergeKey.getEmbeddedTable()));
			}
			else {
				match = QueryBuilder.start(this.mergeKey.getEmbeddedTable()).exists(true).get(); 
			}			
		}

		DBCollection collection = mongo.getCollection(this.mergeKey.getParentTable());
		DBObject result = collection.findOne(match);
		if (result == null) {
			throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18006, this.mergeKey.getParentTable(), this.mergeKey.getId(), this.mergeKey.getEmbeddedTable()));
		}
		//((BasicDBObject)match).append("_id", result.get("_id")); //$NON-NLS-1$ //$NON-NLS-2$

		String nestedKey = getTable().getName();
		if (embedTable != null) {
			nestedKey = getTable().getName()+"."+embedTable; //$NON-NLS-1$
		}
		md.embeddedDocument = nestedKey;
		md.match = match;
		md.update = new BasicDBObject(nestedKey, insert);
		md.nested = nested;
		return md;
	}

	/**
	 * References that are going OUT
	 * @return
	 */
	public List<MutableDBRef> getEmbeddedInReferences(){
		return this.copyto;
	}

	MutableDBRef getMergeKey() {
		return this.mergeKey;
	}

	/**
	 * references that are coming IN
	 * @return
	 */
	List<MutableDBRef> getEmbeddableReferences(){
		return this.pullKeys;
	}
	
    public boolean embeds(MongoDocument right) throws TranslatorException {
        if (equals(right)) {
            return false;
        }

        for (MutableDBRef ref:this.pullKeys) {
            if (ref.getEmbeddedTable().equals(right.getTable().getName())) {
                return true;
            }
        }

        for (MutableDBRef ref:right.getEmbeddedInReferences()) {
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

		for (MutableDBRef ref:this.pullKeys) {
			MongoDocument parent = getDocument(ref.getEmbeddedTable());
			if (parent.embeds(right)) {
				return true;
			}
		}

		for (MutableDBRef ref:right.getEmbeddedInReferences()) {
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

	private MongoDocument getDocument(String tblName) throws TranslatorException {
		if (this.relatedDocs.get(tblName) != null) {
			return this.relatedDocs.get(tblName);
		}

		Table tbl = this.metadata.getTable(this.table.getParent().getName(), tblName);
		MongoDocument doc = new MongoDocument(tbl, this.metadata);
		this.relatedDocs.put(tblName, doc);

		return doc;
	}

	public MutableDBRef getEmbeddedDocumentReferenceKey(MongoDocument right) throws TranslatorException {
		if (equals(right)) {
			return null;
		}

		for (MutableDBRef ref:this.pullKeys) {
			if (ref.getEmbeddedTable().equals(right.getTable().getName())) {
				return ref.clone();
			}
		}

		for (MutableDBRef ref:right.getEmbeddedInReferences()) {
			if (ref.getParentTable().equals(getTable().getName())) {
				return ref.clone();
			}
		}

		if (right.isMerged()) {
			if (right.mergeKey.getParentTable().equals(getTable().getName())) {
				return right.mergeKey.clone();
			}
		}
		return getNestedEmbeddedDocumentReferenceKey(right);
	}

	private MutableDBRef getNestedEmbeddedDocumentReferenceKey(MongoDocument right) throws TranslatorException {

		for (MutableDBRef ref:this.pullKeys) {
			MongoDocument parent = getDocument(ref.getEmbeddedTable());
			if (parent.contains(right)) {
				MutableDBRef key = parent.getEmbeddedDocumentReferenceKey(right);
				key.setName(parent.getTable().getName()+"."+key.getName()); //$NON-NLS-1$
				key.setNested(true);
				return key;
			}
		}

		for (MutableDBRef ref:right.getEmbeddedInReferences()) {
			MongoDocument parent = getDocument(ref.getParentTable());
			if (parent.contains(right)) {
				MutableDBRef key = parent.getEmbeddedDocumentReferenceKey(right);
				key.setName(parent.getTable().getName()+"."+key.getName()); //$NON-NLS-1$
				key.setNested(true);
				return key;
			}
		}

		if (right.isMerged()) {
			MongoDocument parent = getDocument(right.mergeKey.getParentTable());
			if (parent.contains(right)) {
				MutableDBRef key = parent.getEmbeddedDocumentReferenceKey(right);
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

	public void setDocumentAlias(String alias) {
		this.documentAlias = alias;
	}
	
	public String getDocumentName() {
		if (this.documentAlias == null) {
			return getTable().getName();
		}
		return this.documentAlias;
	}
}
