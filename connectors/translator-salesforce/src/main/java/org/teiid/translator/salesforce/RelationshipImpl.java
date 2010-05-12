package org.teiid.translator.salesforce;

public class RelationshipImpl implements Relationship {
	boolean cascadeDelete;
	public String childTablename;
	public String parentTableName;
	public String foreignKeyField;
	
	public void setCascadeDelete(boolean delete) {
		cascadeDelete = delete;
	}

	public boolean isCascadeDelete() {
		return cascadeDelete;
	}

	public void setChildTable(String childTable) {
		childTablename = childTable;
	}

	public String getChildTable() {
		return childTablename;
	}

	public String getForeignKeyField() {
		return foreignKeyField;
	}

	public void setForeignKeyField(String foreignKeyField) {
		this.foreignKeyField = foreignKeyField;
	}

	public String getParentTable() {
		return parentTableName;
	}

	public void setParentTable(String parentTableName) {
		this.parentTableName = parentTableName;
	}
}
