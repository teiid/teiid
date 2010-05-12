package org.teiid.translator.salesforce;

public interface Relationship {

	void setParentTable(String name);

	void setChildTable(String childSObject);

	void setForeignKeyField(String field);

	void setCascadeDelete(boolean cascadeDelete);

	public boolean isCascadeDelete();

	public String getChildTable();

	public String getForeignKeyField();

	public String getParentTable();

}
