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

package org.teiid.metadata;

import java.util.List;

/**
 * ForeignKeyRecordImpl
 */
public class ForeignKey extends KeyRecord {

	private static final long serialVersionUID = -8835750783230001311L;
	
	private String uniqueKeyID;
    private KeyRecord primaryKey;
    private String referenceTableName;
    private List<String> referenceColumns;

	public static final String ALLOW_JOIN = AbstractMetadataRecord.RELATIONAL_URI + "allow-join"; //$NON-NLS-1$
    
    public ForeignKey() {
		super(Type.Foreign);
	}
    
    public String getUniqueKeyID() {
        return uniqueKeyID;
    }

    /**
     * @param object
     */
    public void setUniqueKeyID(String keyID) {
        uniqueKeyID = keyID;
    }    
    
    /**
     * @return the primary key or unique key referenced by this foreign key
     */
    public KeyRecord getPrimaryKey() {
    	return this.primaryKey;
    }
    
    /**
     * 
     * @param primaryKey,  the primary key or unique key referenced by this foreign key
     */
    public void setPrimaryKey(KeyRecord primaryKey) {
		this.primaryKey = primaryKey;
	}

	public String getReferenceTableName() {
		if (referenceTableName == null && primaryKey != null) {
			referenceTableName = primaryKey.getParent().getName();
		}
		return referenceTableName;
	}

	public void setReferenceTableName(String tableName) {
		this.referenceTableName = tableName;
	}

	public List<String> getReferenceColumns() {
		return referenceColumns;
	}

	public void setReferenceColumns(List<String> referenceColumns) {
		this.referenceColumns = referenceColumns;
	}
}