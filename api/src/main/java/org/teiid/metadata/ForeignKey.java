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

import java.util.ArrayList;
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
     * @deprecated
     * @see #getReferenceKey()
     */
    public KeyRecord getPrimaryKey() {
    	return this.primaryKey;
    }
    
    /**
     * @return the primary or unique key referenced by this foreign key
     */
    public KeyRecord getReferenceKey() {
    	return this.primaryKey;
    }
    
    /**
     * Note: does not need to be directly called.  The engine can resolve the
     * referenced key if {@link #setReferenceColumns(List)} and {@link #setReferenceTableName(String)}
     * are used.
     * @param primaryKey,  the primary key or unique key referenced by this foreign key
     */
    public void setReferenceKey(KeyRecord primaryKey) {
    	this.primaryKey = primaryKey;
		if (this.primaryKey != null) {
			this.referenceColumns = new ArrayList<String>();
			for (Column c : primaryKey.getColumns()) {
				this.referenceColumns.add(c.getName());
			}
			if (primaryKey.getParent() != null) {
				this.referenceTableName = primaryKey.getParent().getName();
			}
			this.uniqueKeyID = primaryKey.getUUID();
		} else {
			this.referenceColumns = null;
			this.referenceTableName = null;
			this.uniqueKeyID = null;
		}
    }

    /**
     * 
     * @param primaryKey,  the primary key or unique key referenced by this foreign key
     * @deprecated
     * @see #setReferenceKey(KeyRecord)
     */
    public void setPrimaryKey(KeyRecord primaryKey) {
		this.setReferenceKey(primaryKey);
	}

    /**
     * WARNING prior to validation this method will return a potentially fully-qualified name
     * after resolving it will return an unqualified name
     * @return
     */
	public String getReferenceTableName() {
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