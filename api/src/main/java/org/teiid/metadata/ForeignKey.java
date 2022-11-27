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

    public static final String ALLOW_JOIN = AbstractMetadataRecord.RELATIONAL_PREFIX + "allow-join"; //$NON-NLS-1$

    public ForeignKey() {
        super(Type.Foreign);
    }

    public String getUniqueKeyID() {
        return uniqueKeyID;
    }

    /**
     * @param keyID
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
     * @param primaryKey the primary key or unique key referenced by this foreign key
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
     * @param primaryKey the primary key or unique key referenced by this foreign key
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