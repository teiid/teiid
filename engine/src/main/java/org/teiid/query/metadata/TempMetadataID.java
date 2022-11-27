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

package org.teiid.query.metadata;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.teiid.core.util.LRUCache;
import org.teiid.metadata.AbstractMetadataRecord.DataModifiable;
import org.teiid.metadata.AbstractMetadataRecord.Modifiable;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Symbol;

/**
 * This class represents a temporary metadata ID.  A temporary metadata ID
 * does not exist in a real metadata source.  Rather, it is used temporarily
 * in context of processing a single query.  This metadata ID can be used to
 * represent either a group or an element depending on the constructor used.
 *
 * TODO: we should be using the real metadata objects, but internal and
 * designer legacy keep us on the temp framework
 */
public class TempMetadataID implements Serializable, Modifiable, DataModifiable {

    private static final long serialVersionUID = -1879211827339120135L;
    private static final int LOCAL_CACHE_SIZE = 8;

    private static final int MOD_COUNT_FOR_COST_UPDATE = 8;

    public static class TableData {
        Collection<TempMetadataID> accessPatterns;
        List<TempMetadataID> elements;
        long cardinality = QueryMetadataInterface.UNKNOWN_CARDINALITY;
        List<TempMetadataID> primaryKey;
        QueryNode queryNode;
        Map<Object, Object> localCache;
        volatile CacheHint cacheHint;
        long cacheHintUpdated;
        List<List<TempMetadataID>> keys;
        List<TempMetadataID> indexes;
        volatile long lastDataModification;
        volatile long lastModified = System.currentTimeMillis();
        int modCount;
        private LinkedHashMap<Expression, Integer> functionBasedExpressions;
        private Object model;
        List<TempMetadataID> foreignKeys;

        public long getLastDataModification() {
            return lastDataModification;
        }

        public void removed() {
            this.lastModified = -1;
        }

        public void dataModified(long updateCount) {
            if (updateCount == 0) {
                return;
            }
            long ts = System.currentTimeMillis();
            modCount += updateCount;
            if (modCount > MOD_COUNT_FOR_COST_UPDATE) {
                this.lastModified = ts;
                modCount = 0;
            }
            this.lastDataModification = ts;
        }

        public long getLastModified() {
            return lastModified;
        }

        public void setFunctionBasedExpressions(
                LinkedHashMap<Expression, Integer> newExprs) {
            this.functionBasedExpressions = newExprs;
        }

        public LinkedHashMap<Expression, Integer> getFunctionBasedExpressions() {
            return functionBasedExpressions;
        }

        public void setModel(Object mid) {
            this.model = mid;
        }

        public Object getModel() {
            return model;
        }

        public synchronized boolean updateCacheHint(long time) {
            if (time >= cacheHintUpdated) {
                cacheHintUpdated = time;
                return true;
            }
            return false;
        }

    }

    private static TableData DUMMY_DATA = new TableData();

    public enum Type {
        VIRTUAL,
        TEMP,
        SCALAR,
        INDEX,
        FOREIGN_KEY
    }

    private String ID;      // never null, upper cased fully-qualified string
    private String name;
    private Type metadataType = Type.VIRTUAL;
    private Object originalMetadataID;

    private TableData data;

    //Column metadata
    private int position;
    private Class<?> type;     // type of this element, only for element
    private boolean autoIncrement;
    private boolean notNull;
    private boolean updatable;

    /**
     * Constructor for group form of metadata ID.
     * @param ID Fully-qualified, upper-case name of ID
     * @param elements List of TempMetadataID representing elements
     */
    public TempMetadataID(String ID, List<TempMetadataID> elements) {
        this(ID, elements, Type.VIRTUAL);
    }

    /**
     * Constructor for group form of metadata ID.
     * @param ID Fully-qualified, upper-case name of ID
     * @param elements List of TempMetadataID representing elements
     * @param type of group
     */
    public TempMetadataID(String ID, List<TempMetadataID> elements, Type type) {
        this.data = new TableData();
        this.ID = ID;
        this.data.elements = elements;
        int pos = 1;
        for (TempMetadataID tempMetadataID : elements) {
            tempMetadataID.setPosition(pos++);
        }
        this.name = ID;
        this.metadataType = type;
    }

    /**
     * Constructor for element form of metadata ID.
     * @param ID Fully-qualified, upper-case name of ID
     * @param type Type of elements List of TempMetadataID representing elements
     */
    public TempMetadataID(String ID, Class<?> type) {
        this.ID = ID;
        this.type = type;
    }

    /**
     * Constructor for element form of metadata ID with the underlying element.
     * @param ID Fully-qualified, upper-case name of ID
     * @param type Type of elements List of TempMetadataID representing elements
     * @param metadataID the orginal metadataID
     */
    public TempMetadataID(String ID, Class<?> type, Object metadataID) {
        this.ID = ID;
        this.type = type;
        this.originalMetadataID = metadataID;
    }

    @Override
    public long getLastDataModification() {
        return getTableData().getLastDataModification();
    }

    @Override
    public long getLastModified() {
        return getTableData().getLastModified();
    }

    /**
     * Get ID value
     * @return ID value
     */
    public String getID() {
        return this.ID;
    }

    /**
     * Get type - only valid for elements
     * @return Type for elements, null for groups
     */
    public Class<?> getType() {
        return this.type;
    }

    /**
     * Get elements - only valid for groups
     * @return List of TempMetadataID for groups, null for elements
     */
    public List<TempMetadataID> getElements() {
        return this.getTableData().elements;
    }

    /**
     * add a element to the temp table.
     * @param elem
     */
    protected void addElement(TempMetadataID elem) {
        if (this.getTableData().elements != null) {
            this.getTableData().elements.add(elem);
            elem.setPosition(this.getTableData().elements.size());
        }
        if (this.getTableData().localCache != null) {
            this.getTableData().localCache.clear();
        }
    }

    /**
     * Check whether this group is virtual
     * @return True if virtual
     */
    public boolean isVirtual() {
        return metadataType == Type.VIRTUAL;
    }

    /**
     * Whether it is a temporary table
     * @return
     * @since 5.5
     */
    public boolean isTempTable() {
        return this.metadataType == Type.TEMP;
    }

    /**
     * Return string representation of ID
     * @return String representation
     */
    public String toString() {
        return ID;
    }

    /**
     * Compare this temp metadata ID with another object.
     * @return True if obj is another TempMetadataID with same ID value
     */
    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        }

        if(!(obj instanceof TempMetadataID)) {
            return false;
        }
        return this.getID().equals( ((TempMetadataID) obj).getID());
    }

    /**
     * Return hash code
     * @return Hash code value for object
     */
    public int hashCode() {
        return this.ID.hashCode();
    }

    public void setOriginalMetadataID(Object metadataId) {
        this.originalMetadataID = metadataId;
    }

    /**
     * @return Returns the originalMetadataID.
     * @since 4.3
     */
    public Object getOriginalMetadataID() {
        return this.originalMetadataID;
    }

    public Collection<TempMetadataID> getAccessPatterns() {
        if (this.getTableData().accessPatterns == null) {
            return Collections.emptyList();
        }
        return this.getTableData().accessPatterns;
    }

    public void setAccessPatterns(Collection<TempMetadataID> accessPatterns) {
        this.getTableData().accessPatterns = accessPatterns;
    }

    public long getCardinality() {
        return this.getTableData().cardinality;
    }

    public void setCardinality(long cardinality) {
        this.getTableData().cardinality = cardinality;
    }

    public void setTempTable(boolean isTempTable) {
        if (isTempTable) {
            this.metadataType = Type.TEMP;
        } else {
            this.metadataType = Type.VIRTUAL;
        }
    }

    Object getProperty(Object key) {
        if (this.getTableData().localCache != null) {
            return this.getTableData().localCache.get(key);
        }
        return null;
    }

    Object setProperty(Object key, Object value) {
        if (this.getTableData().localCache == null) {
            this.getTableData().localCache = Collections.synchronizedMap(new LRUCache<Object, Object>(LOCAL_CACHE_SIZE));
        }
        return this.getTableData().localCache.put(key, value);
    }

    public boolean isScalarGroup() {
        return this.metadataType == Type.SCALAR;
    }

    public void setMetadataType(Type metadataType) {
        this.metadataType = metadataType;
    }

    public Type getMetadataType() {
        return metadataType;
    }

    public List<TempMetadataID> getPrimaryKey() {
        return getTableData().primaryKey;
    }

    public void setPrimaryKey(List<TempMetadataID> primaryKey) {
        this.getTableData().primaryKey = primaryKey;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public QueryNode getQueryNode() {
        return getTableData().queryNode;
    }

    public void setQueryNode(QueryNode queryNode) {
        this.getTableData().queryNode = queryNode;
    }

    public CacheHint getCacheHint() {
        return getTableData().cacheHint;
    }

    public void setCacheHint(CacheHint cacheHint) {
        this.getTableData().cacheHint = cacheHint;
    }

    public List<TempMetadataID> getIndexes() {
        return getTableData().indexes;
    }

    public List<TempMetadataID> getForeignKeys() {
        return getTableData().foreignKeys;
    }

    public void addIndex(Object originalMetadataId, List<TempMetadataID> index) {
        if (this.getTableData().indexes == null) {
            this.getTableData().indexes = new LinkedList<TempMetadataID>();
        }
        TempMetadataID id = new TempMetadataID(ID, Collections.EMPTY_LIST, Type.INDEX);
        id.getTableData().elements = index;
        id.setOriginalMetadataID(originalMetadataId);
        this.getTableData().indexes.add(id);
    }

    public void addForeignKey(Object originalMetadataId, Object pk, List<TempMetadataID> cols) {
        if (this.getTableData().foreignKeys == null) {
            this.getTableData().foreignKeys = new LinkedList<TempMetadataID>();
        }
        TempMetadataID id = new TempMetadataID(ID, Collections.EMPTY_LIST, Type.FOREIGN_KEY);
        id.getTableData().elements = cols;
        id.setOriginalMetadataID(pk);
        this.getTableData().foreignKeys.add(id);
    }

    public List<List<TempMetadataID>> getUniqueKeys() {
        return getTableData().keys;
    }

    public void addUniqueKey(List<TempMetadataID> key) {
        if (this.getTableData().keys == null) {
            this.getTableData().keys = new LinkedList<List<TempMetadataID>>();
        }
        this.getTableData().keys.add(key);
    }

    public TableData getTableData() {
        if (data == null) {
            return DUMMY_DATA;
        }
        return data;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public void setAutoIncrement(boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    public boolean isNotNull() {
        return notNull;
    }

    public void setNotNull(boolean notNull) {
        this.notNull = notNull;
    }

    public void setUpdatable(boolean updatable) {
        this.updatable = updatable;
    }

    public boolean isUpdatable() {
        return updatable;
    }

    public String getName() {
        if (this.name == null) {
            this.name = Symbol.getShortName(this.ID);
        }
        return this.name;
    }

}
