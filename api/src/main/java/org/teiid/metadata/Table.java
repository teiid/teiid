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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.AbstractMetadataRecord.DataModifiable;
import org.teiid.metadata.AbstractMetadataRecord.Modifiable;

public class Table extends ColumnSet<Schema> implements Modifiable, DataModifiable {

    public static final int UNKNOWN_CARDINALITY = -1;
    private static final long serialVersionUID = 4891356771125218672L;

    public enum Type {
        Table,
        View,
        Document,
        @Deprecated
        XmlMappingClass,
        @Deprecated
        XmlStagingTable,
        MaterializedTable,
        /** Temporary from a Teiid Perspective - physical will not have a parent set */
        TemporaryTable {
            @Override
            public String toString() {
                return "TEMPORARY TABLE"; //$NON-NLS-1$
            }
        }
    }

    public static enum TriggerEvent {
        INSERT,
        UPDATE,
        DELETE
    }

    static final int asInt(long f) {
        if (f == UNKNOWN_CARDINALITY || f < 0) {
            return UNKNOWN_CARDINALITY;
        } else if (f <= Integer.MAX_VALUE) {
            return (int)f;
        }
        //NaN 0x7ffffffff will map to -1 (unknown)
        return Float.floatToRawIntBits(f) | 0x80000000;
    }

    static final float asFloat(int i) {
        if (i >= UNKNOWN_CARDINALITY) {
            return i;
        }
        return Float.intBitsToFloat(i & 0x7fffffff);
    }

    private volatile int cardinality = UNKNOWN_CARDINALITY;
    private Type tableType;
    private boolean isVirtual;
    private boolean isSystem;
    private boolean isMaterialized;
    private boolean supportsUpdate;
    private List<ForeignKey> foreignKeys = new ArrayList<ForeignKey>(2);
    private List<KeyRecord> indexes = new ArrayList<KeyRecord>(2);
    private List<KeyRecord> functionBasedIndexes = new ArrayList<KeyRecord>(2);
    private List<KeyRecord> uniqueKeys = new ArrayList<KeyRecord>(2);
    private List<KeyRecord> accessPatterns = new ArrayList<KeyRecord>(2);
    private KeyRecord primaryKey;

    //table information
    private Map<String, Trigger> triggers = new LinkedHashMap<String, Trigger>();

    //view information
    private volatile String selectTransformation;
    private volatile String insertPlan;
    private volatile String updatePlan;
    private volatile String deletePlan;
    private volatile boolean insertPlanEnabled;
    private volatile boolean updatePlanEnabled;
    private volatile boolean deletePlanEnabled;
    private Table materializedStageTable;
    private Table materializedTable;

    //XML specific
    private List<String> bindings;
    private List<String> schemaPaths;
    private String resourcePath;

    private volatile transient long lastModified;
    private volatile transient long lastDataModification;

    /**
     * Used in xml document models mapping classes to represent input parameters
     * @return
     */
    @Deprecated
    public List<String> getBindings() {
        return bindings;
    }

    @Deprecated
    public void setBindings(List<String> bindings) {
        this.bindings = bindings;
    }

    /**
     * If the table represents an xml document model with associated schemas in the vdb, this
     * will return a list of the file paths
     * @return
     * @deprecated
     */
    @Deprecated
    public List<String> getSchemaPaths() {
        return schemaPaths;
    }

    @Deprecated
    public void setSchemaPaths(List<String> schemaPaths) {
        this.schemaPaths = schemaPaths;
    }

    public int getCardinality() {
        if (cardinality >= UNKNOWN_CARDINALITY) {
            return cardinality;
        }
        return Integer.MAX_VALUE;
    }

    public float getCardinalityAsFloat() {
        return asFloat(cardinality);
    }

    public boolean isVirtual() {
        return isVirtual;
    }

    public boolean isMaterialized() {
        return isMaterialized;
    }

    public boolean isPhysical() {
        return !isVirtual();
    }

    public boolean isSystem() {
        return isSystem;
    }

    public Type getTableType() {
        if (tableType == null) {
            return Type.Table;
        }
        return tableType;
    }

    public boolean supportsUpdate() {
        return supportsUpdate;
    }

    /**
     * @param i
     */
    public void setCardinality(int i) {
        cardinality = asInt(i);
    }

    public void setCardinality(long f) {
        cardinality = asInt(f);
    }

    /**
     * @param i
     */
    public void setTableType(Type i) {
        tableType = i;
    }

    /**
     * @param b
     */
    public void setSupportsUpdate(boolean b) {
        supportsUpdate = b;
    }

    /**
     * @param b
     */
    public void setVirtual(boolean b) {
        isVirtual = b;
    }

    /**
     * @param isMaterialized The isMaterialized to set.
     * @since 4.2
     */
    public void setMaterialized(boolean isMaterialized) {
        this.isMaterialized = isMaterialized;
    }

    /**
     * @param b
     */
    public void setSystem(boolean b) {
        isSystem = b;
    }

    public String getInsertPlan() {
        return insertPlan;
    }

    public String getUpdatePlan() {
        return updatePlan;
    }

    public String getDeletePlan() {
        return deletePlan;
    }

    public void setInsertPlan(String insertPlan) {
        this.insertPlan = DataTypeManager.getCanonicalString(insertPlan);
        this.insertPlanEnabled = true;
    }

    public void setUpdatePlan(String updatePlan) {
        this.updatePlan = DataTypeManager.getCanonicalString(updatePlan);
        this.updatePlanEnabled = true;
    }

    public void setDeletePlan(String deletePlan) {
        this.deletePlan = DataTypeManager.getCanonicalString(deletePlan);
        this.deletePlanEnabled = true;
    }

    public List<ForeignKey> getForeignKeys() {
        return this.foreignKeys;
    }

    public void setForeignKeys(List<ForeignKey> foreignKeys) {
        this.foreignKeys = foreignKeys;
    }

    @Deprecated
    public void setForiegnKeys(List<ForeignKey> foriegnKeys) {
        this.foreignKeys = foriegnKeys;
    }

    public List<KeyRecord> getIndexes() {
        return this.indexes;
    }

    public void setIndexes(List<KeyRecord> indexes) {
        this.indexes = indexes;
    }

    public List<KeyRecord> getFunctionBasedIndexes() {
        return functionBasedIndexes;
    }

    public void setFunctionBasedIndexes(List<KeyRecord> functionBasedIndexes) {
        this.functionBasedIndexes = functionBasedIndexes;
    }

    public List<KeyRecord> getUniqueKeys() {
        return this.uniqueKeys;
    }

    public void setUniqueKeys(List<KeyRecord> uniqueKeys) {
        this.uniqueKeys = uniqueKeys;
    }

    public List<KeyRecord> getAccessPatterns() {
        return this.accessPatterns;
    }

    public void setAccessPatterns(List<KeyRecord> accessPatterns) {
        this.accessPatterns = accessPatterns;
    }

    public KeyRecord getPrimaryKey() {
        return this.primaryKey;
    }

    public void setPrimaryKey(KeyRecord primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getSelectTransformation() {
        return selectTransformation;
    }

    public void setSelectTransformation(String selectTransformation) {
        this.selectTransformation = DataTypeManager.getCanonicalString(selectTransformation);
    }

    public Table getMaterializedStageTable() {
        return materializedStageTable;
    }

    public Table getMaterializedTable() {
        return materializedTable;
    }

    public void setMaterializedStageTable(Table materializedStageTable) {
        this.materializedStageTable = materializedStageTable;
    }

    public void setMaterializedTable(Table materializedTable) {
        this.materializedTable = materializedTable;
    }

    public void setResourcePath(String resourcePath) {
        this.resourcePath = DataTypeManager.getCanonicalString(resourcePath);
    }

    public String getResourcePath() {
        return resourcePath;
    }

    public Collection<KeyRecord> getAllKeys() {
        Collection<KeyRecord> keys = new LinkedList<KeyRecord>();
        if (getPrimaryKey() != null) {
            keys.add(getPrimaryKey());
        }
        keys.addAll(getForeignKeys());
        keys.addAll(getAccessPatterns());
        keys.addAll(getIndexes());
        keys.addAll(getUniqueKeys());
        return keys;
    }

    @Override
    public void addColumn(Column column) {
        super.addColumn(column);
        column.setParent(this);
    }

    public long getLastDataModification() {
        return lastDataModification;
    }

    public long getLastModified() {
        return lastModified;
    }

    public void setLastDataModification(long lastDataModification) {
        this.lastDataModification = lastDataModification;
    }

    public void setLastModified(long lastModified) {
        this.lastModified = lastModified;
    }

    public void setTableStats(TableStats stats) {
        if (stats.getCardinality() != null) {
            setCardinality(stats.getCardinality().longValue());
        }
    }

    public boolean isDeletePlanEnabled() {
        return deletePlanEnabled;
    }

    public boolean isInsertPlanEnabled() {
        return insertPlanEnabled;
    }

    public boolean isUpdatePlanEnabled() {
        return updatePlanEnabled;
    }

    public void setInsertPlanEnabled(boolean insertPlanEnabled) {
        this.insertPlanEnabled = insertPlanEnabled;
    }

    public void setDeletePlanEnabled(boolean deletePlanEnabled) {
        this.deletePlanEnabled = deletePlanEnabled;
    }

    public void setUpdatePlanEnabled(boolean updatePlanEnabled) {
        this.updatePlanEnabled = updatePlanEnabled;
    }

    private void readObject(java.io.ObjectInputStream in)
    throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        if (this.functionBasedIndexes == null) {
            this.functionBasedIndexes = new ArrayList<KeyRecord>(2);
        }
    }

    @Override
    public String getFullName() {
        if (this.tableType == Type.TemporaryTable && !this.isVirtual) {
            return this.getName();
        }
        return super.getFullName();
    }

    public Map<String, Trigger> getTriggers() {
        return triggers;
    }
}