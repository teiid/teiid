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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.StringUtil;
import org.teiid.metadata.Column;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Table;
import org.teiid.query.QueryPlugin;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.TempMetadataID.Type;
import org.teiid.query.sql.symbol.Expression;


/**
 * <p>This is an adapter class, it contains another instance of
 * QueryMetadataInterface as well as a TempMetadataStore.  It defers to
 * either one of these when appropriate.
 *
 * <p>When a metadataID Object is requested for a group or element name, this
 * will first check the QueryMetadataInterface.  If an ID wasn't found there,
 * it will then check the TempMetadataStore.
 *
 * <p>For methods that take a metadataID arg, this class may check whether it
 * is a TempMetadataID or not and react accordingly.
 */
public class TempMetadataAdapter extends BasicQueryMetadataWrapper {

    private static final String SEPARATOR = "."; //$NON-NLS-1$
    public static final TempMetadataID TEMP_MODEL = new TempMetadataID("__TEMP__", Collections.EMPTY_LIST); //$NON-NLS-1$

    private TempMetadataStore tempStore;
    private Map<Object, Object> materializationTables;
    private Map<Object, QueryNode> queryNodes;
    private boolean session;

    public TempMetadataAdapter(QueryMetadataInterface metadata, TempMetadataStore tempStore) {
        super(metadata);
        this.tempStore = tempStore;
    }

    public TempMetadataAdapter(QueryMetadataInterface metadata, TempMetadataStore tempStore, Map<Object, Object> materializationTables, Map<Object, QueryNode> queryNodes) {
        super(metadata);
        this.tempStore = tempStore;
        this.materializationTables = materializationTables;
        this.queryNodes = queryNodes;
    }

    public boolean isSession() {
        return session;
    }

    public void setSession(boolean session) {
        this.session = session;
    }

    public QueryMetadataInterface getSessionMetadata() {
        if (isSession()) {
            TempMetadataAdapter tma = new TempMetadataAdapter(new BasicQueryMetadata(), this.tempStore);
            tma.session = true;
            return tma;
        }
        return this.actualMetadata.getSessionMetadata();
    }

    @Override
    protected QueryMetadataInterface createDesignTimeMetadata() {
        if (isSession()) {
            return new TempMetadataAdapter(this.actualMetadata.getDesignTimeMetadata(), new TempMetadataStore());
        }
        return new TempMetadataAdapter(this.actualMetadata.getDesignTimeMetadata(), tempStore, materializationTables, queryNodes);
    }

    public TempMetadataStore getMetadataStore() {
        return this.tempStore;
    }

    public QueryMetadataInterface getMetadata() {
        return this.actualMetadata;
    }

    /**
     * Check metadata first, then check temp groups if not found
     */
    public Object getElementID(String elementName)
        throws TeiidComponentException, QueryMetadataException {

        Object tempID = null;
        try {
            tempID = this.actualMetadata.getElementID(elementName);
        } catch (QueryMetadataException e) {
            //ignore
        }

        if (tempID == null){
            tempID = this.tempStore.getTempElementID(elementName);
        }

        if(tempID != null) {
            return tempID;
        }
        throw new QueryMetadataException(QueryPlugin.Event.TEIID30350, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30350, elementName));
    }

    /**
     * Check metadata first, then check temp groups if not found
     */
    public Object getGroupID(String groupName)
        throws TeiidComponentException, QueryMetadataException {

        Object tempID = null;
        try {
            tempID = this.actualMetadata.getGroupID(groupName);
        } catch (QueryMetadataException e) {
            //ignore
        }

        if (tempID == null){
            tempID = this.tempStore.getTempGroupID(groupName);
        }

        if(tempID != null) {
            return tempID;
        }
        throw new QueryMetadataException(QueryPlugin.Event.TEIID30351, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30351, groupName));
    }

    @Override
    public Collection getGroupsForPartialName(String partialGroupName)
            throws TeiidComponentException, QueryMetadataException {
        Collection groups = super.getGroupsForPartialName(partialGroupName);
        ArrayList<String> allGroups = new ArrayList<String>(groups);
        for (Map.Entry<String, TempMetadataID> entry : tempStore.getData().entrySet()) {
            String name = entry.getKey();
            if (StringUtil.endsWithIgnoreCase(name, partialGroupName)
                    //don't want to match tables by anything less than the full name,
                    //since this should be a temp or a global temp and in the latter case there's a real metadata entry
                    //alternatively we could check to see if the name is already in the result list
                    && (name.length() == partialGroupName.length() || (entry.getValue().getMetadataType() != Type.TEMP && name.length() > partialGroupName.length() && name.charAt(name.length() - partialGroupName.length() - 1) == '.'))) {
                allGroups.add(name);
            }
        }
        return allGroups;
    }

    public Object getModelID(Object groupOrElementID)
        throws TeiidComponentException, QueryMetadataException {

        groupOrElementID = getActualMetadataId(groupOrElementID);

        if(groupOrElementID instanceof TempMetadataID) {
            TempMetadataID tid = (TempMetadataID)groupOrElementID;
            Object oid = tid.getOriginalMetadataID();
            if (oid instanceof Procedure) {
                return actualMetadata.getModelID(oid);
            }
            return TempMetadataAdapter.TEMP_MODEL;
        }
        //special handling for global temp tables
        Object id = groupOrElementID;
        if (groupOrElementID instanceof Column) {
            id = ((Column)id).getParent();
        }
        if (id instanceof Table) {
            Table t = (Table)id;
            if (t.getTableType() == Table.Type.TemporaryTable && t.isVirtual()) {
                return TempMetadataAdapter.TEMP_MODEL;
            }
        }
         return this.actualMetadata.getModelID(groupOrElementID);
    }

    // SPECIAL: Override for temp groups
    public String getFullName(Object metadataID)
        throws TeiidComponentException, QueryMetadataException {

        if(metadataID instanceof TempMetadataID) {
            return ((TempMetadataID)metadataID).getID();
        }
        return this.actualMetadata.getFullName(metadataID);
    }

    @Override
    public String getName(Object metadataID) throws TeiidComponentException,
            QueryMetadataException {
        if(metadataID instanceof TempMetadataID) {
            TempMetadataID tid = (TempMetadataID)metadataID;
            return tid.getName();
        }
        return this.actualMetadata.getName(metadataID);
    }

    // SPECIAL: Override for temp groups
    public List getElementIDsInGroupID(Object groupID)
        throws TeiidComponentException, QueryMetadataException {

        groupID = getActualMetadataId(groupID);

        if(groupID instanceof TempMetadataID) {
            return new ArrayList<Object>(((TempMetadataID)groupID).getElements());
        }
        return this.actualMetadata.getElementIDsInGroupID(groupID);
    }

    // SPECIAL: Override for temp groups
    public Object getGroupIDForElementID(Object elementID)
        throws TeiidComponentException, QueryMetadataException {

        if(elementID instanceof TempMetadataID) {
            String elementName = ((TempMetadataID)elementID).getID();
            String groupName = elementName.substring(0, elementName.lastIndexOf(SEPARATOR));
            return this.tempStore.getTempGroupID(groupName);
        }
        return this.actualMetadata.getGroupIDForElementID(elementID);
    }

    // SPECIAL: Override for temp groups
    public String getElementRuntimeTypeName(Object elementID)
        throws TeiidComponentException, QueryMetadataException {

        if(elementID instanceof TempMetadataID) {
            TempMetadataID tempID = (TempMetadataID)elementID;
            if (tempID.getType() != null) {
                return DataTypeManager.getDataTypeName( tempID.getType() );
            }
            throw new AssertionError("No type set for element " + elementID); //$NON-NLS-1$
        }
        return this.actualMetadata.getElementRuntimeTypeName(elementID);
    }

    public String getDefaultValue(Object elementID)
        throws TeiidComponentException, QueryMetadataException {

        if(elementID instanceof TempMetadataID) {
            return null;
        }
        return this.actualMetadata.getDefaultValue(elementID);
    }

    public Object getMaximumValue(Object elementID) throws TeiidComponentException, QueryMetadataException {
        if (elementID instanceof TempMetadataID) {
            TempMetadataID id = (TempMetadataID)elementID;
            elementID = id.getOriginalMetadataID();
            if (elementID == null) {
                return null;
            }
        }
        return this.actualMetadata.getMaximumValue(elementID);
    }

    public Object getMinimumValue(Object elementID) throws TeiidComponentException, QueryMetadataException {
        if (elementID instanceof TempMetadataID) {
            TempMetadataID id = (TempMetadataID)elementID;
            elementID = id.getOriginalMetadataID();
            if (elementID == null) {
                return null;
            }
        }
        return this.actualMetadata.getMinimumValue(elementID);
    }

    /**
     * @see org.teiid.query.metadata.QueryMetadataInterface#getDistinctValues(java.lang.Object)
     */
    public float getDistinctValues(Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof TempMetadataID) {
            return -1;
        }
        return this.actualMetadata.getDistinctValues(elementID);
    }

    /**
     * @see org.teiid.query.metadata.QueryMetadataInterface#getNullValues(java.lang.Object)
     */
    public float getNullValues(Object elementID) throws TeiidComponentException, QueryMetadataException {
        if (elementID instanceof TempMetadataID) {
            TempMetadataID id = (TempMetadataID)elementID;
            elementID = id.getOriginalMetadataID();
            if (elementID == null) {
                return -1;
            }
        }
        return this.actualMetadata.getNullValues(elementID);
    }

    public QueryNode getVirtualPlan(Object groupID)
        throws TeiidComponentException, QueryMetadataException {

        if (this.queryNodes != null) {
            QueryNode node = this.queryNodes.get(groupID);
            if (node != null) {
                return node;
            }
        }

        if(groupID instanceof TempMetadataID && !(actualMetadata instanceof TempMetadataAdapter)) {
            TempMetadataID tid = (TempMetadataID)groupID;
            QueryNode queryNode = tid.getQueryNode();
            if (queryNode != null) {
                return queryNode;
            }
            throw new QueryMetadataException(QueryPlugin.Event.TEIID31265, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31265, tid.getName()));
        }
           return this.actualMetadata.getVirtualPlan(groupID);
    }

    // SPECIAL: Override for temp groups
    public boolean isVirtualGroup(Object groupID)
        throws TeiidComponentException, QueryMetadataException {

        if(groupID instanceof TempMetadataID) {
            return ((TempMetadataID)groupID).isVirtual();
        }
        return this.actualMetadata.isVirtualGroup(groupID);
    }

    /**
     * @see org.teiid.query.metadata.QueryMetadataInterface#hasMaterialization(java.lang.Object)
     * @since 4.2
     */
    public boolean hasMaterialization(Object groupID)
        throws TeiidComponentException, QueryMetadataException {

        // check if any dynamic materialization tables are defined
        if (this.materializationTables != null && this.materializationTables.containsKey(groupID)) {
            return true;
        }

        if(groupID instanceof TempMetadataID && !(actualMetadata instanceof TempMetadataAdapter)) {
            return false;
        }

        return this.actualMetadata.hasMaterialization(groupID);
    }

    /**
     * @see org.teiid.query.metadata.QueryMetadataInterface#getMaterialization(java.lang.Object)
     * @since 4.2
     */
    public Object getMaterialization(Object groupID)
        throws TeiidComponentException, QueryMetadataException {

        // check if any dynamic materialization tables are defined
        if (this.materializationTables != null) {
            Object result = this.materializationTables.get(groupID);
            if (result != null) {
                return result;
            }
        }

        if(groupID instanceof TempMetadataID && !(actualMetadata instanceof TempMetadataAdapter)) {
            return null;
        }

        return this.actualMetadata.getMaterialization(groupID);
    }

    /**
     * @see org.teiid.query.metadata.QueryMetadataInterface#getMaterializationStage(java.lang.Object)
     * @since 4.2
     */
    public Object getMaterializationStage(Object groupID)
        throws TeiidComponentException, QueryMetadataException {

        if(groupID instanceof TempMetadataID) {
            return null;
        }

        // we do not care about the dynamic materialization tables here as they are loaded dynamically.
        return this.actualMetadata.getMaterializationStage(groupID);
    }

    public boolean isVirtualModel(Object modelID)
        throws TeiidComponentException, QueryMetadataException {

        if(modelID.equals(TEMP_MODEL)) {
            return false;
        }
        return this.actualMetadata.isVirtualModel(modelID);
    }

    // --------------------- Implement OptimizerMetadata -------------------

    public boolean elementSupports(Object elementID, int supportConstant)
        throws TeiidComponentException, QueryMetadataException {

        if (elementID instanceof TempMetadataID) {
            TempMetadataID id = (TempMetadataID)elementID;

            switch(supportConstant) {
                case SupportConstants.Element.SEARCHABLE_LIKE:   return true;
                case SupportConstants.Element.SEARCHABLE_COMPARE:return true;
                case SupportConstants.Element.SEARCHABLE_EQUALITY:return true;
                case SupportConstants.Element.SELECT:            return true;
                case SupportConstants.Element.NULL: {
                    if (id.isNotNull()) {
                        return false;
                    }
                    if (id.isTempTable()) {
                        return true;
                    }
                    break;
                }
                case SupportConstants.Element.AUTO_INCREMENT:     return id.isAutoIncrement();
                case SupportConstants.Element.UPDATE:             return id.isTempTable() || id.isUpdatable();

            }

            // If this is a temp table column or real metadata is unknown, return hard-coded values
            elementID = id.getOriginalMetadataID();
            if(elementID == null || id.isTempTable()) {
                switch(supportConstant) {
                    case SupportConstants.Element.NULL:              return true;
                    case SupportConstants.Element.SIGNED:            return true;
                }

                return false;
            }
        }

        return this.actualMetadata.elementSupports(elementID, supportConstant);
    }

    /**
     * @see org.teiid.query.metadata.QueryMetadataInterface#getIndexesInGroup(java.lang.Object)
     */
    public Collection getIndexesInGroup(Object groupID)
        throws TeiidComponentException, QueryMetadataException {

        groupID = getActualMetadataId(groupID);

        if(groupID instanceof TempMetadataID) {
            List<TempMetadataID> result = ((TempMetadataID)groupID).getIndexes();
            if (result == null) {
                return Collections.emptyList();
            }
            return result;
        }
        return this.actualMetadata.getIndexesInGroup(groupID);
    }

    public Collection getUniqueKeysInGroup(Object groupID)
        throws TeiidComponentException, QueryMetadataException {

        groupID = getActualMetadataId(groupID);

        if(groupID instanceof TempMetadataID) {
            LinkedList<List<TempMetadataID>> result = new LinkedList<List<TempMetadataID>>();
            TempMetadataID id = (TempMetadataID)groupID;
            if (id.getPrimaryKey() != null) {
                result.add(id.getPrimaryKey());
            }
            if (id.getUniqueKeys() != null) {
                result.addAll(id.getUniqueKeys());
            }
            return result;
        }
        return this.actualMetadata.getUniqueKeysInGroup(groupID);
    }

    public Collection getForeignKeysInGroup(Object groupID)
        throws TeiidComponentException, QueryMetadataException {

        groupID = getActualMetadataId(groupID);

        if(groupID instanceof TempMetadataID) {
            if(groupID instanceof TempMetadataID) {
                List<TempMetadataID> result = ((TempMetadataID)groupID).getForeignKeys();
                if (result == null) {
                    return Collections.emptyList();
                }
                return result;
            }
        }
        return this.actualMetadata.getForeignKeysInGroup(groupID);
    }

    public List getElementIDsInKey(Object keyID)
        throws TeiidComponentException, QueryMetadataException {

        if (keyID instanceof List) {
            return (List)keyID;
        }

        if (keyID instanceof TempMetadataID) {
            TempMetadataID id = (TempMetadataID)keyID;
            if (id.getMetadataType() == Type.INDEX || id.getMetadataType() == Type.FOREIGN_KEY) {
                return id.getElements();
            }
        }

        return this.actualMetadata.getElementIDsInKey(keyID);
    }

    @Override
    public Object getPrimaryKeyIDForForeignKeyID(Object foreignKeyID)
            throws TeiidComponentException, QueryMetadataException {
        if (foreignKeyID instanceof TempMetadataID) {
            TempMetadataID id = (TempMetadataID)foreignKeyID;
            if (id.getMetadataType() == Type.FOREIGN_KEY) {
                return id.getOriginalMetadataID();
            }
        }
        return super.getPrimaryKeyIDForForeignKeyID(foreignKeyID);
    }

    public boolean groupSupports(Object groupID, int groupConstant)
        throws TeiidComponentException, QueryMetadataException {

        groupID = getActualMetadataId(groupID);

        if(groupID instanceof TempMetadataID){
            return true;
        }

        return this.actualMetadata.groupSupports(groupID, groupConstant);
    }

    /**
     * @see org.teiid.query.metadata.QueryMetadataInterface#getVirtualDatabaseName()
     */
    public String getVirtualDatabaseName()
        throws TeiidComponentException, QueryMetadataException {

        return this.actualMetadata.getVirtualDatabaseName();
    }

    /**
     * @see org.teiid.query.metadata.QueryMetadataInterface#getAccessPatternsInGroup(Object)
     */
    public Collection getAccessPatternsInGroup(Object groupID)
        throws TeiidComponentException, QueryMetadataException {

        groupID = getActualMetadataId(groupID);

        if(groupID instanceof TempMetadataID) {
            TempMetadataID id = (TempMetadataID)groupID;

            return id.getAccessPatterns();
        }
        return this.actualMetadata.getAccessPatternsInGroup(groupID);
    }

    /**
     * @see org.teiid.query.metadata.QueryMetadataInterface#getElementIDsInAccessPattern(Object)
     */
    public List getElementIDsInAccessPattern(Object accessPattern)
        throws TeiidComponentException, QueryMetadataException {

        if (accessPattern instanceof TempMetadataID) {
            TempMetadataID id = (TempMetadataID)accessPattern;
            if (id.getElements() != null) {
                return id.getElements();
            }
            return Collections.EMPTY_LIST;
        }

        return this.actualMetadata.getElementIDsInAccessPattern(accessPattern);
    }

    public float getCardinality(Object groupID)
        throws TeiidComponentException, QueryMetadataException{

        groupID = getActualMetadataId(groupID);

        if(groupID instanceof TempMetadataID) {
           return ((TempMetadataID)groupID).getCardinality();
        }
        if (this.isSession() && groupID instanceof Table) {
            Table t = (Table)groupID;
            if (t.getTableType() == Table.Type.TemporaryTable && t.isVirtual()) {
                TempMetadataID id = this.tempStore.getTempGroupID(t.getName());
                if (id != null) {
                    return id.getCardinality();
                }
            }
        }

        return this.actualMetadata.getCardinality(groupID);
    }

    public Properties getExtensionProperties(Object metadataID)
        throws TeiidComponentException, QueryMetadataException {

        metadataID = getActualMetadataId(metadataID);

        if (metadataID instanceof TempMetadataID) {
            return TransformationMetadata.EMPTY_PROPS;
        }

        return actualMetadata.getExtensionProperties(metadataID);
    }

    public int getElementLength(Object elementID) throws TeiidComponentException, QueryMetadataException {
        if (elementID instanceof TempMetadataID) {
            TempMetadataID id = (TempMetadataID)elementID;
            Object origElementID = id.getOriginalMetadataID();
            if (origElementID == null) {
                String type = getElementRuntimeTypeName(elementID);
                if(type.equals(DataTypeManager.DefaultDataTypes.STRING)) {
                    return 255;
                }
                return 10;
            }
            elementID = origElementID;
        }

        return actualMetadata.getElementLength(elementID);
    }

    public int getPosition(Object elementID) throws TeiidComponentException, QueryMetadataException {
        if (elementID instanceof TempMetadataID) {
            return ((TempMetadataID)elementID).getPosition();
        }
        return actualMetadata.getPosition(elementID);
    }

    public int getPrecision(Object elementID) throws TeiidComponentException, QueryMetadataException {
        if (elementID instanceof TempMetadataID) {
            TempMetadataID id = (TempMetadataID)elementID;
            elementID = id.getOriginalMetadataID();
            if (elementID == null) {
                return 0;
            }
        }
        return actualMetadata.getPrecision(elementID);
    }

    public int getRadix(Object elementID) throws TeiidComponentException, QueryMetadataException {
        if (elementID instanceof TempMetadataID) {
            TempMetadataID id = (TempMetadataID)elementID;
            elementID = id.getOriginalMetadataID();
            if (elementID == null) {
                return 0;
            }
        }
        return actualMetadata.getRadix(elementID);
    }

    public int getScale(Object elementID) throws TeiidComponentException, QueryMetadataException {
        if (elementID instanceof TempMetadataID) {
            TempMetadataID id = (TempMetadataID)elementID;
            elementID = id.getOriginalMetadataID();
            if (elementID == null) {
                return 0;
            }
        }
        return actualMetadata.getScale(elementID);
    }

    /**
     * Get the native type name for the element.
     * @see org.teiid.query.metadata.QueryMetadataInterface#getNativeType(java.lang.Object)
     * @since 4.2
     */
    public String getNativeType(Object elementID) throws TeiidComponentException,
                                                QueryMetadataException {
        if (elementID instanceof TempMetadataID) {
            TempMetadataID id = (TempMetadataID)elementID;
            elementID = id.getOriginalMetadataID();
            if (elementID == null) {
                return ""; //$NON-NLS-1$
            }
        }

        return actualMetadata.getNativeType(elementID);
    }

    public boolean isProcedure(Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof TempMetadataID) {
            Object oid = ((TempMetadataID) elementID).getOriginalMetadataID();
            if (oid != null) {
                return actualMetadata.isProcedure(oid);
            }
            return false;
        }

        return actualMetadata.isProcedure(elementID);
    }

    public boolean isTemporaryTable(Object groupID) throws TeiidComponentException, QueryMetadataException {
        if(groupID instanceof TempMetadataID) {
            return ((TempMetadataID)groupID).isTempTable();
        }
        if (groupID instanceof Table) {
            Table t = (Table)groupID;
            if (t.getTableType() == Table.Type.TemporaryTable) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Object addToMetadataCache(Object metadataID, String key, Object value)
            throws TeiidComponentException, QueryMetadataException {
        if (metadataID instanceof TempMetadataID) {
            TempMetadataID tid = (TempMetadataID)metadataID;
            return tid.setProperty(key, value);
        }

        return this.actualMetadata.addToMetadataCache(metadataID, key, value);
    }

    @Override
    public Object getFromMetadataCache(Object metadataID, String key)
            throws TeiidComponentException, QueryMetadataException {
        if (metadataID instanceof TempMetadataID) {
            TempMetadataID tid = (TempMetadataID)metadataID;
            return tid.getProperty(key);
        }

        return this.actualMetadata.getFromMetadataCache(metadataID, key);
    }

    @Override
    public boolean isScalarGroup(Object groupID)
            throws TeiidComponentException, QueryMetadataException {
        if (groupID instanceof TempMetadataID) {
            TempMetadataID tid = (TempMetadataID)groupID;
            return tid.isScalarGroup();
        }

        return this.actualMetadata.isScalarGroup(groupID);
    }

    @Override
    public Object getPrimaryKey(Object metadataID) {

        metadataID = getActualMetadataId(metadataID);

        if (metadataID instanceof TempMetadataID) {
            return ((TempMetadataID)metadataID).getPrimaryKey();
        }
        return this.actualMetadata.getPrimaryKey(metadataID);
    }

    @Override
    public boolean isMultiSource(Object modelId) throws QueryMetadataException,
            TeiidComponentException {
        if (modelId instanceof TempMetadataID) {
            return false;
        }
        return this.actualMetadata.isMultiSource(modelId);
    }

    @Override
    public boolean isMultiSourceElement(Object elementId)
            throws QueryMetadataException, TeiidComponentException {
        if (elementId instanceof TempMetadataID) {
            return false;
        }
        return this.actualMetadata.isMultiSourceElement(elementId);
    }

    @Override
    public Map<Expression, Integer> getFunctionBasedExpressions(Object metadataID) {
        if (metadataID instanceof TempMetadataID) {
            return ((TempMetadataID)metadataID).getTableData().getFunctionBasedExpressions();
        }
        return super.getFunctionBasedExpressions(metadataID);
    }

    public static Object getActualMetadataId(Object id) {
        if (!(id instanceof TempMetadataID)) {
            return id;
        }
        TempMetadataID tid = (TempMetadataID)id;
        Object oid = tid.getOriginalMetadataID();
        if (oid != null && tid.getTableData().getModel() != null) {
            return tid.getOriginalMetadataID();
        }
        return tid;
    }

    @Override
    public String getExtensionProperty(Object metadataID, String key,
            boolean checkUnqualified) {
        metadataID = getActualMetadataId(metadataID);

        if (metadataID instanceof TempMetadataID) {
            return null;
        }
        return super.getExtensionProperty(metadataID, key, checkUnqualified);
    }

}
