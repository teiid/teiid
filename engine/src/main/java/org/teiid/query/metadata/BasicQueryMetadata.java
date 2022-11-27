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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.script.ScriptEngine;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.FunctionMethod;
import org.teiid.query.eval.TeiidScriptEngine;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.sql.lang.ObjectTable;
import org.teiid.query.sql.symbol.Expression;


/**
 * This is an abstract implementation of the metadata interface.  It can
 * be subclassed to create test implementations or partial implementations.
 */
public class BasicQueryMetadata implements QueryMetadataInterface {

    /**
     * Constructor for AbstractQueryMetadata.
     */
    public BasicQueryMetadata() {
        super();
    }

    /**
     * @see QueryMetadataInterface#getElementID(String)
     */
    public Object getElementID(String elementName)
        throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    /**
     * @see QueryMetadataInterface#getGroupID(String)
     */
    public Object getGroupID(String groupName)
        throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    /**
     * @see QueryMetadataInterface#getGroupID(String)
     */
    public Collection getGroupsForPartialName(String partialGroupName)
        throws TeiidComponentException, QueryMetadataException {
        return Collections.EMPTY_LIST;
    }

    /**
     * @see QueryMetadataInterface#getModelID(Object)
     */
    public Object getModelID(Object groupOrElementID)
        throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    /**
     * @see QueryMetadataInterface#getFullName(Object)
     */
    public String getFullName(Object metadataID)
        throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    /**
     * @see QueryMetadataInterface#getElementIDsInGroupID(Object)
     */
    public List getElementIDsInGroupID(Object groupID)
        throws TeiidComponentException, QueryMetadataException {
        return Collections.EMPTY_LIST;
    }

    /**
     * @see QueryMetadataInterface#getGroupIDForElementID(Object)
     */
    public Object getGroupIDForElementID(Object elementID)
        throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    /**
     * @see QueryMetadataInterface#getStoredProcedureInfoForProcedure(String)
     */
    public StoredProcedureInfo getStoredProcedureInfoForProcedure(String fullyQualifiedProcedureName)
        throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    @Override
    public String getElementRuntimeTypeName(Object elementID)
        throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    public String getDefaultValue(Object elementID)
        throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    public Object getMaximumValue(Object elementID) throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    public Object getMinimumValue(Object elementID) throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    /**
     * @see org.teiid.query.metadata.QueryMetadataInterface#getDistinctValues(java.lang.Object)
     * @since 4.3
     */
    public float getDistinctValues(Object elementID) throws TeiidComponentException,
                                                  QueryMetadataException {
        return -1;
    }
    /**
     * @see org.teiid.query.metadata.QueryMetadataInterface#getNullValues(java.lang.Object)
     * @since 4.3
     */
    public float getNullValues(Object elementID) throws TeiidComponentException,
                                              QueryMetadataException {
        return -1;
    }

    public int getPosition(Object elementID) throws TeiidComponentException, QueryMetadataException {
        return 0;
    }

    public int getPrecision(Object elementID) throws TeiidComponentException, QueryMetadataException {
        return 0;
    }

    public int getRadix(Object elementID) throws TeiidComponentException, QueryMetadataException {
        return 0;
    }

    @Override
    public String getFormat(Object elementID) throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    public int getScale(Object elementID) throws TeiidComponentException, QueryMetadataException {
        return 0;
    }


    /**
     * @see QueryMetadataInterface#isVirtualGroup(Object)
     */
    public boolean isVirtualGroup(Object groupID)
        throws TeiidComponentException, QueryMetadataException {
        return false;
    }

    /**
     * @see org.teiid.query.metadata.QueryMetadataInterface#hasMaterialization(java.lang.Object)
     * @since 4.2
     */
    public boolean hasMaterialization(Object groupID)
        throws TeiidComponentException, QueryMetadataException {
        return false;
    }

    /**
     * @see org.teiid.query.metadata.QueryMetadataInterface#getMaterialization(java.lang.Object)
     * @since 4.2
     */
    public Object getMaterialization(Object groupID)
        throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    /**
     * @see org.teiid.query.metadata.QueryMetadataInterface#getMaterializationStage(java.lang.Object)
     * @since 4.2
     */
    public Object getMaterializationStage(Object groupID)
        throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    /**
     * @see QueryMetadataInterface#isVirtualModel(Object)
     */
    public boolean isVirtualModel(Object modelID)
        throws TeiidComponentException, QueryMetadataException {
        return false;
    }

    @Override
    public QueryNode getVirtualPlan(Object groupID)
        throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    /**
     * Get procedure defining the insert plan for this group.
     * @param groupID Group
     * @return A string giving the procedure for inserts.
     */
    public String getInsertPlan(Object groupID)
        throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    /**
     * Get procedure defining the update plan for this group.
     * @param groupID Group
     * @return A string giving the procedure for inserts.
     */
    public String getUpdatePlan(Object groupID)
        throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    /**
     * Get procedure defining the delete plan for this group.
     * @param groupID Group
     * @return A string giving the procedure for inserts.
     */
    public String getDeletePlan(Object groupID)
        throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    /**
     * @see QueryMetadataInterface#modelSupports(Object, int)
     */
    public boolean modelSupports(Object modelID, int modelConstant)
        throws TeiidComponentException, QueryMetadataException {
        return false;
    }

    /**
     * @see QueryMetadataInterface#groupSupports(Object, int)
     */
    public boolean groupSupports(Object groupID, int groupConstant)
        throws TeiidComponentException, QueryMetadataException {
        return false;
    }

    /**
     * @see QueryMetadataInterface#elementSupports(Object, int)
     */
    public boolean elementSupports(Object elementID, int elementConstant)
        throws TeiidComponentException, QueryMetadataException {
        return false;
    }

    /**
     * @see QueryMetadataInterface#getMaxSetSize(Object)
     */
    public int getMaxSetSize(Object modelID)
        throws TeiidComponentException, QueryMetadataException {
        return 0;
    }


    /**
     * @see org.teiid.query.metadata.QueryMetadataInterface#getIndexesInGroup(java.lang.Object)
     */
    public Collection getIndexesInGroup(Object groupID)
        throws TeiidComponentException, QueryMetadataException {
        return Collections.EMPTY_SET;
    }

    /**
     * @see QueryMetadataInterface#getUniqueKeysInGroup(Object)
     */
    public Collection getUniqueKeysInGroup(Object groupID)
        throws TeiidComponentException, QueryMetadataException {
        return Collections.EMPTY_SET;
    }

    /**
     * @see QueryMetadataInterface#getForeignKeysInGroup(Object)
     */
    public Collection getForeignKeysInGroup(Object groupID)
        throws TeiidComponentException, QueryMetadataException {
        return Collections.EMPTY_SET;
    }

    /**
     * @see QueryMetadataInterface#getPrimaryKeyIDForForeignKeyID(Object)
     */
    public Object getPrimaryKeyIDForForeignKeyID(Object foreignKeyID)
        throws TeiidComponentException, QueryMetadataException{
        return null;
    }

    /**
     * @see QueryMetadataInterface#getElementIDsInKey(Object)
     */
    public List getElementIDsInKey(Object key)
        throws TeiidComponentException, QueryMetadataException {
        return Collections.EMPTY_LIST;
    }

    /**
     * @see org.teiid.query.metadata.QueryMetadataInterface#getAccessPatternsInGroup(Object)
     */
    public Collection getAccessPatternsInGroup(Object groupID)
        throws TeiidComponentException, QueryMetadataException {
        return Collections.EMPTY_SET;
    }

    /**
     * @see org.teiid.query.metadata.QueryMetadataInterface#getElementIDsInAccessPattern(Object)
     */
    public List getElementIDsInAccessPattern(Object accessPattern)
        throws TeiidComponentException, QueryMetadataException {
        return Collections.EMPTY_LIST;
    }

    /**
     * @see org.teiid.query.metadata.QueryMetadataInterface#getVirtualDatabaseName()
     */
    public String getVirtualDatabaseName()
        throws TeiidComponentException, QueryMetadataException {

        return null;
    }

    public float getCardinality(Object groupID)
        throws TeiidComponentException, QueryMetadataException{

        return QueryMetadataInterface.UNKNOWN_CARDINALITY;
    }

    public List getXMLSchemas(Object groupID) throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    public String getNameInSource(Object metadataID) throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    public int getElementLength(Object elementID) throws TeiidComponentException, QueryMetadataException {
        return 0;
    }

    public Properties getExtensionProperties(Object metadataID)
        throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    public String getNativeType(Object elementID) throws TeiidComponentException,
                                                 QueryMetadataException {
        return null;
    }

    public boolean isProcedure(Object elementID) throws TeiidComponentException, QueryMetadataException {
        return false;
    }

    public byte[] getBinaryVDBResource(String resourcePath) throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    public String getCharacterVDBResource(String resourcePath) throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    public String[] getVDBResourcePaths() throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    public boolean isTemporaryTable(Object groupID)
        throws TeiidComponentException, QueryMetadataException {
        return false;
    }

    public Object addToMetadataCache(Object metadataID, String key, Object value)
            throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    public Object getFromMetadataCache(Object metadataID, String key)
            throws TeiidComponentException, QueryMetadataException {
        return null;
    }

    public boolean isScalarGroup(Object groupID)
            throws TeiidComponentException, QueryMetadataException {
        return false;
    }

    @Override
    public FunctionLibrary getFunctionLibrary() {
        return null;
    }

    @Override
    public Object getPrimaryKey(Object metadataID) {
        return null;
    }

    @Override
    public boolean isMultiSource(Object modelId) {
        return false;
    }

    @Override
    public boolean isMultiSourceElement(Object elementId) {
        return false;
    }

    @Override
    public QueryMetadataInterface getDesignTimeMetadata() {
        return this;
    }

    @Override
    public boolean hasProcedure(String name) throws TeiidComponentException {
        return false;
    }

    @Override
    public String getName(Object metadataID) throws TeiidComponentException,
            QueryMetadataException {
        return null;
    }

    @Override
    public QueryMetadataInterface getSessionMetadata() {
        return null;
    }

    @Override
    public Set<String> getImportedModels() {
        return Collections.emptySet();
    }

    @Override
    public ScriptEngine getScriptEngine(String language) throws TeiidProcessingException {
        if (language == null || ObjectTable.DEFAULT_LANGUAGE.equals(language)) {
            return new TeiidScriptEngine();
        }
        return getScriptEngineDirect(language);
    }

    /**
     *
     * @param language
     * @return
     * @throws TeiidProcessingException
     */
    public ScriptEngine getScriptEngineDirect(String language) throws TeiidProcessingException {
        return null;
    }

    @Override
    public boolean isVariadic(Object metadataID) {
        return false;
    }

    @Override
    public Map<Expression, Integer> getFunctionBasedExpressions(Object metadataID) {
        return null;
    }

    @Override
    public boolean isPseudo(Object elementId) {
        return false;
    }

    @Override
    public Object getModelID(String modelName) throws TeiidComponentException,
            QueryMetadataException {
        return null;
    }

    @Override
    public String getExtensionProperty(Object metadataID, String key,
            boolean checkUnqualified) {
        return null;
    }

    @Override
    public boolean findShortName() {
        return false;
    }

    @Override
    public boolean useOutputName() {
        return true;
    }

    @Override
    public boolean widenComparisonToString() {
        return true;
    }

    @Override
    public Class<?> getDataTypeClass(String typeName)
            throws QueryMetadataException {
        return DataTypeManager.getDataTypeClass(typeName);
    }

    @Override
    public boolean isEnvAllowed() {
        return true;
    }

    @Override
    public boolean isLongRanks() {
        return false;
    }

    @Override
    public List<? extends Object> getModelIDs() {
        return Collections.emptyList();
    }

    @Override
    public FunctionMethod getPushdownFunction(Object modelID, String fullName) {
        return null;
    }

}
