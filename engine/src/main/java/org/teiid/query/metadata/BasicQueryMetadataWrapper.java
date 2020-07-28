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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.script.ScriptEngine;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.metadata.FunctionMethod;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.sql.symbol.Expression;


public class BasicQueryMetadataWrapper implements QueryMetadataInterface {

    protected QueryMetadataInterface actualMetadata;
    protected QueryMetadataInterface designTimeMetadata;
    protected boolean designTime;

    public BasicQueryMetadataWrapper(QueryMetadataInterface actualMetadata) {
        this.actualMetadata = actualMetadata;
    }

    public boolean elementSupports(Object elementID, int elementConstant)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.elementSupports(elementID, elementConstant);
    }

    public Collection getAccessPatternsInGroup(Object groupID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getAccessPatternsInGroup(groupID);
    }

    public byte[] getBinaryVDBResource(String resourcePath)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getBinaryVDBResource(resourcePath);
    }

    public float getCardinality(Object groupID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getCardinality(groupID);
    }

    public String getCharacterVDBResource(String resourcePath)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getCharacterVDBResource(resourcePath);
    }

    public String getDefaultValue(Object elementID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getDefaultValue(elementID);
    }

    public String getDeletePlan(Object groupID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getDeletePlan(groupID);
    }

    public float getDistinctValues(Object elementID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getDistinctValues(elementID);
    }

    public Object getElementID(String elementName)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getElementID(elementName);
    }

    public List getElementIDsInAccessPattern(Object accessPattern)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getElementIDsInAccessPattern(accessPattern);
    }

    public List getElementIDsInGroupID(Object groupID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getElementIDsInGroupID(groupID);
    }

    public List getElementIDsInKey(Object key)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getElementIDsInKey(key);
    }

    public int getElementLength(Object elementID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getElementLength(elementID);
    }

    public String getElementRuntimeTypeName(Object elementID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getElementRuntimeTypeName(elementID);
    }

    public Properties getExtensionProperties(Object metadataID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getExtensionProperties(metadataID);
    }

    public Collection getForeignKeysInGroup(Object groupID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getForeignKeysInGroup(groupID);
    }

    public String getFullName(Object metadataID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getFullName(metadataID);
    }

    public Object getGroupID(String groupName)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getGroupID(groupName);
    }

    public Object getGroupIDForElementID(Object elementID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getGroupIDForElementID(elementID);
    }

    public Collection getGroupsForPartialName(String partialGroupName)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getGroupsForPartialName(partialGroupName);
    }

    public Collection getIndexesInGroup(Object groupID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getIndexesInGroup(groupID);
    }

    public String getInsertPlan(Object groupID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getInsertPlan(groupID);
    }

    public Object getMaterialization(Object groupID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getMaterialization(groupID);
    }

    public Object getMaterializationStage(Object groupID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getMaterializationStage(groupID);
    }

    public Object getMaximumValue(Object elementID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getMaximumValue(elementID);
    }

    public int getMaxSetSize(Object modelID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getMaxSetSize(modelID);
    }

    public Object getMinimumValue(Object elementID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getMinimumValue(elementID);
    }

    public Object getModelID(Object groupOrElementID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getModelID(groupOrElementID);
    }

    public String getNameInSource(Object metadataID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getNameInSource(metadataID);
    }

    public String getNativeType(Object elementID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getNativeType(elementID);
    }

    public float getNullValues(Object elementID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getNullValues(elementID);
    }

    public int getPosition(Object elementID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getPosition(elementID);
    }

    public int getPrecision(Object elementID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getPrecision(elementID);
    }

    public Object getPrimaryKeyIDForForeignKeyID(Object foreignKeyID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getPrimaryKeyIDForForeignKeyID(foreignKeyID);
    }

    public int getRadix(Object elementID) throws TeiidComponentException,
            QueryMetadataException {
        return actualMetadata.getRadix(elementID);
    }

    public String getFormat(Object elementID) throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getFormat(elementID);
    }

    public int getScale(Object elementID) throws TeiidComponentException,
            QueryMetadataException {
        return actualMetadata.getScale(elementID);
    }

    public StoredProcedureInfo getStoredProcedureInfoForProcedure(
            String fullyQualifiedProcedureName)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata
                .getStoredProcedureInfoForProcedure(fullyQualifiedProcedureName);
    }

    public Collection getUniqueKeysInGroup(Object groupID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getUniqueKeysInGroup(groupID);
    }

    public String getUpdatePlan(Object groupID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getUpdatePlan(groupID);
    }

    public String[] getVDBResourcePaths() throws TeiidComponentException,
            QueryMetadataException {
        return actualMetadata.getVDBResourcePaths();
    }

    public String getVirtualDatabaseName() throws TeiidComponentException,
            QueryMetadataException {
        return actualMetadata.getVirtualDatabaseName();
    }

    public QueryNode getVirtualPlan(Object groupID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getVirtualPlan(groupID);
    }

    public boolean groupSupports(Object groupID, int groupConstant)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.groupSupports(groupID, groupConstant);
    }

    public boolean hasMaterialization(Object groupID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.hasMaterialization(groupID);
    }

    public boolean isProcedure(Object groupID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.isProcedure(groupID);
    }

    public boolean isTemporaryTable(Object groupID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.isTemporaryTable(groupID);
    }

    public boolean isVirtualGroup(Object groupID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.isVirtualGroup(groupID);
    }

    public boolean isVirtualModel(Object modelID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.isVirtualModel(modelID);
    }

    public boolean modelSupports(Object modelID, int modelConstant)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.modelSupports(modelID, modelConstant);
    }

    public Object addToMetadataCache(Object metadataID, String key, Object value)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.addToMetadataCache(metadataID, key, value);
    }

    public Object getFromMetadataCache(Object metadataID, String key)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.getFromMetadataCache(metadataID, key);
    }

    public boolean isScalarGroup(Object groupID)
            throws TeiidComponentException, QueryMetadataException {
        return actualMetadata.isScalarGroup(groupID);
    }

    @Override
    public FunctionLibrary getFunctionLibrary() {
        return actualMetadata.getFunctionLibrary();
    }

    @Override
    public Object getPrimaryKey(Object metadataID) {
        return actualMetadata.getPrimaryKey(metadataID);
    }

    @Override
    public boolean isMultiSource(Object modelId) throws QueryMetadataException, TeiidComponentException {
        return actualMetadata.isMultiSource(modelId);
    }

    @Override
    public boolean isMultiSourceElement(Object elementId) throws QueryMetadataException, TeiidComponentException {
        return actualMetadata.isMultiSourceElement(elementId);
    }

    @Override
    public QueryMetadataInterface getDesignTimeMetadata() {
        if (designTime) {
            return this;
        }
        if (designTimeMetadata == null) {
            designTimeMetadata = createDesignTimeMetadata();
            if (designTimeMetadata instanceof BasicQueryMetadataWrapper) {
                ((BasicQueryMetadataWrapper)designTimeMetadata).designTime = true;
            }
        }
        return designTimeMetadata;
    }

    protected QueryMetadataInterface createDesignTimeMetadata() {
        return actualMetadata.getDesignTimeMetadata();
    }

    @Override
    public boolean hasProcedure(String name) throws TeiidComponentException {
        return actualMetadata.hasProcedure(name);
    }

    @Override
    public String getName(Object metadataID) throws TeiidComponentException,
            QueryMetadataException {
        return actualMetadata.getName(metadataID);
    }

    @Override
    public QueryMetadataInterface getSessionMetadata() {
        return actualMetadata.getSessionMetadata();
    }

    @Override
    public Set<String> getImportedModels() {
        return actualMetadata.getImportedModels();
    }

    @Override
    public ScriptEngine getScriptEngine(String langauge) throws TeiidProcessingException {
        return actualMetadata.getScriptEngine(langauge);
    }

    @Override
    public boolean isVariadic(Object metadataID) {
        return actualMetadata.isVariadic(metadataID);
    }

    @Override
    public Map<Expression, Integer> getFunctionBasedExpressions(Object metadataID) {
        return actualMetadata.getFunctionBasedExpressions(metadataID);
    }

    @Override
    public boolean isPseudo(Object elementId) {
        return actualMetadata.isPseudo(elementId);
    }

    @Override
    public Object getModelID(String modelName) throws TeiidComponentException,
            QueryMetadataException {
        return actualMetadata.getModelID(modelName);
    }

    @Override
    public String getExtensionProperty(Object metadataID, String key,
            boolean checkUnqualified) {
        return actualMetadata.getExtensionProperty(metadataID, key, checkUnqualified);
    }

    @Override
    public boolean findShortName() {
        return actualMetadata.findShortName();
    }

    @Override
    public boolean useOutputName() {
        return actualMetadata.useOutputName();
    }

    @Override
    public boolean widenComparisonToString() {
        return actualMetadata.widenComparisonToString();
    }

    @Override
    public Class<?> getDataTypeClass(String typeName)
            throws QueryMetadataException {
        return actualMetadata.getDataTypeClass(typeName);
    }

    @Override
    public boolean isEnvAllowed() {
        return actualMetadata.isEnvAllowed();
    }

    @Override
    public boolean isLongRanks() {
        return actualMetadata.isLongRanks();
    }

    @Override
    public List<? extends Object> getModelIDs() {
        return actualMetadata.getModelIDs();
    }

    @Override
    public FunctionMethod getPushdownFunction(Object modelID, String fullName) {
        return actualMetadata.getPushdownFunction(modelID, fullName);
    }

}
