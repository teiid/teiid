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

package org.teiid.query.metadata;

import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.mapping.xml.MappingNode;


public class BasicQueryMetadataWrapper implements QueryMetadataInterface {
	
	protected QueryMetadataInterface actualMetadata;

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

	public int getCardinality(Object groupID)
			throws TeiidComponentException, QueryMetadataException {
		return actualMetadata.getCardinality(groupID);
	}

	public String getCharacterVDBResource(String resourcePath)
			throws TeiidComponentException, QueryMetadataException {
		return actualMetadata.getCharacterVDBResource(resourcePath);
	}

	public Object getDefaultValue(Object elementID)
			throws TeiidComponentException, QueryMetadataException {
		return actualMetadata.getDefaultValue(elementID);
	}

	public String getDeletePlan(Object groupID)
			throws TeiidComponentException, QueryMetadataException {
		return actualMetadata.getDeletePlan(groupID);
	}

	public int getDistinctValues(Object elementID)
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

	public List getElementIDsInIndex(Object index)
			throws TeiidComponentException, QueryMetadataException {
		return actualMetadata.getElementIDsInIndex(index);
	}

	public List getElementIDsInKey(Object key)
			throws TeiidComponentException, QueryMetadataException {
		return actualMetadata.getElementIDsInKey(key);
	}

	public int getElementLength(Object elementID)
			throws TeiidComponentException, QueryMetadataException {
		return actualMetadata.getElementLength(elementID);
	}

	public String getElementType(Object elementID)
			throws TeiidComponentException, QueryMetadataException {
		return actualMetadata.getElementType(elementID);
	}

	public Properties getExtensionProperties(Object metadataID)
			throws TeiidComponentException, QueryMetadataException {
		return actualMetadata.getExtensionProperties(metadataID);
	}

	public Collection getForeignKeysInGroup(Object groupID)
			throws TeiidComponentException, QueryMetadataException {
		return actualMetadata.getForeignKeysInGroup(groupID);
	}

	public String getFullElementName(String fullGroupName,
			String shortElementName) throws TeiidComponentException,
			QueryMetadataException {
		return actualMetadata.getFullElementName(fullGroupName,
				shortElementName);
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

	public String getGroupName(String fullElementName)
			throws TeiidComponentException, QueryMetadataException {
		return actualMetadata.getGroupName(fullElementName);
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

	public MappingNode getMappingNode(Object groupID)
			throws TeiidComponentException, QueryMetadataException {
		return actualMetadata.getMappingNode(groupID);
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

	public String getModeledBaseType(Object elementID)
			throws TeiidComponentException, QueryMetadataException {
		return actualMetadata.getModeledBaseType(elementID);
	}

	public String getModeledPrimitiveType(Object elementID)
			throws TeiidComponentException, QueryMetadataException {
		return actualMetadata.getModeledPrimitiveType(elementID);
	}

	public String getModeledType(Object elementID)
			throws TeiidComponentException, QueryMetadataException {
		return actualMetadata.getModeledType(elementID);
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

	public int getNullValues(Object elementID)
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

	public String getShortElementName(String fullElementName)
			throws TeiidComponentException, QueryMetadataException {
		return actualMetadata.getShortElementName(fullElementName);
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

	public List getXMLSchemas(Object groupID)
			throws TeiidComponentException, QueryMetadataException {
		return actualMetadata.getXMLSchemas(groupID);
	}

	public Collection getXMLTempGroups(Object groupID)
			throws TeiidComponentException, QueryMetadataException {
		return actualMetadata.getXMLTempGroups(groupID);
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

	public boolean isXMLGroup(Object groupID)
			throws TeiidComponentException, QueryMetadataException {
		return actualMetadata.isXMLGroup(groupID);
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
		return actualMetadata.getDesignTimeMetadata();
	}
	
	@Override
	public boolean hasProcedure(String name) throws TeiidComponentException {
		return actualMetadata.hasProcedure(name);
	}

}
