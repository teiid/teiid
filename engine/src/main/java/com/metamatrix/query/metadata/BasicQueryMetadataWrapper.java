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

package com.metamatrix.query.metadata;

import java.util.Collection;
import java.util.List;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.query.mapping.relational.QueryNode;
import com.metamatrix.query.mapping.xml.MappingNode;

public class BasicQueryMetadataWrapper implements QueryMetadataInterface {
	
	protected QueryMetadataInterface actualMetadata;

	public BasicQueryMetadataWrapper(QueryMetadataInterface actualMetadata) {
		this.actualMetadata = actualMetadata;
	}

	public boolean elementSupports(Object elementID, int elementConstant)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.elementSupports(elementID, elementConstant);
	}

	public Collection getAccessPatternsInGroup(Object groupID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getAccessPatternsInGroup(groupID);
	}

	public byte[] getBinaryVDBResource(String resourcePath)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getBinaryVDBResource(resourcePath);
	}

	public int getCardinality(Object groupID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getCardinality(groupID);
	}

	public String getCharacterVDBResource(String resourcePath)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getCharacterVDBResource(resourcePath);
	}

	public Object getDefaultValue(Object elementID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getDefaultValue(elementID);
	}

	public String getDeletePlan(Object groupID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getDeletePlan(groupID);
	}

	public int getDistinctValues(Object elementID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getDistinctValues(elementID);
	}

	public Object getElementID(String elementName)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getElementID(elementName);
	}

	public List getElementIDsInAccessPattern(Object accessPattern)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getElementIDsInAccessPattern(accessPattern);
	}

	public List getElementIDsInGroupID(Object groupID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getElementIDsInGroupID(groupID);
	}

	public List getElementIDsInIndex(Object index)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getElementIDsInIndex(index);
	}

	public List getElementIDsInKey(Object key)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getElementIDsInKey(key);
	}

	public int getElementLength(Object elementID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getElementLength(elementID);
	}

	public String getElementType(Object elementID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getElementType(elementID);
	}

	public Properties getExtensionProperties(Object metadataID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getExtensionProperties(metadataID);
	}

	public Collection getForeignKeysInGroup(Object groupID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getForeignKeysInGroup(groupID);
	}

	public String getFullElementName(String fullGroupName,
			String shortElementName) throws MetaMatrixComponentException,
			QueryMetadataException {
		return actualMetadata.getFullElementName(fullGroupName,
				shortElementName);
	}

	public String getFullName(Object metadataID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getFullName(metadataID);
	}

	public Object getGroupID(String groupName)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getGroupID(groupName);
	}

	public Object getGroupIDForElementID(Object elementID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getGroupIDForElementID(elementID);
	}

	public String getGroupName(String fullElementName)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getGroupName(fullElementName);
	}

	public Collection getGroupsForPartialName(String partialGroupName)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getGroupsForPartialName(partialGroupName);
	}

	public Collection getIndexesInGroup(Object groupID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getIndexesInGroup(groupID);
	}

	public String getInsertPlan(Object groupID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getInsertPlan(groupID);
	}

	public MappingNode getMappingNode(Object groupID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getMappingNode(groupID);
	}

	public Object getMaterialization(Object groupID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getMaterialization(groupID);
	}

	public Object getMaterializationStage(Object groupID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getMaterializationStage(groupID);
	}

	public Object getMaximumValue(Object elementID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getMaximumValue(elementID);
	}

	public int getMaxSetSize(Object modelID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getMaxSetSize(modelID);
	}

	public Object getMinimumValue(Object elementID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getMinimumValue(elementID);
	}

	public String getModeledBaseType(Object elementID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getModeledBaseType(elementID);
	}

	public String getModeledPrimitiveType(Object elementID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getModeledPrimitiveType(elementID);
	}

	public String getModeledType(Object elementID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getModeledType(elementID);
	}

	public Object getModelID(Object groupOrElementID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getModelID(groupOrElementID);
	}

	public String getNameInSource(Object metadataID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getNameInSource(metadataID);
	}

	public String getNativeType(Object elementID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getNativeType(elementID);
	}

	public int getNullValues(Object elementID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getNullValues(elementID);
	}

	public int getPosition(Object elementID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getPosition(elementID);
	}

	public int getPrecision(Object elementID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getPrecision(elementID);
	}

	public Object getPrimaryKeyIDForForeignKeyID(Object foreignKeyID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getPrimaryKeyIDForForeignKeyID(foreignKeyID);
	}

	public int getRadix(Object elementID) throws MetaMatrixComponentException,
			QueryMetadataException {
		return actualMetadata.getRadix(elementID);
	}
	
	public String getFormat(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getFormat(elementID);
	}   	

	public int getScale(Object elementID) throws MetaMatrixComponentException,
			QueryMetadataException {
		return actualMetadata.getScale(elementID);
	}

	public String getShortElementName(String fullElementName)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getShortElementName(fullElementName);
	}

	public StoredProcedureInfo getStoredProcedureInfoForProcedure(
			String fullyQualifiedProcedureName)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata
				.getStoredProcedureInfoForProcedure(fullyQualifiedProcedureName);
	}

	public Collection getUniqueKeysInGroup(Object groupID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getUniqueKeysInGroup(groupID);
	}

	public String getUpdatePlan(Object groupID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getUpdatePlan(groupID);
	}

	public String[] getVDBResourcePaths() throws MetaMatrixComponentException,
			QueryMetadataException {
		return actualMetadata.getVDBResourcePaths();
	}

	public String getVirtualDatabaseName() throws MetaMatrixComponentException,
			QueryMetadataException {
		return actualMetadata.getVirtualDatabaseName();
	}

	public QueryNode getVirtualPlan(Object groupID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getVirtualPlan(groupID);
	}

	public List getXMLSchemas(Object groupID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getXMLSchemas(groupID);
	}

	public Collection getXMLTempGroups(Object groupID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getXMLTempGroups(groupID);
	}

	public boolean groupSupports(Object groupID, int groupConstant)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.groupSupports(groupID, groupConstant);
	}

	public boolean hasMaterialization(Object groupID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.hasMaterialization(groupID);
	}

	public boolean isProcedure(Object groupID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.isProcedure(groupID);
	}

	public boolean isTemporaryTable(Object groupID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.isTemporaryTable(groupID);
	}

	public boolean isVirtualGroup(Object groupID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.isVirtualGroup(groupID);
	}

	public boolean isVirtualModel(Object modelID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.isVirtualModel(modelID);
	}

	public boolean isXMLGroup(Object groupID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.isXMLGroup(groupID);
	}

	public boolean modelSupports(Object modelID, int modelConstant)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.modelSupports(modelID, modelConstant);
	}

	public Object addToMetadataCache(Object metadataID, String key, Object value)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.addToMetadataCache(metadataID, key, value);
	}

	public Object getFromMetadataCache(Object metadataID, String key)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.getFromMetadataCache(metadataID, key);
	}

	public boolean isScalarGroup(Object groupID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return actualMetadata.isScalarGroup(groupID);
	}

}
