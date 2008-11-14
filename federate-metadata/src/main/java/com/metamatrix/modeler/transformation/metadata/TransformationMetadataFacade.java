/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.modeler.transformation.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.LRUCache;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.metadata.runtime.RuntimeMetadataPlugin;
import com.metamatrix.modeler.core.index.IndexConstants;
import com.metamatrix.modeler.core.metadata.runtime.ColumnRecord;
import com.metamatrix.modeler.core.metadata.runtime.MetadataRecord;
import com.metamatrix.modeler.core.metadata.runtime.TableRecord;
import com.metamatrix.modeler.core.metadata.runtime.ColumnSetRecord.ColumnSetRecordProperties;
import com.metamatrix.modeler.core.metadata.runtime.ForeignKeyRecord.ForeignKeyRecordProperties;
import com.metamatrix.modeler.core.metadata.runtime.MetadataRecord.MetadataRecordProperties;
import com.metamatrix.modeler.core.metadata.runtime.ProcedureRecord.ProcedureRecordProperties;
import com.metamatrix.modeler.core.metadata.runtime.TableRecord.TableRecordProperties;
import com.metamatrix.modeler.internal.transformation.util.UuidUtil;
import com.metamatrix.query.mapping.relational.QueryNode;
import com.metamatrix.query.mapping.xml.MappingDocument;
import com.metamatrix.query.mapping.xml.MappingNode;
import com.metamatrix.query.metadata.GroupInfo;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.StoredProcedureInfo;

/**
 * Modelers implementation of QueryMetadataInterface that reads columns, groups, modeles etc.
 * index files for various metadata properties.
 * TransformationMetadataFacade should only be used when the metadata is read only. It is used
 * in the modeler with in the context of validating a Query(when the metadata is read only).
 */
public class TransformationMetadataFacade implements QueryMetadataInterface {
    
    /**
     * Default amount of space in the cache
     */
    public static final int DEFAULT_SPACELIMIT = 4000;
    
    /**
     * Partial name cache is useful when the user executes same kind of
     * partial name queries many times, number of such queries will not be many
     * so limiting this cachesize to 100. 
     */
    public static final int DEFAULT_SPACELIMIT_PARTIAL_NAME_CACHE = 100;    
    
    private static final int GROUP_INFO_CACHE_SIZE = 500;

    private final TransformationMetadata metadata;
    private final Map<String, Object> nameToIdCache;
    private final Map<Object, MetadataRecord> idToRecordCache;
    private final Map<String, String> partialNameToFullNameCache;
    private final Map groupInfoCache = Collections.synchronizedMap(new LRUCache(GROUP_INFO_CACHE_SIZE));

    public TransformationMetadataFacade(final TransformationMetadata delegate) {
        this(delegate, DEFAULT_SPACELIMIT);
    }

    public TransformationMetadataFacade(final TransformationMetadata delegate, int cacheSize) {
        ArgCheck.isNotNull(delegate);
        this.metadata = delegate;
        this.nameToIdCache   = Collections.synchronizedMap(new LRUCache<String, Object>(cacheSize));
        this.idToRecordCache = Collections.synchronizedMap(new LRUCache<Object, MetadataRecord>(cacheSize));
        this.partialNameToFullNameCache = Collections.synchronizedMap(new LRUCache<String, String>(DEFAULT_SPACELIMIT_PARTIAL_NAME_CACHE));
    }

    //==================================================================================
    //                     I N T E R F A C E   M E T H O D S
    //==================================================================================

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementID(java.lang.String)
     */
    public Object getElementID(final String elementName) throws MetaMatrixComponentException, QueryMetadataException {
        // Check the cache first ...
        MetadataRecord record = getRecordByName(elementName, IndexConstants.RECORD_TYPE.COLUMN);

        // If not found in the cache then retrieve it from the index
        if (record == null) {
			record = (MetadataRecord) this.metadata.getElementID(elementName);
			// Update the cache ... 
			if (record != null) {
				updateNameToIdCache(elementName, IndexConstants.RECORD_TYPE.COLUMN, record.getUUID());
				updateIdToRecordCache(record.getUUID(),record);
			}
        }
        return record;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getGroupID(java.lang.String)
     */
    public Object getGroupID(final String groupName) throws MetaMatrixComponentException, QueryMetadataException {
        // Check the cache first ...
        MetadataRecord record = getRecordByName(groupName, IndexConstants.RECORD_TYPE.TABLE);

        // If not found in the cache then retrieve it from the index
        if (record == null) {
            record = (MetadataRecord) this.metadata.getGroupID(groupName);
            // Update the cache ... 
            if (record != null) {
                updateNameToIdCache(groupName, IndexConstants.RECORD_TYPE.TABLE, record.getUUID());
                updateIdToRecordCache(record.getUUID(),record);
            }
        }
        return record;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getGroupsForPartialName(java.lang.String)
     */
    public Collection getGroupsForPartialName(final String partialGroupName) throws MetaMatrixComponentException, QueryMetadataException {
        // Check the cache first ...
        String fullName = getFullNameByPartialName(partialGroupName, IndexConstants.RECORD_TYPE.TABLE);

        // If not found in the cache then retrieve it from the index
        if (fullName == null) {
            synchronized(partialNameToFullNameCache) {
                // look up the cache again, might have been updated by
                // the thread that just released the lock
                fullName = getFullNameByPartialName(partialGroupName, IndexConstants.RECORD_TYPE.TABLE);
				if(fullName == null) {
	                // search for the records that match the partial name
	                Collection partialNameRecords = this.metadata.getGroupsForPartialName(partialGroupName);
	                // Update the cache only if there is one matching record...otherwise its a failure case (ambiguous) 
	                if (partialNameRecords != null && partialNameRecords.size() == 1) {
	                    updatePartialNameToFullName(partialGroupName, (String) partialNameRecords.iterator().next(), IndexConstants.RECORD_TYPE.TABLE);
	                }
	                return partialNameRecords;
				}
            }
        }
        Collection partialNameRecords = new ArrayList(1);
        partialNameRecords.add(fullName);
        return partialNameRecords;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getModelID(java.lang.Object)
     */
    public Object getModelID(final Object groupOrElementID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(MetadataRecord.class, groupOrElementID);

        MetadataRecord record = (MetadataRecord) groupOrElementID;

        Object modelRecord = record.getPropertyValue(MetadataRecordProperties.MODEL_FOR_RECORD);
        if(modelRecord == null) {
            synchronized(record) {
                // look up the cache again, might have been updated by
                // the thread that just released the lock
                modelRecord = record.getPropertyValue(MetadataRecordProperties.MODEL_FOR_RECORD);
                if(modelRecord == null) {
                    modelRecord = this.metadata.getModelID(groupOrElementID);
                	record.setPropertyValue(MetadataRecordProperties.MODEL_FOR_RECORD, modelRecord);
                }
            }
        }

        return modelRecord;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getFullName(java.lang.Object)
     */
    public String getFullName(final Object metadataID) throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.getFullName(metadataID);
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getFullElementName(java.lang.String, java.lang.String)
     */
    public String getFullElementName(final String fullGroupName, final String shortElementName) throws MetaMatrixComponentException, QueryMetadataException {
		ArgCheck.isNotZeroLength(fullGroupName);
		ArgCheck.isNotZeroLength(shortElementName);

		if(UuidUtil.isStringifiedUUID(shortElementName)) {
			// If element name is uuid, return element uuid (sans prefix)
			return UuidUtil.stripPrefixFromUUID(shortElementName);
		} else if(UuidUtil.isStringifiedUUID(fullGroupName)) {    
			// If group name is uuid and element is not, return just element uuid

			// Lookup group name
			MetadataRecord group = (MetadataRecord) getGroupID(fullGroupName);
			if(group == null){
				throw new QueryMetadataException(RuntimeMetadataPlugin.Util.getString("TransformationMetadata.Unable_to_determine_fullname_for_element__1") + shortElementName); //$NON-NLS-1$
			}
			String groupName = group.getFullName();

			// Combine group name and element name
			return groupName + TransformationMetadata.DELIMITER_CHAR + shortElementName;
		}
		// Both are not UUIDs, so just combine them
		return fullGroupName + TransformationMetadata.DELIMITER_CHAR + shortElementName;
    }


    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getShortElementName(java.lang.String)
     */
    public String getShortElementName(final String fullElementName) throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.getShortElementName(fullElementName);
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getGroupName(java.lang.String)
     */
    public String getGroupName(final String fullElementName) throws MetaMatrixComponentException, QueryMetadataException {
		ArgCheck.isNotZeroLength(fullElementName);  

		int index = fullElementName.lastIndexOf(TransformationMetadata.DELIMITER_CHAR);
		if(index >= 0) { 
			return fullElementName.substring(0, index);
		}
		// Not fully qualified, but it still may be a UUID
		if(UuidUtil.isStringifiedUUID(fullElementName)) {
			String newFullElementName = UuidUtil.stripPrefixFromUUID(fullElementName);
			MetadataRecord record = getRecordByID(newFullElementName); 
			// record could not be found in the cache look up transformation metadata
			if(record == null) {
				record = (MetadataRecord) this.metadata.getElementID(newFullElementName);
				updateIdToRecordCache(newFullElementName,record);
			}
			return record.getParentUUID();
		}
		// Otherwise return null, signifying that we were not
		// able to resolve the UUID into a real element
		return null;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementIDsInGroupID(java.lang.Object)
     */
    public List getElementIDsInGroupID(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(TableRecord.class, groupID);
        
        MetadataRecord record = (MetadataRecord) groupID;

        List elementIDs = (List) record.getPropertyValue(TableRecordProperties.ELEMENTS_IN_GROUP);
        if(elementIDs == null) {
            synchronized(record) {
                // look up the cache again, might have been updated by
                // the thread that just released the lock
                elementIDs = (List) record.getPropertyValue(TableRecordProperties.ELEMENTS_IN_GROUP);
                if(elementIDs == null) {
                    elementIDs = this.metadata.getElementIDsInGroupID(groupID);
					if(elementIDs != null) {                    
	                    record.setPropertyValue(TableRecordProperties.ELEMENTS_IN_GROUP, elementIDs);
	                    Iterator elemntIter =  elementIDs.iterator();
	                    while(elemntIter.hasNext()) {
	                        MetadataRecord columnRecord = (MetadataRecord) elemntIter.next();
	                        // Update the cache ...
	                        if (columnRecord != null) {
	                            updateNameToIdCache(columnRecord.getFullName(), columnRecord.getRecordType(), columnRecord.getUUID());
	                            updateIdToRecordCache(columnRecord.getUUID(),columnRecord);
	                        }
	                    }
					}
                }
            }    
        }
        
        return elementIDs;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getGroupIDForElementID(java.lang.Object)
     */
    public Object getGroupIDForElementID(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(ColumnRecord.class, elementID);
        ColumnRecord columnRecord = (ColumnRecord) elementID;
        
        String tableUUID = columnRecord.getParentUUID();

        // Check the cache first ...
        MetadataRecord record = getRecordByID(tableUUID);

        // If not found in the cache then retrieve it from the index
        if (record == null) {
			record = (MetadataRecord) this.metadata.getGroupID(tableUUID);
			// Update the cache ... 
			if (record != null) {
				updateNameToIdCache(record.getFullName(), record.getRecordType(),record.getUUID());
				updateIdToRecordCache(record.getUUID(),record);
			}
        } 
        return record;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getStoredProcedureInfoForProcedure(java.lang.String)
     */
    public StoredProcedureInfo getStoredProcedureInfoForProcedure(final String fullyQualifiedProcedureName)
        throws MetaMatrixComponentException, QueryMetadataException {

        StoredProcedureInfo procInfo = null;

        // Check the cache first ...
        MetadataRecord record = getRecordByName(fullyQualifiedProcedureName, IndexConstants.RECORD_TYPE.CALLABLE);
    
        // If not found in the cache then retrieve it from the index
        if (record == null) {
            // lookup the indexes for the record
            procInfo = this.metadata.getStoredProcedureInfoForProcedure(fullyQualifiedProcedureName);
            if(procInfo != null) {
            	// a record should always be found on the procInfo
				record = (MetadataRecord) procInfo.getProcedureID();
				// update the cache on the record with the procIndo object
				record.setPropertyValue(ProcedureRecordProperties.STORED_PROC_INFO_FOR_RECORD, procInfo);
				// Update the cache ... with procedure info 
				updateNameToIdCache(fullyQualifiedProcedureName, IndexConstants.RECORD_TYPE.CALLABLE, record.getUUID());
				updateIdToRecordCache(record.getUUID(),record);
            }
        }

		// found record
		if(procInfo == null && record != null) {
			// if the record is found it should have been update with the procInfo object
			procInfo = (StoredProcedureInfo) record.getPropertyValue(ProcedureRecordProperties.STORED_PROC_INFO_FOR_RECORD);
			// this should never occur but if procInfo cannot be found on the record
			if(procInfo == null) {
				procInfo = this.metadata.getStoredProcedureInfoForProcedure(fullyQualifiedProcedureName);
				record.setPropertyValue(ProcedureRecordProperties.STORED_PROC_INFO_FOR_RECORD, procInfo);
			}
		}

        return procInfo;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementType(java.lang.Object)
     */
    public String getElementType(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.getElementType(elementID);
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getDefaultValue(java.lang.String)
     */
    public Object getDefaultValue(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.getDefaultValue(elementID);
    }

    public Object getMaximumValue(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.getMaximumValue(elementID);
    }

    public Object getMinimumValue(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.getMinimumValue(elementID);
    }
    
    public int getPosition(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.getPosition(elementID);
    }
    
    public int getPrecision(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.getPrecision(elementID);
    }
    
    public int getRadix(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.getRadix(elementID);
    }
    
    public int getScale(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.getScale(elementID);
    }
    
    public int getDistinctValues(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.getDistinctValues(elementID);
    }
    
    public int getNullValues(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.getNullValues(elementID);
    }
    
    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#isVirtualGroup(java.lang.Object)
     */
    public boolean isVirtualGroup(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.isVirtualGroup(groupID);
    }

    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#hasMaterialization(java.lang.Object)
     * @since 4.2
     */
    public boolean hasMaterialization(final Object groupID) throws MetaMatrixComponentException,
                                                     QueryMetadataException {
        return this.metadata.hasMaterialization(groupID);
    }
    
    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getMaterialization(java.lang.Object)
     * @since 4.2
     */
    public Object getMaterialization(final Object groupID) 
        throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.getMaterialization(groupID);
    }
    
    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getMaterializationStage(java.lang.Object)
     * @since 4.2
     */
    public Object getMaterializationStage(final Object groupID) 
        throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.getMaterializationStage(groupID);
    }
    
    public boolean isVirtualModel(final Object modelID) throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.isVirtualModel(modelID);
    }

    /*
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#isProcedureInputElement(java.lang.Object)
     */
    public boolean isProcedure(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.isProcedure(elementID);
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getVirtualPlan(java.lang.Object)
     */
    public QueryNode getVirtualPlan(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(TableRecord.class, groupID);

        TableRecord tableRecord = (TableRecord) groupID;

        QueryNode queryPlan = (QueryNode) tableRecord.getPropertyValue(TableRecordProperties.QUERY_PLAN);
        if(queryPlan == null) {
            synchronized(tableRecord) {
                // look up the cache again, might have been updated by
                // the thread that just released the lock
                queryPlan = (QueryNode) tableRecord.getPropertyValue(TableRecordProperties.QUERY_PLAN);
                if(queryPlan == null) {
                    queryPlan = this.metadata.getVirtualPlan(groupID);
                    tableRecord.setPropertyValue(TableRecordProperties.QUERY_PLAN, queryPlan);
                }
            }    
        }

        return queryPlan;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getInsertPlan(java.lang.Object)
     */
    public String getInsertPlan(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(TableRecord.class, groupID);

        TableRecord tableRecord = (TableRecord) groupID;
        String insertPlan = (String) tableRecord.getPropertyValue(TableRecordProperties.INSERT_PLAN);
        if(insertPlan == null) {
            // look up the cache again, might have been updated by
            // the thread that just released the lock
            synchronized(tableRecord) {
                insertPlan = (String) tableRecord.getPropertyValue(TableRecordProperties.INSERT_PLAN);
                if(insertPlan == null) {
                    insertPlan = this.metadata.getInsertPlan(groupID);
                    tableRecord.setPropertyValue(TableRecordProperties.INSERT_PLAN, insertPlan);
                }
            }    
        }

        return insertPlan;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getUpdatePlan(java.lang.Object)
     */
    public String getUpdatePlan(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(TableRecord.class, groupID);

        TableRecord tableRecord = (TableRecord) groupID;
        String updatePlan = (String) tableRecord.getPropertyValue(TableRecordProperties.UPDATE_PLAN);
        if(updatePlan == null) {
            synchronized(tableRecord) {
                // look up the cache again, might have been updated by
                // the thread that just released the lock
                updatePlan = (String) tableRecord.getPropertyValue(TableRecordProperties.UPDATE_PLAN);
                if(updatePlan == null) {
                    updatePlan = this.metadata.getUpdatePlan(groupID);
                    tableRecord.setPropertyValue(TableRecordProperties.UPDATE_PLAN, updatePlan);
                }
            }    
        }

        return updatePlan;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getDeletePlan(java.lang.Object)
     */
    public String getDeletePlan(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(TableRecord.class, groupID);

        TableRecord tableRecord = (TableRecord) groupID;
        String deletePlan = (String) tableRecord.getPropertyValue(TableRecordProperties.DELETE_PLAN);
        if(deletePlan == null) {
            synchronized(tableRecord) {
                // look up the cache again, might have been updated by
                // the thread that just released the lock
                deletePlan = (String) tableRecord.getPropertyValue(TableRecordProperties.DELETE_PLAN);
                if(deletePlan == null) {
                    deletePlan = this.metadata.getDeletePlan(groupID);
                    tableRecord.setPropertyValue(TableRecordProperties.DELETE_PLAN, deletePlan);
                }
            }    
        }

        return deletePlan;

    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#modelSupports(java.lang.Object, int)
     */
    public boolean modelSupports(final Object modelID, final int modelConstant) throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.modelSupports(modelID,modelConstant);
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#groupSupports(java.lang.Object, int)
     */
    public boolean groupSupports(final Object groupID, final int groupConstant) throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.groupSupports(groupID,groupConstant);
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#elementSupports(java.lang.Object, int)
     */
    public boolean elementSupports(final Object elementID, final int elementConstant) throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.elementSupports(elementID,elementConstant);
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getMaxSetSize(java.lang.Object)
     */
    public int getMaxSetSize(final Object modelID) throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.getMaxSetSize(modelID);
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getIndexesInGroup(java.lang.Object)
     */
    public Collection getIndexesInGroup(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(TableRecord.class, groupID);

        MetadataRecord record = (MetadataRecord) groupID;
        Collection indexes = (Collection) record.getPropertyValue(TableRecordProperties.INDEXES_IN_GROUP);
        if(indexes == null) {
            synchronized(record) {
                // look up the cache again, might have been updated by
                // the thread that just released the lock
                indexes = (Collection) record.getPropertyValue(TableRecordProperties.INDEXES_IN_GROUP);
                if(indexes == null) {
                    indexes = this.metadata.getIndexesInGroup(groupID);
                    record.setPropertyValue(TableRecordProperties.INDEXES_IN_GROUP, indexes);
                }
            }    
        }

        return indexes;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getUniqueKeysInGroup(java.lang.Object)
     */
    public Collection getUniqueKeysInGroup(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(TableRecord.class, groupID);
        
        MetadataRecord record = (MetadataRecord) groupID;
        Collection uks = (Collection) record.getPropertyValue(TableRecordProperties.UNIQUEKEYS_IN_GROUP);
        if(uks == null) {
            synchronized(record) {
                // look up the cache again, might have been updated by
                // the thread that just released the lock
                uks = (Collection) record.getPropertyValue(TableRecordProperties.UNIQUEKEYS_IN_GROUP);
                if(uks == null) {
                    uks = this.metadata.getUniqueKeysInGroup(groupID);
                    record.setPropertyValue(TableRecordProperties.UNIQUEKEYS_IN_GROUP, uks);
                }
            }    
        }

        return uks;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getForeignKeysInGroup(java.lang.Object)
     */
    public Collection getForeignKeysInGroup(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(TableRecord.class, groupID);
        
        MetadataRecord record = (MetadataRecord) groupID;
        Collection fks = (Collection) record.getPropertyValue(TableRecordProperties.FOREIGNKEYS_IN_GROUP);
        if(fks == null) {
            synchronized(record) {
                // look up the cache again, might have been updated by
                // the thread that just released the lock
                fks = (Collection) record.getPropertyValue(TableRecordProperties.FOREIGNKEYS_IN_GROUP);
                if(fks == null) {
                    fks = this.metadata.getForeignKeysInGroup(groupID);
                    record.setPropertyValue(TableRecordProperties.FOREIGNKEYS_IN_GROUP, fks);
                }
            }    
        }

        return fks;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getPrimaryKeyIDForForeignKeyID(java.lang.Object)
     */
    public Object getPrimaryKeyIDForForeignKeyID(final Object foreignKeyID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(MetadataRecord.class, foreignKeyID);

        MetadataRecord keyRecord = (MetadataRecord) foreignKeyID;
        Object primaryKey = keyRecord.getPropertyValue(ForeignKeyRecordProperties.PRIMARY_KEY_FOR_FK);
        if(primaryKey == null) {
            synchronized(keyRecord) {
                // look up the cache again, might have been updated by
                // the thread that just released the lock
                primaryKey = keyRecord.getPropertyValue(ForeignKeyRecordProperties.PRIMARY_KEY_FOR_FK);
                if(primaryKey == null) {
                    primaryKey = this.metadata.getPrimaryKeyIDForForeignKeyID(foreignKeyID);
                    keyRecord.setPropertyValue(ForeignKeyRecordProperties.PRIMARY_KEY_FOR_FK, primaryKey);
                }
            }    
        }

        return primaryKey;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getAccessPatternsInGroup(java.lang.Object)
     */
    public Collection getAccessPatternsInGroup(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(MetadataRecord.class, groupID);

        MetadataRecord record = (MetadataRecord) groupID;
        Collection accPatterns = (Collection) record.getPropertyValue(TableRecordProperties.ACCESS_PTTRNS_IN_GROUP);
        if(accPatterns == null) {
            synchronized(record) {
                // look up the cache again, might have been updated by
                // the thread that just released the lock
                accPatterns = (Collection) record.getPropertyValue(TableRecordProperties.ACCESS_PTTRNS_IN_GROUP);
                if(accPatterns == null) {
                    accPatterns = this.metadata.getAccessPatternsInGroup(groupID);
                    record.setPropertyValue(TableRecordProperties.ACCESS_PTTRNS_IN_GROUP, accPatterns);
                }
            }    
        }

        return accPatterns;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementIDsInIndex(java.lang.Object)
     */
    public List getElementIDsInIndex(final Object index) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(MetadataRecord.class, index);
        
        MetadataRecord record = (MetadataRecord) index;

        List elementIDs = (List) record.getPropertyValue(ColumnSetRecordProperties.ELEMENTS_IN_INDEX);
        if(elementIDs == null) {
            synchronized(record) {
                // look up the cache again, might have been updated by
                // the thread that just released the lock
                elementIDs = (List) record.getPropertyValue(ColumnSetRecordProperties.ELEMENTS_IN_INDEX);
                if(elementIDs == null) {
                    elementIDs = this.metadata.getElementIDsInIndex(index);
					if(elementIDs != null) {
	                    record.setPropertyValue(ColumnSetRecordProperties.ELEMENTS_IN_INDEX, elementIDs);
	                    for(Iterator elemntIter =  elementIDs.iterator();elemntIter.hasNext();) {
	                        MetadataRecord columnRecord = (MetadataRecord) elemntIter.next();
	                        // Update the cache ...
	                        if (columnRecord != null) {
	                            updateNameToIdCache(columnRecord.getFullName(), columnRecord.getRecordType(), columnRecord.getUUID());
	                            updateIdToRecordCache(columnRecord.getUUID(),columnRecord);
	                        }
	                    }
					}
                }
            }    
        }
        
        return elementIDs;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementIDsInKey(java.lang.Object)
     */
    public List getElementIDsInKey(final Object key) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(MetadataRecord.class, key);
        
        MetadataRecord record = (MetadataRecord) key;

        List elementIDs = (List) record.getPropertyValue(ColumnSetRecordProperties.ELEMENTS_IN_KEY);
        if(elementIDs == null) {
            synchronized(record) {
                // look up the cache again, might have been updated by
                // the thread that just released the lock
                elementIDs = (List) record.getPropertyValue(ColumnSetRecordProperties.ELEMENTS_IN_KEY);
                if(elementIDs == null) {
                    elementIDs = this.metadata.getElementIDsInKey(key);
					if(elementIDs != null) {
	                    record.setPropertyValue(ColumnSetRecordProperties.ELEMENTS_IN_KEY, elementIDs);
	                    for(Iterator elemntIter =  elementIDs.iterator();elemntIter.hasNext();) {
	                        MetadataRecord columnRecord = (MetadataRecord) elemntIter.next();
	                        // Update the cache ...
	                        if (columnRecord != null) {
	                            updateNameToIdCache(columnRecord.getFullName(), columnRecord.getRecordType(), columnRecord.getUUID());
	                            updateIdToRecordCache(columnRecord.getUUID(),columnRecord);
	                        }
	                    }
					}
                }
            }    
        }
        
        return elementIDs;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementIDsInAccessPattern(java.lang.Object)
     */
    public List getElementIDsInAccessPattern(final Object accessPattern) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(MetadataRecord.class, accessPattern);
        
        MetadataRecord record = (MetadataRecord) accessPattern;

        List elementIDs = (List) record.getPropertyValue(ColumnSetRecordProperties.ELEMENTS_IN_ACCESS_PTTRN);
        if(elementIDs == null) {
            synchronized(record) {
                // look up the cache again, might have been updated by
                // the thread that just released the lock
                elementIDs = (List) record.getPropertyValue(ColumnSetRecordProperties.ELEMENTS_IN_ACCESS_PTTRN);
                if(elementIDs == null) {
                    elementIDs = this.metadata.getElementIDsInAccessPattern(accessPattern);
					if(elementIDs != null) {
	                    record.setPropertyValue(ColumnSetRecordProperties.ELEMENTS_IN_ACCESS_PTTRN, elementIDs);
	                    for(Iterator elemntIter =  elementIDs.iterator();elemntIter.hasNext();) {
	                        MetadataRecord columnRecord = (MetadataRecord) elemntIter.next();
	                        // Update the cache ...
	                        if (columnRecord != null) {
	                            updateNameToIdCache(columnRecord.getFullName(), columnRecord.getRecordType(), columnRecord.getUUID());
	                            updateIdToRecordCache(columnRecord.getUUID(),columnRecord);
	                        }
	                    }
					}
                }
            }    
        }
        
        return elementIDs;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#isXMLGroup(java.lang.Object)
     */
    public boolean isXMLGroup(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.isXMLGroup(groupID);
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getMappingNode(java.lang.Object)
     */
    public MappingNode getMappingNode(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(TableRecord.class, groupID);

        MetadataRecord record = (MetadataRecord) groupID;

        MappingDocument mappingNode = (MappingDocument) record.getPropertyValue(TableRecordProperties.MAPPING_NODE_FOR_RECORD);
        if(mappingNode == null) {
            synchronized(record) {
                // look up the cache again, might have been updated by
                // the thread that just released the lock
                mappingNode = (MappingDocument) record.getPropertyValue(TableRecordProperties.MAPPING_NODE_FOR_RECORD);
                if(mappingNode == null) {
                    mappingNode = (MappingDocument)this.metadata.getMappingNode(groupID);
                    record.setPropertyValue(TableRecordProperties.MAPPING_NODE_FOR_RECORD, mappingNode);
                }
            }
        }

        return (MappingNode) mappingNode.clone();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getVirtualDatabaseName()
     */
    public String getVirtualDatabaseName() throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.getVirtualDatabaseName();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getXMLTempGroups(java.lang.Object)
     */
    public Collection getXMLTempGroups(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(TableRecord.class, groupID);

        MetadataRecord record = (MetadataRecord) groupID;

        Collection tempGroups = (Collection) record.getPropertyValue(TableRecordProperties.TEMPORARY_GROUPS_FOR_DOCUMENT);
        if(tempGroups == null) {
            synchronized(record) {
                // look up the cache again, might have been updated by
                // the thread that just released the lock
                tempGroups = (Collection) record.getPropertyValue(TableRecordProperties.TEMPORARY_GROUPS_FOR_DOCUMENT);
                if(tempGroups == null) {
                    tempGroups = this.metadata.getXMLTempGroups(groupID);
                    record.setPropertyValue(TableRecordProperties.TEMPORARY_GROUPS_FOR_DOCUMENT, tempGroups);
                }
            }
        }

        return tempGroups;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getCardinality(java.lang.Object)
     */
    public int getCardinality(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.getCardinality(groupID);
    }

    public List getXMLSchemas(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(TableRecord.class, groupID);

        MetadataRecord record = (MetadataRecord) groupID;

        List schemas = (List) record.getPropertyValue(TableRecordProperties.SCHEMAS_FOR_DOCUMENT);
        if(schemas == null) {
            synchronized(record) {
                // look up the cache again, might have been updated by
                // the thread that just released the lock
                schemas = (List) record.getPropertyValue(TableRecordProperties.SCHEMAS_FOR_DOCUMENT);
                if(schemas == null) {
                    schemas = this.metadata.getXMLSchemas(groupID);
                    record.setPropertyValue(TableRecordProperties.SCHEMAS_FOR_DOCUMENT, schemas);
                }
            }
        }

        return schemas;
    }

    public String getNameInSource(final Object metadataID) throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.getNameInSource(metadataID);
    }

    public int getElementLength(final Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.getElementLength(elementID);
    }

    /* 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getExtensionProperties(java.lang.Object)
     */
    public Properties getExtensionProperties(final Object metadataID)
        throws MetaMatrixComponentException, QueryMetadataException {

        ArgCheck.isInstanceOf(MetadataRecord.class, metadataID);

        MetadataRecord record = (MetadataRecord) metadataID;

        Properties extentions = (Properties) record.getPropertyValue(MetadataRecordProperties.EXTENSIONS_FOR_RECORD);
        if(extentions == null) {
            synchronized(record) {
                // look up the cache again, might have been updated by
                // the thread that just released the lock
                extentions = (Properties) record.getPropertyValue(MetadataRecordProperties.EXTENSIONS_FOR_RECORD);
                if(extentions == null) {
                    extentions = this.metadata.getExtensionProperties(metadataID);
                    record.setPropertyValue(MetadataRecordProperties.EXTENSIONS_FOR_RECORD, extentions);
                }
            }
        }

        return extentions;
    }

    
    public String getNativeType(final Object elementID) throws MetaMatrixComponentException,
                                                 QueryMetadataException {
        return this.metadata.getNativeType(elementID);
    }

    public byte[] getBinaryVDBResource(String resourcePath) throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }

    public String getCharacterVDBResource(String resourcePath) throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }

    public String[] getVDBResourcePaths() throws MetaMatrixComponentException, QueryMetadataException {
        return null;
    }
    
    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getModeledType(java.lang.Object)
     * @since 5.0
     */
    public String getModeledType(Object elementID) throws MetaMatrixComponentException,
                                                  QueryMetadataException {
        
        return this.metadata.getModeledType(elementID);
    }
    
    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getModeledBaseType(java.lang.Object)
     * @since 5.0
     */
    public String getModeledBaseType(Object elementID) throws MetaMatrixComponentException,
                                                      QueryMetadataException {
        
        return this.metadata.getModeledBaseType(elementID);
    }

    /** 
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getModeledPrimitiveType(java.lang.Object)
     * @since 5.0
     */
    public String getModeledPrimitiveType(Object elementID) throws MetaMatrixComponentException,
                                                      QueryMetadataException {
        
        return this.metadata.getModeledPrimitiveType(elementID);
    }
    
    public boolean isTemporaryTable(final Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        return this.metadata.isTemporaryTable(groupID);
    }
    
    // ==================================================================================
    //                      P U B L I C   M E T H O D S
    // ==================================================================================

    /**
     * Return the IndexSelector reference
     * @return
     */
    public TransformationMetadata getDelegate() {
        return this.metadata;
    }

    // ==================================================================================
    //                         P R I V A T E   M E T H O D S
    // ==================================================================================

    private MetadataRecord getRecordByName(final String fullname, final char recordType) {
        Assertion.isNotZeroLength(fullname);

        // Check the cache for the identifier corresponding to this name ...
        Object id = this.nameToIdCache.get(getLookupKey(fullname, recordType));

        // If the identifier was found then check the cache for the record object for this identifier ...
        if (id != null) {
            return getRecordByID(id);
        }
        return null;
    }

    private String getFullNameByPartialName(final String partialName, final char recordType) {
        Assertion.isNotZeroLength(partialName);

        // Check the cache for the identifier corresponding to this partialname ...
        return this.partialNameToFullNameCache.get(getLookupKey(partialName, recordType));
    }

    private MetadataRecord getRecordByID(final Object id) {
        Assertion.isNotNull(id);
        return this.idToRecordCache.get(id);
    }

    private void updateNameToIdCache(final String fullName, final char recordType, final Object id) {
        if (!StringUtil.isEmpty(fullName) && id != null) {
            this.nameToIdCache.put(getLookupKey(fullName, recordType),id);
        }
    }

    private void updateIdToRecordCache(final Object id, final MetadataRecord record) {
        if (id != null && record != null) {
            this.idToRecordCache.put(id,record);
        }
    }

    private void updatePartialNameToFullName(final String partialName, final String fullName, final char recordType) {
        if (!StringUtil.isEmpty(partialName) && !StringUtil.isEmpty(fullName)) {
            this.partialNameToFullNameCache.put(getLookupKey(partialName, recordType), fullName);
        }
    }
    
    private String getLookupKey(final String name, final char recordType) {
        return name.toUpperCase()+recordType;
    }

	@Override
	public Object addToMetadataCache(Object metadataID, String key, Object value)
			throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(MetadataRecord.class, metadataID);
        if (key.startsWith(GroupInfo.CACHE_PREFIX)) {
        	return this.groupInfoCache.put(metadataID + "/" + key, value); //$NON-NLS-1$
        }
    	MetadataRecord record = (MetadataRecord)metadataID; 
        synchronized (record) {
        	Object result = record.getPropertyValue(key);
            record.setPropertyValue(key, value);
            return result;
		}
	}

	@Override
	public Object getFromMetadataCache(Object metadataID, String key)
			throws MetaMatrixComponentException, QueryMetadataException {
        ArgCheck.isInstanceOf(MetadataRecord.class, metadataID);
        if (key.startsWith(GroupInfo.CACHE_PREFIX)) {
        	return this.groupInfoCache.get(metadataID + "/" + key); //$NON-NLS-1$
        }
    	MetadataRecord record = (MetadataRecord)metadataID; 
        synchronized (record) {
        	return record.getPropertyValue(key);
		}
	}

	@Override
	public boolean isScalarGroup(Object groupID)
			throws MetaMatrixComponentException, QueryMetadataException {
		return this.metadata.isScalarGroup(groupID);
	}

}