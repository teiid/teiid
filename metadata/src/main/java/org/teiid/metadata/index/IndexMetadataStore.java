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

package org.teiid.metadata.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.connector.metadata.runtime.AbstractMetadataRecord;
import org.teiid.connector.metadata.runtime.ColumnRecordImpl;
import org.teiid.connector.metadata.runtime.ColumnSetRecordImpl;
import org.teiid.connector.metadata.runtime.DatatypeRecordImpl;
import org.teiid.connector.metadata.runtime.ForeignKeyRecordImpl;
import org.teiid.connector.metadata.runtime.MetadataConstants;
import org.teiid.connector.metadata.runtime.ModelRecordImpl;
import org.teiid.connector.metadata.runtime.ProcedureParameterRecordImpl;
import org.teiid.connector.metadata.runtime.ProcedureRecordImpl;
import org.teiid.connector.metadata.runtime.PropertyRecordImpl;
import org.teiid.connector.metadata.runtime.TableRecordImpl;
import org.teiid.connector.metadata.runtime.TransformationRecordImpl;
import org.teiid.core.index.IEntryResult;
import org.teiid.internal.core.index.Index;
import org.teiid.metadata.RuntimeMetadataPlugin;
import org.teiid.metadata.TransformationMetadata;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.id.UUID;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.metadata.runtime.api.MetadataSource;
import com.metamatrix.query.metadata.MetadataStore;

/**
 * Loads MetadataRecords from index files.  
 * Only datatypes are directly cached.
 */
public class IndexMetadataStore implements MetadataStore {

	private Index[] indexes;
    private Map<String, DatatypeRecordImpl> datatypeCache;

    public IndexMetadataStore(MetadataSource source) throws IOException {
    	ArrayList<Index> tmp = new ArrayList<Index>();
		for (String fileName : source.getEntries()) {
			if (SimpleIndexUtil.isIndexFile(fileName)) {
				File f = source.getFile(fileName);
	            tmp.add( new Index(f.getAbsolutePath(), true) );
	        } 
		}
		this.indexes = tmp.toArray(new Index[tmp.size()]);
    }
    
    @Override
    public boolean postProcessFindMetadataRecords() {
    	return false;
    }
    
    @Override
    public Collection<String> getModelNames() {
    	Collection<ModelRecordImpl> records;
		try {
			records = findMetadataRecords(MetadataConstants.RECORD_TYPE.MODEL, null, false);
		} catch (MetaMatrixComponentException e) {
			throw new MetaMatrixRuntimeException(e);
		}
    	List<String> result = new ArrayList<String>(records.size());
    	for (ModelRecordImpl modelRecord : records) {
			result.add(modelRecord.getName());
		}
    	return result;
    }
    
    public ModelRecordImpl getModel(String name) throws QueryMetadataException, MetaMatrixComponentException {
    	return (ModelRecordImpl)getRecordByType(name, MetadataConstants.RECORD_TYPE.MODEL);
    }
    
    @Override
    public TableRecordImpl findGroup(String groupName) throws QueryMetadataException, MetaMatrixComponentException {
        TableRecordImpl tableRecord = (TableRecordImpl)getRecordByType(groupName, MetadataConstants.RECORD_TYPE.TABLE);
    	List<ColumnRecordImpl> columns = new ArrayList<ColumnRecordImpl>(findChildRecords(tableRecord, MetadataConstants.RECORD_TYPE.COLUMN, true));
        for (ColumnRecordImpl columnRecordImpl : columns) {
    		columnRecordImpl.setDatatype(getDatatypeCache().get(columnRecordImpl.getDatatypeUUID()));
		}
        Collections.sort(columns);
        tableRecord.setColumns(columns);
        tableRecord.setAccessPatterns(findChildRecords(tableRecord, MetadataConstants.RECORD_TYPE.ACCESS_PATTERN, true));
        Map<String, ColumnRecordImpl> uuidColumnMap = new HashMap<String, ColumnRecordImpl>();
        for (ColumnRecordImpl columnRecordImpl : columns) {
			uuidColumnMap.put(columnRecordImpl.getUUID(), columnRecordImpl);
		}
        for (ColumnSetRecordImpl columnSetRecordImpl : tableRecord.getAccessPatterns()) {
			loadColumnSetRecords(columnSetRecordImpl, uuidColumnMap);
		}
        tableRecord.setForiegnKeys(findChildRecords(tableRecord, MetadataConstants.RECORD_TYPE.FOREIGN_KEY, true));
        for (ForeignKeyRecordImpl foreignKeyRecord : tableRecord.getForeignKeys()) {
        	(foreignKeyRecord).setPrimaryKey((ColumnSetRecordImpl)this.getRecordByType(foreignKeyRecord.getUniqueKeyID(), MetadataConstants.RECORD_TYPE.PRIMARY_KEY));
        	loadColumnSetRecords(foreignKeyRecord, uuidColumnMap);
		}
        tableRecord.setUniqueKeys(findChildRecords(tableRecord, MetadataConstants.RECORD_TYPE.UNIQUE_KEY, true));
        for (ColumnSetRecordImpl columnSetRecordImpl : tableRecord.getUniqueKeys()) {
			loadColumnSetRecords(columnSetRecordImpl, uuidColumnMap);
		}
        if (tableRecord.getPrimaryKeyID() != null) {
        	ColumnSetRecordImpl primaryKey = (ColumnSetRecordImpl)getRecordByType(tableRecord.getPrimaryKeyID(), MetadataConstants.RECORD_TYPE.PRIMARY_KEY);
        	loadColumnSetRecords(primaryKey, uuidColumnMap);
        	tableRecord.setPrimaryKey(primaryKey);
        }
        if (tableRecord.isVirtual()) {
        	TransformationRecordImpl update = (TransformationRecordImpl)getRecordByType(groupName, MetadataConstants.RECORD_TYPE.UPDATE_TRANSFORM,false);
	        if (update != null) {
	        	tableRecord.setUpdatePlan(update.getTransformation());
	        }
	        TransformationRecordImpl insert = (TransformationRecordImpl)getRecordByType(groupName, MetadataConstants.RECORD_TYPE.INSERT_TRANSFORM,false);
	        if (insert != null) {
	        	tableRecord.setInsertPlan(insert.getTransformation());
	        }
	        TransformationRecordImpl delete = (TransformationRecordImpl)getRecordByType(groupName, MetadataConstants.RECORD_TYPE.DELETE_TRANSFORM,false);
	        if (delete != null) {
	        	tableRecord.setDeletePlan(delete.getTransformation());
	        }
	        TransformationRecordImpl select = (TransformationRecordImpl)getRecordByType(groupName, MetadataConstants.RECORD_TYPE.SELECT_TRANSFORM,false);
	        // this group may be an xml document            
	        if(select == null) {
		        select = (TransformationRecordImpl)getRecordByType(groupName, MetadataConstants.RECORD_TYPE.MAPPING_TRANSFORM,false);
	        }
	        tableRecord.setSelectTransformation(select);
        }
        if (tableRecord.isMaterialized()) {
        	tableRecord.setMaterializedStageTableName(getRecordByType(tableRecord.getMaterializedStageTableID(), MetadataConstants.RECORD_TYPE.TABLE).getFullName());
        	tableRecord.setMaterializedTableName(getRecordByType(tableRecord.getMaterializedTableID(), MetadataConstants.RECORD_TYPE.TABLE).getFullName());
        }
        return tableRecord;
    }

	private Map<String, DatatypeRecordImpl> getDatatypeCache() throws MetaMatrixComponentException {
		if (this.datatypeCache == null) {
			this.datatypeCache = new HashMap<String, DatatypeRecordImpl>();
			Collection<DatatypeRecordImpl> dataTypes = findMetadataRecords(MetadataConstants.RECORD_TYPE.DATATYPE, null, false);
			for (DatatypeRecordImpl datatypeRecordImpl : dataTypes) {
				datatypeCache.put(datatypeRecordImpl.getUUID(), datatypeRecordImpl);
			}
		}
		return datatypeCache;
	}
	
	public Collection<DatatypeRecordImpl> getDatatypes() throws MetaMatrixComponentException {
		return getDatatypeCache().values();
	}
    
    @Override
    public ColumnRecordImpl findElement(String fullName) throws QueryMetadataException, MetaMatrixComponentException {
        ColumnRecordImpl columnRecord = (ColumnRecordImpl)getRecordByType(fullName, MetadataConstants.RECORD_TYPE.COLUMN);
    	columnRecord.setDatatype(getDatatypeCache().get(columnRecord.getDatatypeUUID()));
        return columnRecord;
    }
    
    @Override
    public Collection<String> getGroupsForPartialName(String partialGroupName)
    		throws MetaMatrixComponentException, QueryMetadataException {
        // Query the index files
		Collection<String> tableRecords = findMetadataRecords(MetadataConstants.RECORD_TYPE.TABLE,partialGroupName,true);

		// Extract the fully qualified names to return
		final Collection tableNames = new ArrayList(tableRecords.size());
		for(Iterator recordIter = tableRecords.iterator();recordIter.hasNext();) {
			// get the table record for this result            
			TableRecordImpl tableRecord = (TableRecordImpl) recordIter.next();                    
			tableNames.add(tableRecord.getFullName());
		}
		return tableNames;
    }
    
    private AbstractMetadataRecord getRecordByType(final String entityName, final char recordType) throws MetaMatrixComponentException, QueryMetadataException {
    	return getRecordByType(entityName, recordType, true);
    }
    
    private AbstractMetadataRecord getRecordByType(final String entityName, final char recordType, boolean mustExist) throws MetaMatrixComponentException, QueryMetadataException {
    	// Query the index files
		final Collection results = findMetadataRecords(recordType,entityName,false);
        
		int resultSize = results.size();
        if(resultSize == 1) {
            // get the columnset record for this result            
            return (AbstractMetadataRecord) results.iterator().next();
        }
        if(resultSize == 0) {
        	if (mustExist) {
			// there should be only one for the UUID
	            throw new QueryMetadataException(entityName+TransformationMetadata.NOT_EXISTS_MESSAGE);
        	} 
        	return null;
		} 
        throw new QueryMetadataException(RuntimeMetadataPlugin.Util.getString("TransformationMetadata.0", entityName)); //$NON-NLS-1$
    }
    
    @Override
    public ProcedureRecordImpl getStoredProcedure(
    		String fullyQualifiedProcedureName)
    		throws MetaMatrixComponentException, QueryMetadataException {
    	ProcedureRecordImpl procedureRecord = (ProcedureRecordImpl)getRecordByType(fullyQualifiedProcedureName, MetadataConstants.RECORD_TYPE.CALLABLE);

    	procedureRecord.setParameters(new ArrayList<ProcedureParameterRecordImpl>(procedureRecord.getParameterIDs().size()));
    	
        // get the parameter metadata info
        for (String paramID : procedureRecord.getParameterIDs()) {
            ProcedureParameterRecordImpl paramRecord = (ProcedureParameterRecordImpl) this.getRecordByType(paramID, MetadataConstants.RECORD_TYPE.CALLABLE_PARAMETER);
            paramRecord.setDatatype(getDatatypeCache().get(paramRecord.getDatatypeUUID()));
            procedureRecord.getParameters().add(paramRecord);
        }
    	
        String resultID = procedureRecord.getResultSetID();
        if(resultID != null) {
            try {
                ColumnSetRecordImpl resultRecord = (ColumnSetRecordImpl) this.getRecordByType(resultID, MetadataConstants.RECORD_TYPE.RESULT_SET);
                loadColumnSetRecords(resultRecord, null);
                procedureRecord.setResultSet(resultRecord);
            } catch (QueryMetadataException e) {
                //it is ok to fail here.  it will happen when a 
                //virtual stored procedure is created from a
                //physical stored procedrue without a result set
                //TODO: find a better fix for this
            }
        }

        // if this is a virtual procedure get the procedure plan
        if(procedureRecord.isVirtual()) {
    		TransformationRecordImpl transformRecord = (TransformationRecordImpl)getRecordByType(fullyQualifiedProcedureName, MetadataConstants.RECORD_TYPE.PROC_TRANSFORM, false);
    		if(transformRecord != null) {
    			procedureRecord.setQueryPlan(transformRecord.getTransformation());
    		}
        }

    	return procedureRecord;
    }
    
	@Override
	public Collection getXMLTempGroups(TableRecordImpl table) throws MetaMatrixComponentException {
		// Query the index files
		final Collection results = findChildRecords(table, MetadataConstants.RECORD_TYPE.TABLE, false);
        Collection tempGroups = new HashSet(results.size());
        for(Iterator resultIter = results.iterator();resultIter.hasNext();) {
            TableRecordImpl record = (TableRecordImpl) resultIter.next();
            if(record.getTableType() == MetadataConstants.TABLE_TYPES.XML_STAGING_TABLE_TYPE) {
                tempGroups.add(record);
            }
        }
        return tempGroups;
	}
	
    private Collection findChildRecords(final AbstractMetadataRecord parentRecord, final char childRecordType, boolean filter) throws MetaMatrixComponentException {
        IEntryResult[] results = queryIndexByParentPath(childRecordType, parentRecord.getFullName());
		Collection records = RecordFactory.getMetadataRecord(results);        
		
        if (filter) {
            final String groupUUID = parentRecord.getUUID();
            
            for( Iterator resultsIter = records.iterator(); resultsIter.hasNext(); ) {
                AbstractMetadataRecord record = (AbstractMetadataRecord) resultsIter.next();
                String parentUUID = record.getParentUUID();

                if(parentUUID == null || !parentUUID.equalsIgnoreCase(groupUUID)) {
                    resultsIter.remove();
                }
            }
        }
        
        return records;
    }
    
	private void loadColumnSetRecords(ColumnSetRecordImpl indexRecord, Map<String, ColumnRecordImpl> columns)
			throws MetaMatrixComponentException, QueryMetadataException {
		List uuids = indexRecord.getColumnIDs();
		List<ColumnRecordImpl> columnRecords = new ArrayList<ColumnRecordImpl>(uuids.size());

		for (Iterator uuidIter = uuids.iterator(); uuidIter.hasNext();) {
			String uuid = (String) uuidIter.next();
			if (columns != null) {
				columnRecords.add(columns.get(uuid));
			} else {
				columnRecords.add(findElement(uuid));
			}
		}
		indexRecord.setColumns(columnRecords);
	}
    
    @Override
	public Collection findMetadataRecords(final char recordType,
			final String entityName, final boolean isPartialName)
			throws MetaMatrixComponentException {
		IEntryResult[] results = queryIndex(recordType, entityName, isPartialName);
		Collection records = RecordFactory.getMetadataRecord(results);;
		
		if(StringUtil.startsWithIgnoreCase(entityName,UUID.PROTOCOL)) {		
	        // Filter out ColumnRecord instances that do not match the specified uuid.
	        // Due to the pattern matching used to query index files if an index record
	        // matched the specified uuid string anywhere in that record it would be returned
	        // in the results (for example, if the parent ObjectID in the index record
	        // matched the specified uuid).
			if (entityName != null && records != null) {
	            for (final Iterator iter = records.iterator(); iter.hasNext();) {
	                final AbstractMetadataRecord record = (AbstractMetadataRecord)iter.next();
	                if (record == null || !entityName.equals(record.getUUID())) {
	                    iter.remove();
	                }
	            }
	        }
		}

		return records;
	}
    
    @Override
    public Collection<AbstractMetadataRecord> findMetadataRecords(String indexName,
    		String pattern, boolean isPrefix,
    		boolean isCaseSensitive) throws MetaMatrixCoreException {
    	IEntryResult[] results = SimpleIndexUtil.queryIndex(null, SimpleIndexUtil.getIndexes(indexName, this), pattern==null?null:pattern.toCharArray(), isPrefix, isCaseSensitive, false);
    	return RecordFactory.getMetadataRecord(results);
    }
    
    @Override
    public Collection<PropertyRecordImpl> getExtensionProperties(AbstractMetadataRecord metadataRecord) throws MetaMatrixComponentException {
		// find the entities properties records
		String uuid = metadataRecord.getUUID();
		String prefixString  = getUUIDPrefixPattern(MetadataConstants.RECORD_TYPE.PROPERTY, uuid);

		IEntryResult[] results = queryIndex(MetadataConstants.RECORD_TYPE.PROPERTY, prefixString.toCharArray(), true, true, true);
		
		return RecordFactory.getMetadataRecord(results);
    }
    
    /**
     * Return the pattern match string that could be used to match a UUID in 
     * an index record. All index records contain a header portion of the form:  
     * recordType|pathInModel|UUID|nameInSource|parentObjectID|
     * @param uuid The UUID for which the pattern match string is to be constructed.
     * @return The pattern match string of the form: recordType|*|uuid|*
     */
    private String getUUIDMatchPattern(final char recordType, final String uuid) {
        ArgCheck.isNotNull(uuid);
        String uuidString = uuid;
        if (StringUtil.startsWithIgnoreCase(uuid,UUID.PROTOCOL)) {
            uuidString = uuid.toLowerCase();
        }
        // construct the pattern string
        String patternStr = "" //$NON-NLS-1$
                          + recordType
                          + IndexConstants.RECORD_STRING.RECORD_DELIMITER
                          + IndexConstants.RECORD_STRING.MATCH_CHAR                    
                          + IndexConstants.RECORD_STRING.RECORD_DELIMITER
                          + uuidString
                          + IndexConstants.RECORD_STRING.RECORD_DELIMITER
                          + IndexConstants.RECORD_STRING.MATCH_CHAR;                    
        return patternStr;        
    }
    
	private String getUUIDPrefixPattern(final char recordType, final String uuid) {

		// construct the pattern string
		String patternStr = "" //$NON-NLS-1$
						  + recordType
						  + IndexConstants.RECORD_STRING.RECORD_DELIMITER;
		if(uuid != null) {                          
			patternStr = patternStr + uuid.trim() + IndexConstants.RECORD_STRING.RECORD_DELIMITER;
		}                    

		return patternStr;
	}
    
    /** 
     * @see com.metamatrix.modeler.core.index.IndexSelector#getIndexes()
     * @since 4.2
     */
    public synchronized Index[] getIndexes() {
    	return this.indexes;
    }

    /**
     * Return the array of MtkIndex instances representing temporary indexes
     * @param selector
     * @return
     * @throws QueryMetadataException
     */
    private Index[] getIndexes(final char recordType) throws MetaMatrixComponentException {
    	// The the index file name for the record type
        try {
            final String indexName = SimpleIndexUtil.getIndexFileNameForRecordType(recordType);
            return SimpleIndexUtil.getIndexes(indexName, this);            
        } catch(Exception e) {
            throw new MetaMatrixComponentException(e, RuntimeMetadataPlugin.Util.getString("TransformationMetadata.Error_trying_to_obtain_index_file_using_IndexSelector_1",this)); //$NON-NLS-1$
        }
    }
    
	/**
	 * Return all index file records that match the specified entity name  
	 * @param indexName
	 * @param entityName the name to match
	 * @param isPartialName true if the entity name is a partially qualified
	 * @return results
	 * @throws QueryMetadataException
	 */
	private IEntryResult[] queryIndexByParentPath(final char recordType, final String parentFullName) throws MetaMatrixComponentException {

		// Query based on fully qualified name
		String prefixString  = getParentPrefixPattern(recordType,parentFullName);

		// Query the model index files
		IEntryResult[] results = queryIndex(recordType, prefixString.toCharArray(), true, true, false);

		return results;
	}
	
	/**
	 * Return all index file records that match the specified entity name  
	 * @param indexName
	 * @param entityName the name to match
	 * @param isPartialName true if the entity name is a partially qualified
	 * @return results
	 * @throws QueryMetadataException
	 */
	private IEntryResult[] queryIndex(final char recordType, final String entityName, final boolean isPartialName) throws MetaMatrixComponentException {

		IEntryResult[] results = null;

		// Query based on UUID
		if (StringUtil.startsWithIgnoreCase(entityName,UUID.PROTOCOL)) {
            String patternString = null;
            if (recordType == MetadataConstants.RECORD_TYPE.DATATYPE) {
                patternString = getDatatypeUUIDMatchPattern(entityName);
            } else {
                patternString = getUUIDMatchPattern(recordType,entityName);
            }
			results = queryIndex(recordType, patternString.toCharArray(), false, true, true);
		}

		// Query based on partially qualified name
		else if (isPartialName) {
			String patternString = getMatchPattern(recordType,entityName);
			results = queryIndex(recordType, patternString.toCharArray(), false, true, false);
		}

		// Query based on fully qualified name
		else {
			String prefixString  = getPrefixPattern(recordType,entityName);
			results = queryIndex(recordType, prefixString.toCharArray(), true, true, true);
		}

		return results;
	}
	
    /**
     * Return the pattern match string that could be used to match a UUID in 
     * a datatype index record. The RECORD_TYPE.DATATYPE records contain a header portion of the form:  
     * recordType|datatypeID|basetypeID|fullName|objectID|nameInSource|...
     * @param uuid The UUID for which the pattern match string is to be constructed.
     * @return The pattern match string of the form: recordType|*|*|*|uuid|*
     */
    private String getDatatypeUUIDMatchPattern(final String uuid) {
        ArgCheck.isNotNull(uuid);
        String uuidString = uuid;
        if (StringUtil.startsWithIgnoreCase(uuid,UUID.PROTOCOL)) {
            uuidString = uuid.toLowerCase();
        }
        // construct the pattern string
        String patternStr = "" //$NON-NLS-1$
                          + MetadataConstants.RECORD_TYPE.DATATYPE            //recordType
                          + IndexConstants.RECORD_STRING.RECORD_DELIMITER
                          + IndexConstants.RECORD_STRING.MATCH_CHAR        //datatypeID 
                          + IndexConstants.RECORD_STRING.RECORD_DELIMITER
                          + IndexConstants.RECORD_STRING.MATCH_CHAR        //basetypeID 
                          + IndexConstants.RECORD_STRING.RECORD_DELIMITER
                          + IndexConstants.RECORD_STRING.MATCH_CHAR        //fullName 
                          + IndexConstants.RECORD_STRING.RECORD_DELIMITER
                          + uuidString                                     //objectID
                          + IndexConstants.RECORD_STRING.RECORD_DELIMITER
                          + IndexConstants.RECORD_STRING.MATCH_CHAR;                    
        return patternStr;        
    }
	
    /**
     * Return the prefix match string that could be used to exactly match a fully 
     * qualified entity name in an index record. All index records 
     * contain a header portion of the form:  
     * recordType|pathInModel|UUID|nameInSource|parentObjectID|
     * @param name The fully qualified name for which the prefix match 
     * string is to be constructed.
     * @return The pattern match string of the form: recordType|name|
     */
    private String getParentPrefixPattern(final char recordType, final String name) {

        // construct the pattern string
        String patternStr = "" //$NON-NLS-1$
                          + recordType
                          + IndexConstants.RECORD_STRING.RECORD_DELIMITER;
        if(name != null) {                          
            patternStr = patternStr + name.trim().toUpperCase()+ TransformationMetadata.DELIMITER_CHAR;
        }                    

        return patternStr;
    }
	
    /**
     * Return the prefix match string that could be used to exactly match a fully 
     * qualified entity name in an index record. All index records 
     * contain a header portion of the form:  
     * recordType|pathInModel|UUID|nameInSource|parentObjectID|
     * @param name The fully qualified name for which the prefix match 
     * string is to be constructed.
     * @return The pattern match string of the form: recordType|name|
     */
    private String getPrefixPattern(final char recordType, final String name) {

        // construct the pattern string
        String patternStr = "" //$NON-NLS-1$
                          + recordType
                          + IndexConstants.RECORD_STRING.RECORD_DELIMITER;
        if(name != null) {                          
            patternStr = patternStr + name.trim().toUpperCase() + IndexConstants.RECORD_STRING.RECORD_DELIMITER;
        }                    

        return patternStr;
    }
	
    /**
     * Return the pattern match string that could be used to match a 
     * partially/fully qualified entity name in an index record. All index records 
     * contain a header portion of the form:  
     * recordType|pathInModel|UUID|nameInSource|parentObjectID|
     * @param name The partially/fully qualified name for which
     * the pattern match string is to be constructed.
     * @return The pattern match string of the form: recordType|*name|* 
     */
    private String getMatchPattern(final char recordType, final String name) {
        ArgCheck.isNotNull(name);

        // construct the pattern string
        String patternStr = "" //$NON-NLS-1$
                          + recordType
                          + IndexConstants.RECORD_STRING.RECORD_DELIMITER
                          + IndexConstants.RECORD_STRING.MATCH_CHAR;
        if(name != null) {
            patternStr =  patternStr + name.trim().toUpperCase()
                          + IndexConstants.RECORD_STRING.RECORD_DELIMITER
                          + IndexConstants.RECORD_STRING.MATCH_CHAR;
        }                    
        return patternStr;        
    }

    /**
     * Return all index file records that match the specified record pattern.
     * @param indexes the array of MtkIndex instances to query
     * @param pattern
     * @return results
     * @throws QueryMetadataException
     */
    private IEntryResult[] queryIndex(char recordType, final char[] pattern, boolean isPrefix, boolean isCaseSensitive, boolean returnFirstMatch) throws MetaMatrixComponentException {
        try {
            return SimpleIndexUtil.queryIndex(null, getIndexes(recordType), pattern, isPrefix, isCaseSensitive, returnFirstMatch);
        } catch (MetaMatrixCoreException e) {
            throw new MetaMatrixComponentException(e, e.getMessage());
        }
    }
    
}
