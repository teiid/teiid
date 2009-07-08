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

package org.teiid.connector.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.teiid.connector.metadata.runtime.AbstractMetadataRecord;
import org.teiid.connector.metadata.runtime.DatatypeRecordImpl;
import org.teiid.connector.metadata.runtime.ModelRecordImpl;
import org.teiid.connector.metadata.runtime.PropertyRecordImpl;
import org.teiid.metadata.index.CharOperation;
import org.teiid.metadata.index.IndexConstants;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.common.vdb.api.SystemVdbUtility;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.dqp.service.VDBService;


/** 
 * This is used to post process metadata records found by querying index files available
 * to the metadata connector. Some of the post processing steps:
 * 1) VdbRecords get wrapped into an object that has the name and version used by the user to logon.
 * 2) Update ModelRecods with the visibility info from VDBService.
 * 3) Filter FileRecords that are not visible.
 * 4) Apply serch criteria to take care of case sensitive matches that get ingnore when querying indexes.    
 * @since 4.3
 */
public class MetadataResultsPostProcessor {

    // real vdb information
    private final String vdbName;
    private final String vdbVersion;
    private final VDBService vdbService;

    /**
     * Constructor MetadataResultsPostProcessor
     * @param context
     * @since 4.3
     */
    public MetadataResultsPostProcessor(final VdbMetadataContext context) {
        this.vdbName = context.getVdbName();
        this.vdbVersion = context.getVdbVersion();
        this.vdbService = context.getVdbService();
        Assertion.isNotNull(this.vdbName);
        Assertion.isNotNull(this.vdbVersion);
    }
    
    /**
     * Post process metadata records, to update record information and filter out recods. 
     * @param records Collection of MetadataRecods from querying index files
     * @param searchCriteria Map of fieldNames to MetadataSearchCriteria objects used to search
     * @param needSearchFiltering boolean to indicate to post processing of metadata records is
     * needed by applying search criteria
     * @return Collection of processed MetadataRecods.
     * @since 4.3
     */
    public Collection processMetadataRecords(final String indexFileName, final Collection records, final Map searchCriteria, final boolean needSearchFiltering) {
        ArgCheck.isNotNull(records);
        ArgCheck.isNotNull(searchCriteria);
        ArgCheck.isNotNull(indexFileName);

        // short circuit, no need to walk records for following conditions
        if(!needSearchFiltering) {
            if(!indexFileName.equalsIgnoreCase(IndexConstants.INDEX_NAME.VDBS_INDEX) &&
               !indexFileName.equalsIgnoreCase(IndexConstants.INDEX_NAME.MODELS_INDEX) &&
               !indexFileName.equalsIgnoreCase(IndexConstants.INDEX_NAME.FILES_INDEX)) {
                return records;
            }
        }

        Collection processedRecods = new ArrayList(records.size());
        for (Iterator iterator = records.iterator(); iterator.hasNext(); ) {
            AbstractMetadataRecord record = (AbstractMetadataRecord) iterator.next();

            // filterout private fileRecords
            if (record instanceof FileRecordImpl) {
                FileRecordImpl fileRecord = filterFileRecordsByVisibility((FileRecordImpl)record);
                if(fileRecord != null) {
                    processedRecods.add(fileRecord);
                }
                continue;
            }

            // if its a modelRecord update visibility with the one available on
            // vdb service
            if(record instanceof ModelRecordImpl) {
            	AbstractMetadataRecord modelRecord = getModelRecordWithUpdatedVisibility((ModelRecordImpl)record);
                if(modelRecord != null) {
                    record = modelRecord;
                }
            }

            // apply search criteria and filterout unmatched records
            if(needSearchFiltering) {
                AbstractMetadataRecord filteredRecord = filterBySearchCriteria(record, searchCriteria);
                if(filteredRecord != null) {
                    processedRecods.add(filteredRecord);
                    continue;
                }
            } else {
                processedRecods.add(record);                
            }
        }
        
        return processedRecods;
    }
    
    /**
     * Update the ModelRecord with the visibility from vdb service, if vdb service is not available
     * there is nothing to update.
     * @param record The ModelRecord object.
     * @return The updated ModelRecord
     * @since 4.3
     */
    public ModelRecordImpl getModelRecordWithUpdatedVisibility(final ModelRecordImpl record) {
        if(record instanceof ModelRecordImpl){
            //set visibility
            ModelRecordImpl mRecord = (ModelRecordImpl)record;
            String modelName = mRecord.getName();   
            if(!SystemVdbUtility.isSystemModelWithSystemTableType(modelName)){
                int visibility = ModelInfo.PUBLIC;
                if(this.vdbService != null) {
                    try {
                        visibility = vdbService.getModelVisibility(vdbName, vdbVersion, modelName);
                    } catch (MetaMatrixComponentException e) {
                        throw new MetaMatrixRuntimeException(e);
                    }
                }
                mRecord.setVisible(visibility == ModelInfo.PUBLIC);
                return mRecord;
            }
        }
        return null;
    }
    
    /**
     * Filter the FileReod based on visibility, if visible return record else
     * return null. 
     * @param record The FileRecord object
     * @return The visible FileReord
     * @since 4.3
     */
    private FileRecordImpl filterFileRecordsByVisibility(final FileRecordImpl record) {
        // begin with assumption that all files are public
        int visibility = ModelInfo.PUBLIC;
        // if its a model file check visibility
        if(this.vdbService != null) {        
            try {
                visibility = vdbService.getFileVisibility(vdbName, vdbVersion, record.getPathInVdb());
            } catch (MetaMatrixComponentException e) {
                throw new MetaMatrixRuntimeException(e);
            }
        }
        // add only visible records
        if(visibility == ModelInfo.PUBLIC) {
            // file records are special always addem to the result
            return record;
        }

        return null;
    }

    /**
     * Apply the given search criteria and filter out records, currently used to apply case sensitive matches
     * since records matched in indexes are case insensitive matches. 
     * @param record The metadata reord to be filterd
     * @param searchCriteria Map of fieldNames to MetadataSearchCriteria objects used to search
     * @return The record that passed filtering
     * @since 4.3
     */
    public AbstractMetadataRecord filterBySearchCriteria(final AbstractMetadataRecord record, final Map searchCriteria) {
        if(!searchCriteria.isEmpty()) {
            // appply criteria for generic metadata record
            if(!applyMetadataRecordCriteria(record, searchCriteria)) {
                return null;
            }
            // appply criteria for datatype record
            if(record instanceof DatatypeRecordImpl) {
                if(!applyDatatypeRecordCriteria((DatatypeRecordImpl)record, searchCriteria)) {
                    return null;
                }                
            }
            // appply criteria for property record
            if(record instanceof PropertyRecordImpl) {
                if(!applyPropertyRecordCriteria((PropertyRecordImpl)record, searchCriteria)) {
                    return null;
                }                
            }
        }
        // if record type is not available or criteria satisfied apply other criteria
        return record;
    }
    
    /**
     * Apply the given search criteria and filter out datatype records, currently used to apply case sensitive matches
     * since records matched in indexes are case insensitive matches. 
     * @param record The metadata reord to be filterd
     * @param searchCriteria Map of fieldNames to MetadataSearchCriteria objects used to search
     * @return The record that passed filtering
     * @since 4.3
     */
    private boolean applyDatatypeRecordCriteria(final DatatypeRecordImpl record, final Map searchCriteria) {
        // runtimetype criteria
        MetadataSearchCriteria runtypeCriteria = getSearchCriteria(searchCriteria, DatatypeRecordImpl.MetadataFieldNames.RUN_TYPE_NAME);
        if(!applyCriteria(runtypeCriteria, record.getRuntimeTypeName())) {
            return false;
        }
        // basetype id criteria
        MetadataSearchCriteria baseTypeCriteria = getSearchCriteria(searchCriteria, DatatypeRecordImpl.MetadataFieldNames.BASE_TYPE_UUID);
        if(!applyCriteria(baseTypeCriteria, record.getBasetypeID())) {
            return false;
        }
        // datatype id criteria
        MetadataSearchCriteria dataTypeCriteria = getSearchCriteria(searchCriteria, DatatypeRecordImpl.MetadataFieldNames.DATA_TYPE_UUID);
        if(!applyCriteria(dataTypeCriteria, record.getDatatypeID())) {
            return false;
        }        
        return true;
    }
    
    /**
     * Apply the given search criteria and filter out property records, currently used to apply case sensitive matches
     * since records matched in indexes are case insensitive matches. 
     * @param record The metadata reord to be filterd
     * @param searchCriteria Map of fieldNames to MetadataSearchCriteria objects used to search
     * @return The record that passed filtering
     * @since 4.3
     */
    private boolean applyPropertyRecordCriteria(final PropertyRecordImpl record, final Map searchCriteria) {
        // prop name criteria
        MetadataSearchCriteria propNameCriteria = getSearchCriteria(searchCriteria, PropertyRecordImpl.MetadataFieldNames.PROPERTY_NAME_FIELD);
        if(!applyCriteria(propNameCriteria, record.getPropertyName())) {
            return false;
        }
        // prop value criteria
        MetadataSearchCriteria propValueCriteria = getSearchCriteria(searchCriteria, PropertyRecordImpl.MetadataFieldNames.PROPERTY_VALUE_FIELD);
        if(!applyCriteria(propValueCriteria, record.getPropertyValue())) {
            return false;
        }
        return true;
    }
    
    /**
     * Apply the given search criteria and filter out metadata records, currently used to apply case sensitive matches
     * since records matched in indexes are case insensitive matches. 
     * @param record The metadata reord to be filterd
     * @param searchCriteria Map of fieldNames to MetadataSearchCriteria objects used to search
     * @return The record that passed filtering
     * @since 4.3
     */
    private boolean applyMetadataRecordCriteria(final AbstractMetadataRecord record, final Map searchCriteria) {
        // apply record type criteria first
        MetadataSearchCriteria recordTypeCriteria = getSearchCriteria(searchCriteria, AbstractMetadataRecord.MetadataFieldNames.RECORD_TYPE_FIELD);
        if(!applyCriteria(recordTypeCriteria, new Character(record.getRecordType()))) {
            return false;
        }
        // full name criteria
        MetadataSearchCriteria fullNameCriteria = getSearchCriteria(searchCriteria, AbstractMetadataRecord.MetadataFieldNames.FULL_NAME_FIELD);
        if(!applyCriteria(fullNameCriteria, record.getFullName())) {
            return false;
        }
        // name criteria
        MetadataSearchCriteria nameCriteria = getSearchCriteria(searchCriteria, AbstractMetadataRecord.MetadataFieldNames.NAME_FIELD);
        if(!applyCriteria(nameCriteria, record.getName())) {
            return false;
        }
        // model name criteria
        MetadataSearchCriteria modelNameCriteria = getSearchCriteria(searchCriteria, AbstractMetadataRecord.MetadataFieldNames.MODEL_NAME_FIELD);
        if(!applyCriteria(modelNameCriteria, record.getModelName())) {
            return false;
        }
        // nameInSource criteria
        MetadataSearchCriteria nameInSourceCriteria = getSearchCriteria(searchCriteria, AbstractMetadataRecord.MetadataFieldNames.NAME_IN_SOURCE_FIELD);
        if(!applyCriteria(nameInSourceCriteria, record.getNameInSource())) {
            return false;
        }
        // uuid criteria
        MetadataSearchCriteria uuidCriteria = getSearchCriteria(searchCriteria, AbstractMetadataRecord.MetadataFieldNames.UUID_FIELD);
        if(!applyCriteria(uuidCriteria, record.getUUID())) {
            return false;
        }
        // parent uuid criteria        
        MetadataSearchCriteria parentUuidCriteria = getSearchCriteria(searchCriteria, AbstractMetadataRecord.MetadataFieldNames.PARENT_UUID_FIELD);
        if(!applyCriteria(parentUuidCriteria, record.getParentUUID())) {
            return false;
        }        
        return true;
    }
    
    /**
     * Get the search criteria given the fild name 
     * @param searchCriteria Map of fieldNames to MetadataSearchCriteria objects used to search
     * @param key The field name
     * @return The search criteria onj.
     * @since 4.3
     */
    private MetadataSearchCriteria getSearchCriteria(final Map searchCriteria, final String key) {
        return (MetadataSearchCriteria) searchCriteria.get(key.toUpperCase());
    }    
    
    /**
     * Apply the search criteria on the given value, try to match the criteria to value. 
     * @param criteria The search criteria the value needs to satisfy
     * @param value The value from a metadata record
     * @return true if it satisfies the criteria else false
     * @since 4.3
     */
    private boolean applyCriteria(final MetadataSearchCriteria criteria, final Object value) {
        boolean criteriaPassed = true;
        // post processing literal criteria
        if(criteria instanceof MetadataLiteralCriteria) {
            MetadataLiteralCriteria literalCriteria = (MetadataLiteralCriteria) criteria;
            // get the valu in criteria with any functions on the value side of criteria
            // already evaluated
            Object evaluatedLiteral = literalCriteria.getEvaluatedValue();
            // if both are string apply case checks
            if(value instanceof String && evaluatedLiteral instanceof String) {
                String literalString = evaluatedLiteral.toString();
                String valueString = value.toString();
                // if no fucnction just do case sensitive match
                if(!CharOperation.match(literalString.toCharArray(), valueString.toCharArray(), !literalCriteria.hasFieldWithCaseFunctions())) {
                    criteriaPassed = false;
                }
            } else {
                // non string just check if they equal
                if(value != null && !value.equals(evaluatedLiteral)) {
                    criteriaPassed = false;
                } else if(value == null && evaluatedLiteral != null) {
                    criteriaPassed = false;
                }
            }
        // post processing literal criteria
        } 
        return criteriaPassed;
    }
}
