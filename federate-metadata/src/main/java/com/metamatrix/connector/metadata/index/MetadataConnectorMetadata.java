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

package com.metamatrix.connector.metadata.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.connector.metadata.MetadataConnectorConstants;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.index.IEntryResult;
import com.metamatrix.core.util.CharOperation;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.internal.core.index.Index;
import com.metamatrix.metadata.runtime.impl.FileRecordImpl;
import com.metamatrix.metadata.runtime.impl.RecordFactory;
import com.metamatrix.modeler.core.index.IndexConstants;
import com.metamatrix.modeler.core.metadata.runtime.FileRecord;
import com.metamatrix.modeler.core.metadata.runtime.MetadataRecord;
import com.metamatrix.modeler.internal.core.index.SimpleIndexUtil;
import com.metamatrix.modeler.transformation.metadata.ServerRuntimeMetadata;

/**
 * Extends the ServerRuntimeMetadata class with additional methods for querying the indexes.
 */
public class MetadataConnectorMetadata extends ServerRuntimeMetadata {

    // processor for postprocessing metadata results
    private final MetadataResultsPostProcessor processor;
    
    private boolean needSearchPostProcess = false;
    
    /**
     * Constructor MetadataConnectorMetadata.
     * @param context The context object used to pass in info needed by metadata
     * @since 4.3
     */
    public MetadataConnectorMetadata(final VdbMetadataContext context) {
        super(context);
        this.processor = new MetadataResultsPostProcessor(context);
    }

    /**
     * Get the metadataRecords by querying the indexFile with the given table name
     * and a Map of criteria to be applied to limit the results.
     * @param tableName The name of index file with/out a record seperator
     * @param criteria Map of fieldNames to MetadataSearchCriteria objects
     * @return Collection of metadata records.
     * @since 4.3
     */
    public Collection getObjects(final String tableName, final Map criteria) {
        // name of index file could be same as table name (name in source) 
        String indexFileName = tableName;
        // check if there is record type seperator
        int separatorLocation = tableName.indexOf(MetadataConnectorConstants.RECORD_TYPE_SEPERATOR);
        // if there is a seperator 
        if(separatorLocation != -1) {
            // arrive at the correct index file name
            indexFileName = tableName.substring(0,separatorLocation);
            // get the record type
            char recordType = tableName.substring(separatorLocation+1).charAt(0);
            // build a search criteria
            MetadataSearchCriteria recordTypeCriteria = new MetadataLiteralCriteria(MetadataRecord.MetadataFieldNames.RECORD_TYPE_FIELD, new Character(recordType));
            // update the criteria map
            criteria.put(MetadataRecord.MetadataFieldNames.RECORD_TYPE_FIELD.toUpperCase(), recordTypeCriteria);
        }

        // search for metadata records given the indexFileName and criteria
        try {
            // initialize the post processing flag to false before very search
            this.needSearchPostProcess = false;
            Collection results = findMetadataRecords(indexFileName, criteria, false);
            // post process results
            if(!results.isEmpty()) {
                return processor.processMetadataRecords(indexFileName, results, criteria, this.needSearchPostProcess);
            }
        } catch (MetaMatrixComponentException e) {
            throw new MetaMatrixRuntimeException(e);
        }
        
        return Collections.EMPTY_LIST;        
    }
    
    /**
     * Return all index records in the index file that match the given critera.
     * @param indexName The name of the index file to be searched
     * @param criteria Map of fieldNames to MetadataSearchCriteria objects used to search
     * @param returnFirstMatch Boolean indicating if first match is to be returned
     * @return Collection of metadata records
     * @throws QueryMetadataException
     */
    protected Collection findMetadataRecords(final String indexName, final Map criteria, boolean returnFirstMatch) throws MetaMatrixComponentException {
        
        // if file records are needed no need to query index files, these are
        // based on paths of files in vdbs
        if(indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.FILES_INDEX)) {
            return getFileRecords(criteria, returnFirstMatch);
        }

        try {
            // get indexes for the given index name
            Index[] indexes = SimpleIndexUtil.getIndexes(indexName, super.getIndexSelector());
            // if there are indexes search them            
            if(indexes != null && indexes.length > 0) {
                Collection criteriaCollection = IndexCriteriaBuilder.getLiteralCriteria(criteria);
                // collect prefixes or patterns based on criteria
                Collection prefixes = null, patterns = null;
                for(final Iterator critIter = criteriaCollection.iterator(); critIter.hasNext();) {
                    Map literalCriteria = (Map) critIter.next();
                    // short circuit if there is a false criteria
                    if(!hasFalseCriteria(criteria)) {
                        // get a prefix from the criteria if possible
                        String prefixPattern = IndexCriteriaBuilder.getMatchPrefix(indexName, literalCriteria);
                        // if cannot build a prefix, build a match pattern instead
                        if(prefixPattern == null) {
                            if(patterns == null) {
                                patterns = new ArrayList(criteriaCollection.size());
                            }
                            String matchPattern = IndexCriteriaBuilder.getMatchPattern(indexName, literalCriteria);
                            patterns.add(matchPattern);
                        } else {
                            if(prefixes == null) {
                                prefixes = new ArrayList(criteriaCollection.size());
                            }
                            prefixes.add(prefixPattern);
                        }
                    }
                }
                // find results from the collection of prefix match patterns
                // or pattern match patterns
                IEntryResult[] results = null;
                // check if any case functions are used
                boolean hasCaseFunctions = hasCaseFunctions(criteria);
                // no prefixes
                if(patterns != null) {
                    // if no case functions involved do a case sensitive match
                    if(!hasCaseFunctions) {
                        results = SimpleIndexUtil.queryIndex(null, indexes, patterns, false, true, returnFirstMatch);
                    } else {
                        // do case insensitive match
                        results = SimpleIndexUtil.queryIndex(null, indexes, patterns, false, false, returnFirstMatch);
                    }
                } else if(prefixes != null) {
                    // prefix match is always case insensitive (since names in our prefixes are upper cased)
                    // filter case mismatches in post processing
                    results = SimpleIndexUtil.queryIndex(null, indexes, prefixes, true, true, returnFirstMatch);
                    if(!hasCaseFunctions) {
                        // need post processign since prefix search is case insensitive
                        // if only criteria is record type criteria no post processing needed
                        if(!criteria.isEmpty()) {                        
                            if(criteria.size() == 1 &&  criteria.get(MetadataRecord.MetadataFieldNames.RECORD_TYPE_FIELD.toUpperCase()) != null) {                        
                                needSearchPostProcess = false;
                            } else { 
                                needSearchPostProcess = true;
                            }
                        }
                    }
                }
                // if there are results get records
                if (results != null && results.length > 0) {
                    return getMetadataRecords(results, criteria, hasCaseFunctions);
                }                
            }
        } catch (MetaMatrixCoreException e) {
            throw new MetaMatrixComponentException(e, e.getMessage());
        }
        return Collections.EMPTY_LIST;
    }
    
    /**
     * Return the collection of MetadataRecord objects built from the specified array of IEntryResult.
     * The resultant collection will be filtered based on the supplied criteria to ensure no
     * MetadataRecord instances are returned that do not match the criteria
     * @param results The array of IEntryResult instances
     * @param criteria Map of fieldNames to MetadataSearchCriteria objects to use
     * @return Collection of metadata records
     * @throws QueryMetadataException
     */
    protected Collection getMetadataRecords(final IEntryResult[] results, 
                                            final Map criteria, 
                                            final boolean hasCaseFunctions) throws MetaMatrixComponentException {
        if (results != null && results.length > 0) {
            Collection records = RecordFactory.getMetadataRecord(results, null);
            
            // Filter the records according to the specified criteria
            Collection criteriaCollection = IndexCriteriaBuilder.getLiteralCriteria(criteria);
            for (Iterator i = criteriaCollection.iterator(); i.hasNext();) {
                final Map literalCriteria = (Map)i.next();
                
                // Map a copy of the criteria map converting all keys in the map to be upper-cased
                final Map updatedCriteria = new HashMap(literalCriteria.size());        
                for (Iterator j = criteria.entrySet().iterator(); j.hasNext();) {
                    final Map.Entry entry = (Map.Entry)j.next();
                    final String key = (String)entry.getKey();
                    updatedCriteria.put(key.toUpperCase(),entry.getValue());
                }

                // Filter based on name criteria ...
                String nameCriteria = IndexCriteriaBuilder.getValueInCriteria(updatedCriteria, MetadataRecord.MetadataFieldNames.NAME_FIELD);
                if (!StringUtil.isEmpty(nameCriteria)) {
                    for (Iterator j = records.iterator(); j.hasNext();) {
                        MetadataRecord record = (MetadataRecord)j.next();
                        if (!StringUtil.isEmpty(record.getName())) {
                            String recordName = record.getName();
                            if (hasCaseFunctions) {
                                nameCriteria = nameCriteria.toUpperCase();
                                recordName   = recordName.toUpperCase();
                            }
                            //System.out.println("Matching "+nameCriteria+" to "+record.getName());
                            if (!CharOperation.match(nameCriteria.toCharArray(), recordName.toCharArray(), true)) {
                                //System.out.println("--> Removing record");
                                j.remove();
                            }
                        }
                    }
                }
            }
            
            return records;
        }
        return Collections.EMPTY_LIST;
    }
    
    /**
     * Check if any of the criteria in this map evaluates to a false, if it does then no need to proceed,
     * can return no records.
     * @param criteria Map of fieldNames to MetadataSearchCriteria objects used to search 
     * @return true if there is a false criteria else false
     * @since 4.3
     */
    private boolean hasFalseCriteria(final Map criteria) {
        boolean falseCriteria = false;
        for(final Iterator iter = criteria.values().iterator(); iter.hasNext();) {
            Object criteriaObj = iter.next();
            if(criteriaObj instanceof MetadataLiteralCriteria) {
                falseCriteria = ((MetadataLiteralCriteria) criteriaObj).isFalseCriteria();
                if(falseCriteria) {
                    break;
                }
            }
        }
        return falseCriteria;
    }
    
    /**
     * Check if any of the criteria in this map has case functions on the fields involved.
     * @param criteria Map of fieldNames to MetadataSearchCriteria objects used to search 
     * @return true if there is a case criteria else false
     * @since 4.3
     */
    private boolean hasCaseFunctions(final Map criteria) {
        boolean caserFunctions = false;
        for(final Iterator iter = criteria.values().iterator(); iter.hasNext();) {
            Object criteriaObj = iter.next();
            if(criteriaObj instanceof MetadataLiteralCriteria) {
                caserFunctions = ((MetadataLiteralCriteria) criteriaObj).hasFieldWithCaseFunctions();
                if(caserFunctions) {
                    break;
                }
            } else if(criteriaObj instanceof MetadataInCriteria) {
                caserFunctions = ((MetadataInCriteria) criteriaObj).hasFieldWithCaseFunctions();
                if(caserFunctions) {
                    break;
                }
            }
        }
        return caserFunctions;
    }    

    /**
     * Get all the file records for the available files in vdbs given the match criteria 
     * @param criteria Map of fieldNames to MetadataSearchCriteria objects used to search
     * @param returnFirstMatch Boolean indicating if first match is to be returned
     * @return Collection of file records
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    protected Collection getFileRecords(final Map criteria, boolean returnFirstMatch) {

        // get the criteria for path in vdb
        MetadataLiteralCriteria literalCriteria = (MetadataLiteralCriteria) criteria.get(FileRecord.MetadataMethodNames.PATH_IN_VDB_FIELD.toUpperCase());
        // if the criteria exists get the path invdb
        String pathInVDb = literalCriteria != null ? (String) literalCriteria.getEvaluatedValue() : null;

        Collection fileRecords = new ArrayList();
        // get the file paths from the selector
        String[] filePaths = super.getIndexSelector().getFilePaths();
        for(int i=0; i< filePaths.length; i++) {
            String filePath = filePaths[i];
            FileRecordImpl record = new FileRecordImpl();
            record.setIndexSelector(super.getIndexSelector());
            record.setPathInVdb(filePath);
            // check if pattern specified matches the filePath
            if(!fileRecords.contains(record)) {
                // if its a non-null criteria path get the records that match the path
                if(pathInVDb != null) {
                    if(CharOperation.match(pathInVDb.toCharArray(), filePath.toCharArray(), false)) {
                        fileRecords.add(record);
                        if(returnFirstMatch) {
                            return fileRecords;
                        }
                    }
                } else {
                    fileRecords.add(record);
                }
            }
        }
        return fileRecords;            
    }
    
}