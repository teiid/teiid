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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.teiid.core.TeiidException;
import org.teiid.core.index.IEntryResult;
import org.teiid.core.util.ArgCheck;
import org.teiid.internal.core.index.Index;


/**
 * IndexUtil
 */
public class SimpleIndexUtil {
	
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################    

    public static final boolean CASE_SENSITIVE_INDEX_FILE_NAMES = false;

    //############################################################################################################################
    //# Indexing Methods                                                                                                       #
    //############################################################################################################################

    /**
     * Return all index file records that match the specified record prefix
     * or pattern. The pattern can be constructed from any combination of characters
     * including the multiple character wildcard '*' and single character
     * wildcard '?'.  The prefix may be constructed from any combination of 
     * characters excluding the wildcard characters.  The prefix specifies a fixed
     * number of characters that the index record must start with.
     * @param monitor an optional ProgressMonitor
     * @param indexes the array of MtkIndex instances to query
     * @param pattern
     * @return results
     * @throws MetamatrixCoreException
     */
    public static IEntryResult[] queryIndex(final Index[] indexes, final char[] pattern, final boolean isPrefix, final boolean isCaseSensitive, final boolean returnFirstMatch) throws TeiidException {
        final List<IEntryResult> queryResult = new ArrayList<IEntryResult>();
        
        try {
            for (int i = 0; i < indexes.length; i++) {
                
                IEntryResult[] partialResults = null;
                if(isPrefix) {
                    // Query based on prefix. This uses a fast binary search
                    // based on matching the first n characters in the index record.  
                    // The index files contain records that are sorted alphabetically
                    // by fullname such that the search algorithm can quickly determine
                    // which index block(s) contain the matching prefixes.
                    partialResults = indexes[i].queryEntries(pattern, isCaseSensitive);
                } else {
                    // Search for index records matching the specified pattern
                    partialResults = indexes[i].queryEntriesMatching(pattern, isCaseSensitive);
                }

                // If any of these IEntryResults represent an index record that is continued
                // across multiple entries within the index file then we must query for those
                // records and build the complete IEntryResult
                if (partialResults != null) {
                    partialResults = addContinuationRecords(indexes[i], partialResults);
                }

                // Process these results against the specified pattern and return
                // only the subset entries that match both criteria  
                if (partialResults != null) {
                    for (int j = 0; j < partialResults.length; j++) {
                    	// filter out any continuation records, they should already appended
                    	// to index record thet is continued
						IEntryResult result = partialResults[j];
						if(result != null && result.getWord()[0] != MetadataConstants.RECORD_TYPE.RECORD_CONTINUATION) {
	                        queryResult.add(partialResults[j]);
						}
                    }
                }

                if (returnFirstMatch && queryResult.size() > 0) {
                    break; 
                }                
            }
        } catch(IOException e) {
            throw new TeiidException(e);
        }

        return queryResult.toArray(new IEntryResult[queryResult.size()]);
    }
    
    private static IEntryResult[] addContinuationRecords(final Index index, final IEntryResult[] partialResults) throws IOException {
                                                      
        final int blockSize = RecordFactory.INDEX_RECORD_BLOCK_SIZE;
        
        IEntryResult[] results = partialResults;
        for (int i = 0; i < results.length; i++) {
            IEntryResult partialResult = results[i];
            char[] word = partialResult.getWord();

            // If this IEntryResult is not continued on another record then skip to the next result
            if (word.length < blockSize || word[blockSize-1] != MetadataConstants.RECORD_TYPE.RECORD_CONTINUATION) {
                continue;
            }
            // Extract the UUID from the IEntryResult to use when creating the prefix string
            String objectID = RecordFactory.extractUUIDString(partialResult);
            String patternStr = "" //$NON-NLS-1$
                              + MetadataConstants.RECORD_TYPE.RECORD_CONTINUATION
                              + word[0]
                              + IndexConstants.RECORD_STRING.RECORD_DELIMITER
                              + objectID
                              + IndexConstants.RECORD_STRING.RECORD_DELIMITER;                    
            
            // Query the index file for any continuation records
            IEntryResult[] continuationResults =  index.queryEntries(patternStr.toCharArray(), true);
            // If found the continued records then join to the original result and stop searching
            if (continuationResults != null && continuationResults.length > 0) {
                results[i] = RecordFactory.joinEntryResults(partialResult, continuationResults, blockSize);
            }
        }
        return results;
    }
    
    //############################################################################################################################
    //# Helper methods                                                                                                           #
    //############################################################################################################################

    /**
	 * Return an array of indexes given a indexName. 
	 * @param indexName The shortName of the index file
	 * @param selector The indexSelector to lookup indexes
	 * @return An array of indexes, may be duplicates depending on index selector.
	 * @throws MetamatrixCoreException If there is an error looking up indexes
	 * @since 4.2
	 */
    public static Index[] getIndexes(final String indexName, Index[] indexes) {
		ArgCheck.isNotEmpty(indexName);
        // The the index file name for the record type
        final List<Index> tmp = new ArrayList<Index>(indexes.length);
        for (int i = 0; i < indexes.length; i++) {
            Index coreIndex = indexes[i];
            if(coreIndex != null) {
                final String indexFileName = indexes[i].getIndexFile().getName();
                if(indexName.equals(indexFileName)) {
                    tmp.add(coreIndex);
                }
            }
        }
        return tmp.toArray(new Index[tmp.size()]);
    }

    /**
     * Return the name of the index file to use for the specified record type, applies only for sever and vdb
	 * index files.
     * @param recordType
     * @return
     */
    public static String getIndexFileNameForRecordType(final char recordType) {
        switch (recordType) {
  	      case MetadataConstants.RECORD_TYPE.COLUMN: return IndexConstants.INDEX_NAME.COLUMNS_INDEX;
		  case MetadataConstants.RECORD_TYPE.TABLE: return IndexConstants.INDEX_NAME.TABLES_INDEX;
          case MetadataConstants.RECORD_TYPE.MODEL: return IndexConstants.INDEX_NAME.MODELS_INDEX;
          case MetadataConstants.RECORD_TYPE.CALLABLE:
          case MetadataConstants.RECORD_TYPE.CALLABLE_PARAMETER:
		  case MetadataConstants.RECORD_TYPE.RESULT_SET: return IndexConstants.INDEX_NAME.PROCEDURES_INDEX;
          case MetadataConstants.RECORD_TYPE.INDEX:
          case MetadataConstants.RECORD_TYPE.ACCESS_PATTERN:           
          case MetadataConstants.RECORD_TYPE.PRIMARY_KEY:
          case MetadataConstants.RECORD_TYPE.FOREIGN_KEY:
		  case MetadataConstants.RECORD_TYPE.UNIQUE_KEY:  return IndexConstants.INDEX_NAME.KEYS_INDEX;
          case MetadataConstants.RECORD_TYPE.SELECT_TRANSFORM: return IndexConstants.INDEX_NAME.SELECT_TRANSFORM_INDEX;
          case MetadataConstants.RECORD_TYPE.INSERT_TRANSFORM: return IndexConstants.INDEX_NAME.INSERT_TRANSFORM_INDEX;
          case MetadataConstants.RECORD_TYPE.UPDATE_TRANSFORM: return IndexConstants.INDEX_NAME.UPDATE_TRANSFORM_INDEX;
          case MetadataConstants.RECORD_TYPE.DELETE_TRANSFORM: return IndexConstants.INDEX_NAME.DELETE_TRANSFORM_INDEX;
          case MetadataConstants.RECORD_TYPE.PROC_TRANSFORM: return IndexConstants.INDEX_NAME.PROC_TRANSFORM_INDEX;
          case MetadataConstants.RECORD_TYPE.MAPPING_TRANSFORM: return IndexConstants.INDEX_NAME.MAPPING_TRANSFORM_INDEX;
          case MetadataConstants.RECORD_TYPE.DATATYPE: return IndexConstants.INDEX_NAME.DATATYPES_INDEX;
          //case IndexConstants.RECORD_TYPE.DATATYPE_ELEMENT:
          //case IndexConstants.RECORD_TYPE.DATATYPE_FACET:
          case MetadataConstants.RECORD_TYPE.VDB_ARCHIVE: return IndexConstants.INDEX_NAME.VDBS_INDEX;
          case MetadataConstants.RECORD_TYPE.ANNOTATION: return IndexConstants.INDEX_NAME.ANNOTATION_INDEX;
          case MetadataConstants.RECORD_TYPE.PROPERTY: return IndexConstants.INDEX_NAME.PROPERTIES_INDEX;
		  //case IndexConstants.RECORD_TYPE.JOIN_DESCRIPTOR: return null;
		  case MetadataConstants.RECORD_TYPE.FILE: return IndexConstants.INDEX_NAME.FILES_INDEX;
        }
        throw new IllegalArgumentException("Unkown record type " + recordType);
    }    
    
}
