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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.teiid.connector.metadata.runtime.MetadataConstants;
import org.teiid.core.index.IEntryResult;
import org.teiid.internal.core.index.BlocksIndexInput;
import org.teiid.internal.core.index.Index;

import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.StringUtil;

/**
 * IndexUtil
 */
public class SimpleIndexUtil {
	
	public static interface ProgressMonitor {
		
		public void beginTask(String name, int totalWork);
		
		public void worked(int work);
	}
    
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################    

    public static final boolean CASE_SENSITIVE_INDEX_FILE_NAMES = false;

    //############################################################################################################################
    //# Indexing Methods                                                                                                       #
    //############################################################################################################################

    public static String getIndexFilePath(final String indexDirectoryPath, final String indexFileName) {
        StringBuffer sb = new StringBuffer(100);
        sb.append(indexDirectoryPath);
        if (!indexDirectoryPath.endsWith(File.separator)) {
            sb.append(File.separator);
        }
        sb.append(indexFileName);
        return sb.toString();
    }

    //############################################################################################################################
    //# Methods to query indexes                                                                                                 #
    //############################################################################################################################

    /**
     * Return all index file records that match the specified record pattern.
     * The pattern can be constructed from any combination of characters
     * including the multiple character wildcard '*' and single character
     * wildcard '?'.  The field delimiter is used to tokenize both the pattern
     * and the index record so that individual fields can be matched.  The method
     * assumes that the first occurrence of the delimiter in the record alligns 
     * with the first occurrence in the pattern. Any wildcard characters in the 
     * pattern cannot represent a delimiter character.
     * @param indexes the array of MtkIndex instances to query
     * @param pattern
     * @param fieldDelimiter
     * @return results
     * @throws QueryMetadataException
     */
    public static IEntryResult[] queryIndex(final Index[] indexes, final char[] pattern, final char fieldDelimiter) throws MetaMatrixCoreException {
        final boolean isCaseSensitive  = false;
        final List<IEntryResult> queryResult = new ArrayList<IEntryResult>();

        try {
            for (int i = 0; i < indexes.length; i++) {
                // Search for index records matching the specified pattern  
                IEntryResult[] partialResults = indexes[i].queryEntriesMatching(pattern,isCaseSensitive);
                
                // If any of these IEntryResults represent an index record that is continued
                // across multiple entries within the index file then we must query for those
                // records and build the complete IEntryResult
                if (partialResults != null) {
                    partialResults = addContinuationRecords(indexes[i], partialResults);
                }
                
                if (partialResults != null) {
                    queryResult.addAll(Arrays.asList(partialResults));
                }
            }
        } catch(IOException e) {
            throw new MetaMatrixCoreException(e); 
        }
        
        // Remove any results that do not match after tokenizing the record
        for (int i = 0, n = queryResult.size(); i < n; i++) {
            IEntryResult record = queryResult.get(i);
            if ( record == null || !entryMatches(record.getWord(),pattern,fieldDelimiter) ) {
            	queryResult.remove(record);
            }
        }
                
        return queryResult.toArray(new IEntryResult[queryResult.size()]);
    }
    
    /**
     * Return true if the record matches the pattern after being tokenized using
     * the specified delimiter.  The method assumes that the first occurrence of 
     * the delimiter in the record alligns with the first occurrence in the pattern.
     * Any wildcard characters in the pattern cannot represent a delimiter character.
     * @param record
     * @param pattern
     * @param fieldDelimiter
     * @return
     */
    private static boolean entryMatches(final char[] record, final char[] pattern, final char fieldDelimiter) {
        final boolean isCaseSensitive  = false;
        if (record == null)
            return false; // null record cannot match
        if (pattern == null)
            return true; // null pattern is equivalent to '*'
            
        String delimiter = String.valueOf(fieldDelimiter);
        List recordTokens  = StringUtil.split(new String(record),delimiter);
        List patternTokens = StringUtil.split(new String(pattern),delimiter);
        if (patternTokens.size() > recordTokens.size()) {
            return false;
        }
        
        for (int i = 0, n = patternTokens.size(); i < n; i++) {
            char[] patternToken = ((String)patternTokens.get(i)).toCharArray();
            char[] recordToken  = ((String)recordTokens.get(i)).toCharArray();
            if (!CharOperation.match(patternToken,recordToken,isCaseSensitive)) {
                return false;
            }
        }        
        return true;
    }

    /**
     * Return all index file records that match the specified record prefix
     * or pattern. The pattern can be constructed from any combination of characters
     * including the multiple character wildcard '*' and single character
     * wildcard '?'.  The prefix may be constructed from any combination of 
     * characters excluding the wildcard characters.  The prefix specifies a fixed
     * number of characters that the index record must start with.
     * @param indexes the array of MtkIndex instances to query
     * @param pattern
     * @return results
     * @throws MetamatrixCoreException
     */
    public static IEntryResult[] queryIndex(final Index[] indexes, final char[] pattern, final boolean isPrefix, final boolean returnFirstMatch) throws MetaMatrixCoreException {
        return queryIndex(null, indexes, pattern, isPrefix, true, returnFirstMatch);
    }
    
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
    public static IEntryResult[] queryIndex(ProgressMonitor monitor, final Index[] indexes, final char[] pattern, final boolean isPrefix, final boolean returnFirstMatch) throws MetaMatrixCoreException {
        return queryIndex(monitor, indexes, pattern, isPrefix, true, returnFirstMatch);        
    }
    
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
    public static IEntryResult[] queryIndex(ProgressMonitor monitor, final Index[] indexes, final char[] pattern, final boolean isPrefix, final boolean isCaseSensitive, final boolean returnFirstMatch) throws MetaMatrixCoreException {
        final List<IEntryResult> queryResult = new ArrayList<IEntryResult>();
        if ( monitor != null ) {
            monitor.beginTask( null, indexes.length );        
        }
        
        try {
            for (int i = 0; i < indexes.length; i++) {
                
                if ( monitor != null ) {
                    monitor.worked( 1 );
                }
                
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
            throw new MetaMatrixCoreException(e);
        }

        return queryResult.toArray(new IEntryResult[queryResult.size()]);
    }
    
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
    public static IEntryResult[] queryIndex(ProgressMonitor monitor, final Index[] indexes, final Collection patterns, final boolean isPrefix, final boolean isCaseSensitive, final boolean returnFirstMatch) throws MetaMatrixCoreException {
        final List<IEntryResult> queryResult = new ArrayList<IEntryResult>();
        if ( monitor != null ) {
            monitor.beginTask( null, indexes.length );        
        }
        
        // index file input
        BlocksIndexInput input = null;
        
        try {
            for (int i = 0; i < indexes.length; i++) {
                
                if ( monitor != null ) {
                    monitor.worked( 1 );
                }
                // initialize input for the index file
                input = new BlocksIndexInput(indexes[i].getIndexFile());

                IEntryResult[] partialResults = null;
                for(final Iterator patternIter = patterns.iterator(); patternIter.hasNext();) {
                    char[] pattern = ((String) patternIter.next()).toCharArray();
                    if(isPrefix) {
                        // Query based on prefix. This uses a fast binary search
                        // based on matching the first n characters in the index record.  
                        // The index files contain records that are sorted alphabetically
                        // by fullname such that the search algorithm can quickly determine
                        // which index block(s) contain the matching prefixes.
                        partialResults = input.queryEntriesPrefixedBy(pattern, isCaseSensitive);
                    } else {
                        // Search for index records matching the specified pattern
                        partialResults = input.queryEntriesMatching(pattern, isCaseSensitive);
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
                            IEntryResult record = partialResults[j];
                            if(record != null) {
                                char[] recordWord = partialResults[j].getWord();
                                // filter out any continuation records, they should already appended
                                // to index record thet is continued
                                if(recordWord[0] != MetadataConstants.RECORD_TYPE.RECORD_CONTINUATION) {                            
                                    if (!isPrefix) {
                                        // filter results that do not match after tokenizing the record
                                        if(entryMatches(recordWord,pattern,IndexConstants.RECORD_STRING.RECORD_DELIMITER) ) {
                                            queryResult.add(partialResults[j]);
                                        }
                                    } else {
                                        queryResult.add(partialResults[j]);
                                    }
                                }
                            }
                        }
                    }
                    if (returnFirstMatch && queryResult.size() > 0) {
                        break;
                    }
                    
                    // close file input
                    input.close();
                }
            }
        } catch(IOException e) {
            throw new MetaMatrixCoreException(e);
        } finally {
            // close file input
            try {
                if(input != null) {
                    input.close();
                }
            } catch(IOException io) {}
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
     * Return true if the specifed index file exists on the file system
     * otherwise return false.
     */
    public static boolean indexFileExists(final String indexFilePath) {
        if (indexFilePath == null) {
            return false;
        }
        String filePath = indexFilePath.replace(FileUtils.SEPARATOR, File.separatorChar);
        final File indexFile = new File(filePath);
        return indexFileExists(indexFile);
    }  

	/**
	 * Return true if the specifed index file exists on the file system
	 * otherwise return false.
	 */
	public static boolean indexFileExists(final File indexFile) {
		if ( !indexFile.isDirectory() && indexFile.exists() ) {
			return isIndexFile(indexFile.getName());
		}
		return false;
	}
	
    /**
     * Return true if the specifed index file represents a known index file
     * on the file system otherwise return false.
     */
    public static boolean isModelIndex(final String indexFileName) {
        if (!isIndexFile(indexFileName)) {
            return false;
        }
        return !IndexConstants.INDEX_NAME.isKnownIndex(indexFileName);
    }

    /**
     * Return true if the specifed index file represents a index file
     * on the file system otherwise return false.
     */
    public static boolean isIndexFile(final String indexFileName) {
		if (!StringUtil.isEmpty(indexFileName)) {
		    String extension = FileUtils.getExtension(indexFileName);
			if(extension != null ) {
				if( extension.equals(IndexConstants.INDEX_EXT) || extension.equals(IndexConstants.SEARCH_INDEX_EXT)) {
					return true;
				}
			}
		}        
		return false; 
    }

	/**
	 * Return true if the specifed index file represents a index file
	 * on the file system otherwise return false.
	 */
	public static boolean isIndexFile(final File indexFile) {
		if (indexFile != null && indexFile.isFile()) {
			return isIndexFile(indexFile.getName());
		}        
		return false; 
	}

	/**
	 * Return an array of indexes given a indexName. 
	 * @param indexName The shortName of the index file
	 * @param selector The indexSelector to lookup indexes
	 * @return An array of indexes, may be duplicates depending on index selector.
	 * @throws MetamatrixCoreException If there is an error looking up indexes
	 * @since 4.2
	 */
    public static Index[] getIndexes(final String indexName, final IndexMetadataStore selector) {
		ArgCheck.isNotEmpty(indexName);
        // The the index file name for the record type
        final Index[] indexes = selector.getIndexes();
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
    
    /**
     * Return the name of the index file to use for the specified record type, applies only for sever and vdb
     * index files.
     * @param recordType
     * @return
     */
    public static String getRecordTypeForIndexFileName(final String indexName) {
        char recordType;
        if(indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.COLUMNS_INDEX)) {
            recordType = MetadataConstants.RECORD_TYPE.COLUMN;
        } else if(indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.TABLES_INDEX)) {
            recordType = MetadataConstants.RECORD_TYPE.TABLE;
        } else if(indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.MODELS_INDEX)) {
            recordType = MetadataConstants.RECORD_TYPE.MODEL;
        } else if(indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.DATATYPES_INDEX)) {
            recordType = MetadataConstants.RECORD_TYPE.DATATYPE;
        } else if(indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.VDBS_INDEX)) {
            recordType = MetadataConstants.RECORD_TYPE.VDB_ARCHIVE;
        } else if(indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.ANNOTATION_INDEX)) {
            recordType = MetadataConstants.RECORD_TYPE.ANNOTATION;
        } else if(indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.PROPERTIES_INDEX)) {
            recordType = MetadataConstants.RECORD_TYPE.PROPERTY;
        } else if(indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.SELECT_TRANSFORM_INDEX)) {
            recordType = MetadataConstants.RECORD_TYPE.SELECT_TRANSFORM;
        } else if(indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.INSERT_TRANSFORM_INDEX)) {
            recordType = MetadataConstants.RECORD_TYPE.INSERT_TRANSFORM;
        } else if(indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.UPDATE_TRANSFORM_INDEX)) {
            recordType = MetadataConstants.RECORD_TYPE.UPDATE_TRANSFORM;
        } else if(indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.DELETE_TRANSFORM_INDEX)) {
            recordType = MetadataConstants.RECORD_TYPE.DELETE_TRANSFORM;
        } else if(indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.PROC_TRANSFORM_INDEX)) {
            recordType = MetadataConstants.RECORD_TYPE.PROC_TRANSFORM;
        } else if(indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.MAPPING_TRANSFORM_INDEX)) {
            recordType = MetadataConstants.RECORD_TYPE.MAPPING_TRANSFORM;
        } else if(indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.FILES_INDEX)) {
            recordType = MetadataConstants.RECORD_TYPE.FILE;
        } else {
            return null;
        }
        return StringUtil.Constants.EMPTY_STRING + recordType;
    }    
    

	/**
	 * Return the prefix match string that could be used to exactly match a fully 
	 * qualified entity name in an index record. All index records 
	 * contain a header portion of the form:  
	 * recordType|name|
	 * @param name The fully qualified name for which the prefix match 
	 * string is to be constructed.
	 * @return The pattern match string of the form: recordType|name|
	 */
	public static String getPrefixPattern(final char recordType, final String uuid) {

		// construct the pattern string
		String patternStr = "" //$NON-NLS-1$
						  + recordType
						  + IndexConstants.RECORD_STRING.RECORD_DELIMITER;
		if(uuid != null && !uuid.equals(StringUtil.Constants.EMPTY_STRING)) {                          
			patternStr = patternStr + uuid.trim() + IndexConstants.RECORD_STRING.RECORD_DELIMITER;
		}                    

		return patternStr;
	}
    
}
