/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package org.teiid.metadata.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.teiid.core.TeiidException;
import org.teiid.core.index.IEntryResult;
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
     * @param indexes the array of MtkIndex instances to query
     * @param pattern
     * @return results
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
             throw new TeiidException(RuntimeMetadataPlugin.Event.TEIID80003, e);
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

}
