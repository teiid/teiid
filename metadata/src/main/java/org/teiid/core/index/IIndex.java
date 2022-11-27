/*******************************************************************************
 * Copyright (c) 2000, 2003 IBM Corporation and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *     IBM Corporation - initial API and implementation
 *     MetaMatrix, Inc - repackaging and updates for use as a metadata store
 *******************************************************************************/
package org.teiid.core.index;

import java.io.IOException;

/**
 * An IIndex is the interface used to generate an index file, and to make queries on
 * this index.
 */

public interface IIndex {
    /**
     * Returns the number of documents indexed.
     */
    int getNumDocuments() throws IOException;
    /**
     * Returns the number of unique words indexed.
     */
    int getNumWords() throws IOException;
    /**
     * Returns the paths of the documents containing the given word.
     */
    IQueryResult[] query(String word) throws IOException;
    /**
     * Returns all entries for a given word.
     */
    IEntryResult[] queryEntries(char[] pattern) throws IOException;
    /**
     * Returns the paths of the documents whose names contain the given word.
     */
    IQueryResult[] queryInDocumentNames(String word) throws IOException;
    /**
     * Returns the paths of the documents containing the given word prefix.
     */
    IQueryResult[] queryPrefix(char[] prefix) throws IOException;

    /**
     * Closes the index file if open
     *
     * @since 5.0
     */
    void close();

    /**
     * sets a boolean indicating the index file will be cached and should remain open and in-memory
     * @param doCache
     * @since 5.0
     */
    void setDoCache(boolean doCache);
}
