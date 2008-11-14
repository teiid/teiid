/*******************************************************************************
 * Copyright (c) 2000, 2003 IBM Corporation and others.
 * All rights reserved. This program and the accompanying materials 
 * are made available under the terms of the Common Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/cpl-v10.html
 * 
 * Contributors:
 *     IBM Corporation - initial API and implementation
 *     MetaMatrix, Inc - repackaging and updates for use as a metadata store
 *******************************************************************************/
package com.metamatrix.core.index;

import java.io.File;
import java.io.IOException;

/**
 * An IIndex is the interface used to generate an index file, and to make queries on
 * this index.
 */

public interface IIndex {
	/**
	 * Adds the given document to the index.
	 */
	void add(IDocument document, IIndexer indexer) throws IOException;
	/**
	 * Empties the index.
	 */
	void empty() throws IOException;
	/**
	 * Returns the index file on the disk.
	 */
	File getIndexFile();
	/**
	 * Returns the number of documents indexed.
	 */
	int getNumDocuments() throws IOException;
	/**
	 * Returns the number of unique words indexed.
	 */
	int getNumWords() throws IOException;
	/**
	 * Returns the path corresponding to a given document number
	 */
	String getPath(int documentNumber) throws IOException;
	/**
	 * Ansers true if has some changes to save.
	 */
	boolean hasChanged();
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
	 * Removes the corresponding document from the index.
	 */
	void remove(String documentName) throws IOException;
	/**
	 * Saves the index on the disk.
	 */
	void save() throws IOException;
    
    /**
     * Closes the index file if open 
     * 
     * @since 5.0
     */
    void close();
    
    /**
     * closes the index file then deletes it. 
     * 
     * @since 5.0
     */
    void dispose();
    
    /**
     * sets a boolean indicating the index file will be cached and should remain open and in-memory 
     * @param doCache
     * @since 5.0
     */
    void setDoCache(boolean doCache);
}
