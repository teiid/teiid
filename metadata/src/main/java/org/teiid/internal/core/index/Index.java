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
package org.teiid.internal.core.index;

import java.io.IOException;

import org.teiid.core.index.IEntryResult;
import org.teiid.core.index.IIndex;
import org.teiid.core.index.IQueryResult;
import org.teiid.metadata.VDBResource;


/**
 * An Index is used to create an index on the disk, and to make queries. It uses a set of
 * indexers and a mergeFactory. The index fills an inMemoryIndex up
 * to it reaches a certain size, and then merges it with a main index on the disk.
 * <br> <br>
 * The changes are only taken into account by the queries after a merge.
 */

public class Index implements IIndex {
    /**
     * Maximum size of the index in memory.
     */
    public static final int MAX_FOOTPRINT= 10000000;

    private VDBResource indexFile;

    /*
     * Caching the index input object so we can keep it open for multiple pass querying rather than
     * opening/closing and wasting CPU for file IO
     */
    private BlocksIndexInput cachedInput;
    protected boolean doCache = false;
    private String resourceFileName;

    /**
     * String representation of this index.
     */
    public String toString;

    public Index(VDBResource f) throws IOException {
        indexFile = f;
        initialize();
    }

    /**
     * @see IIndex#getNumDocuments
     */
    public int getNumDocuments() throws IOException {
        BlocksIndexInput input = getBlocksIndexInput();
        try {
            input.open();
            return input.getNumFiles();
        } finally {
            if( !doCache ) {
                input.close();
            }
        }
    }

    /**
     * @see IIndex#getNumWords
     */
    public int getNumWords() throws IOException {
        BlocksIndexInput input = getBlocksIndexInput();
        try {
            input.open();
            return input.getNumWords();
        } finally {
            if( !doCache ) {
                input.close();
            }
        }
    }

    /**
     * Initialises the indexGenerator.
     */
    public void initialize() throws IOException {

        // check whether existing index file can be read
        BlocksIndexInput mainIndexInput= getBlocksIndexInput();
        try {
            mainIndexInput.open();
        } catch(IOException e) {
            BlocksIndexInput input = mainIndexInput;
            try {
                input.setOpen(true);
                input.close();
            } finally {
                input.setOpen(false);
            }
            //System.out.println(" Index.initialize(): Deleting Index file = " + indexFile.getName());
            mainIndexInput = null;
            throw e;
        }
        if( !doCache ) {
            mainIndexInput.close();
        }
    }

    /**
     * @see IIndex#query
     */
    public IQueryResult[] query(String word) throws IOException {
        BlocksIndexInput input= getBlocksIndexInput();
        try {
            return input.query(word);
        } finally {
            if( !doCache ) {
                input.close();
            }
        }
    }

    public IEntryResult[] queryEntries(char[] prefix) throws IOException {
        BlocksIndexInput input= getBlocksIndexInput();
        try {
            return input.queryEntriesPrefixedBy(prefix);
        } finally {
            if( !doCache ) {
                input.close();
            }
        }
    }

    /**
     * @see IIndex#queryInDocumentNames
     */
    public IQueryResult[] queryInDocumentNames(String word) throws IOException {
        BlocksIndexInput input= getBlocksIndexInput();
        try {
            return input.queryInDocumentNames(word);
        } finally {
            if( !doCache ) {
                input.close();
            }
        }
    }

    /**
     * @see IIndex#queryPrefix
     */
    public IQueryResult[] queryPrefix(char[] prefix) throws IOException {
        BlocksIndexInput input= getBlocksIndexInput();
        try {
            return input.queryFilesReferringToPrefix(prefix);
        } finally {
            if( !doCache ) {
                input.close();
            }
        }
    }

    /**
     * Overloaded the method in Index to allow a user to specify if the
     * query should be case sensitive.
     */
    public IEntryResult[] queryEntriesMatching(char[] prefix, boolean isCaseSensitive) throws IOException {
        BlocksIndexInput input= getBlocksIndexInput();
        try {
            return input.queryEntriesMatching(prefix,isCaseSensitive);
        } finally {
            if( !doCache ) {
                input.close();
            }
        }
    }

    /**
     * Overloaded the method in Index to allow a user to specify if the
     * query should be case sensitive.
     */
    public IEntryResult[] queryEntries(char[] prefix, boolean isCaseSensitive) throws IOException {
        BlocksIndexInput input = getBlocksIndexInput();
        try {
            return input.queryEntriesPrefixedBy(prefix,isCaseSensitive);
        } finally {
            if( !doCache ) {
                input.close();
            }
        }
    }

    protected BlocksIndexInput getBlocksIndexInput() {
        if( doCache ) {
            if( getCachedInput() == null  ) {
                boolean wasLoaded = false;
                try {
                    if( getCachedInput() == null ) {
                        setCachedInput( new BlocksIndexInput(indexFile) );
                        getCachedInput().open();
                        wasLoaded = true;
                    }
                } catch ( IOException theException ) {

                } finally {
                    if( wasLoaded && getCachedInput() != null ) {
                        return getCachedInput();
                    }
                    setCachedInput(null);
                }
            } else {
                return getCachedInput();
            }
        }

        return new BlocksIndexInput(indexFile);
    }

    public void close() {
        if( getCachedInput() != null ) {
            try {
                getCachedInput().close();
            } catch (IOException theException) {
            } finally {
                setCachedInput(null);
            }
        }
    }

    public String toString() {
        String str = this.toString;
        if (str == null) str = super.toString();
        str += "(length: "+ indexFile.getSize() +")"; //$NON-NLS-1$ //$NON-NLS-2$
        return str;
    }

    public void setDoCache(boolean theDoCache) {
        this.doCache = theDoCache;
    }

    public BlocksIndexInput getCachedInput() {
        return this.cachedInput;
    }

    public void setCachedInput(BlocksIndexInput theCachedInput) {
        this.cachedInput = theCachedInput;
    }

    public String getResourceFileName() {
        return this.resourceFileName;
    }
}
