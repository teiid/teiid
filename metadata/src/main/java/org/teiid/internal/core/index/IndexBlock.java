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

import java.util.Arrays;



/**
 * An indexBlock stores wordEntries.
 */

public abstract class IndexBlock extends Block {

    public IndexBlock(int blockSize) {
        super(blockSize);
    }
    public WordEntry findEntryPrefixedBy(char[] word, boolean isCaseSensitive) {
        reset();
        WordEntry entry= new WordEntry();
        while (nextEntry(entry)) {
            if (CharOperation.prefixEquals(entry.getWord(), word, isCaseSensitive)) {
                return entry;
            }
        }
        return null;
    }
    public WordEntry findExactEntry(char[] word) {
        reset();
        WordEntry entry= new WordEntry();
        while (nextEntry(entry)) {
            if (Arrays.equals(entry.getWord(), word)) {
                return entry;
            }
        }
        return null;
    }
    /**
     * Returns whether the block is empty or not (if it doesn't contain any wordEntry).
     */
    public abstract boolean isEmpty();

    /**
     * Finds the next wordEntry and stores it in the given entry.
     */
    public abstract boolean nextEntry(WordEntry entry);

    public void reset() {
    }
}
