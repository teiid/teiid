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
package org.teiid.internal.core.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.core.index.IDocument;
import org.teiid.core.index.IIndex;


/**
 * This index stores the document names in an <code>ArrayList</code>, and the words in
 * an <code>HashtableOfObjects</code>.
 */

public class InMemoryIndex {

	/**
	 * hashtable of WordEntrys = words+numbers of the files they appear in.
	 */
	protected List<WordEntry> words;

	/**
	 * List of IndexedFiles = file name + a unique number.
	 */
	protected IndexedFileHashedArray files;

	/**
	 * Size of the index.
	 */
	protected long footprint;

	private WordEntry[] sortedWordEntries;
	private IndexedFile[] sortedFiles;
	public InMemoryIndex() {
		init();
	}

	public IndexedFile addDocument(IDocument document) {
		IndexedFile indexedFile= this.files.add(document);
		this.footprint += indexedFile.footprint() + 4;
		this.sortedFiles = null;
		return indexedFile;
	}
	
    /**
     * Looks if the word already exists in the index and add the fileNum to this word.
     * If the word does not exist, it adds it in the index.
     */
    protected void addRef(char[] word, int fileNum) {
		// Assuming that our indexes are already unique we shouldn't
		// worry about whether or not one exists already in the list
		// So let's change this a little and improve performance.
	//  WordEntry entry= (WordEntry) this.words.get(word);
	//  if (entry == null) {
	//	  entry= new WordEntry(word);
	//	  entry.addRef(fileNum);
	//	  this.words.add(entry);
	//	  this.sortedWordEntries= null;
	//	  this.footprint += entry.footprint();
	//  } else {
	//	  this.footprint += entry.addRef(fileNum);
	//  }
	//  return;

      // NEW CODE BELOW
        WordEntry entry= new WordEntry(word);
        entry.addRef(fileNum);
        this.words.add(entry);
        this.sortedWordEntries= null;
        this.footprint += entry.footprint();
    }

	public void addRef(IndexedFile indexedFile, char[] word) {
		addRef(word, indexedFile.getFileNumber());
	}

	/**
	 * Returns the footprint of the index.
	 */
	public long getFootprint() {
		return this.footprint;
	}

	/**
	 * Returns the indexed file with the given path, or null if such file does not exist.
	 */
	public IndexedFile getIndexedFile(String path) {
		return files.get(path);
	}

	/**
	 * @see IIndex#getNumDocuments()
	 */
	public int getNumFiles() {
		return files.size();
	}

	/**
	 * @see IIndex#getNumWords()
	 */
	public int getNumWords() {
		return words.size();
	}

	/**
	 * Returns the words contained in the hashtable of words, sorted by alphabetical order.
	 */
	protected IndexedFile[] getSortedFiles() {
		if (this.sortedFiles == null) {
			IndexedFile[] indexedFiles= files.asArray();
			Arrays.sort(indexedFiles);
			this.sortedFiles= indexedFiles;
		}
		return this.sortedFiles;
	}
	/**
	 * Returns the word entries contained in the hashtable of words, sorted by alphabetical order.
	 */
	protected WordEntry[] getSortedWordEntries() {
		if (this.sortedWordEntries == null) {
			WordEntry[] words= this.words.toArray(new WordEntry[this.words.size()]);
			Arrays.sort(words);
			this.sortedWordEntries= words;
		}
		return this.sortedWordEntries;
	}
	
	/**
	 * Initialises the fields of the index
	 */
	public void init() {
		words= new ArrayList<WordEntry>(256);
		files= new IndexedFileHashedArray(101);
		footprint= 0;
		sortedWordEntries= null;
		sortedFiles= null;
	}
	/**
	 * Saves the index in the given file.
	 * Structure of the saved Index :
	 *   - IndexedFiles in sorted order.
	 *		+ example:
	 *			"c:/com/Test.java 1"
	 *			"c:/com/UI.java 2"
	 *   - References with the words in sorted order
	 *		+ example:
	 *			"classDecl/Test 1"
	 *			"classDecl/UI 2"
	 *			"ref/String 1 2"
	 */

	public void save(File file) throws IOException {
		BlocksIndexOutput output= new BlocksIndexOutput(file);
		save(output);
	}
	/**
	 * Saves the index in the given IndexOutput.
	 * Structure of the saved Index :
	 *   - IndexedFiles in sorted order.
	 *		+ example:
	 *			"c:/com/Test.java 1"
	 *			"c:/com/UI.java 2"
	 *   - References with the words in sorted order
	 *		+ example:
	 *			"classDecl/Test 1"
	 *			"classDecl/UI 2"
	 *			"ref/String 1 2"
	 */

	protected void save(IndexOutput output) throws IOException {
		boolean ok= false;
		try {
			output.open();
			IndexedFile[] indexedFiles= files.asArray();
			for (int i= 0, length = indexedFiles.length; i < length; ++i)
				output.addFile(indexedFiles[i]); // written out in order BUT not alphabetical
			getSortedWordEntries(); // init the slot
			for (int i= 0, numWords= sortedWordEntries.length; i < numWords; ++i)
				output.addWord(sortedWordEntries[i]);
			output.flush();
			output.close();
			ok= true;
		} finally {
			if (!ok && output != null)
				output.close();
		}
	}
}
