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

import java.io.IOException;
import java.util.ArrayList;

import org.teiid.core.index.IDocument;
import org.teiid.core.index.IEntryResult;
import org.teiid.core.index.IQueryResult;


/**
 * A simpleIndexInput is an input on an in memory Index. 
 */

public class SimpleIndexInput extends IndexInput {
	protected WordEntry[] sortedWordEntries;
	protected IndexedFile currentFile;
	protected IndexedFile[] sortedFiles;
	protected InMemoryIndex index;

	public SimpleIndexInput(InMemoryIndex index) {
		super();
		this.index= index;
	}
	/**
	 * @see IndexInput#clearCache()
	 */
	public void clearCache() {
	}
	/**
	 * @see IndexInput#close()
	 */
	public void close() throws IOException {
		sortedFiles= null;
	}
	/**
	 * @see IndexInput#getCurrentFile()
	 */
	public IndexedFile getCurrentFile() throws IOException {
		if (!hasMoreFiles())
			return null;
		return currentFile;
	}
	/**
	 * @see IndexInput#getIndexedFile(int)
	 */
	public IndexedFile getIndexedFile(int fileNum) throws IOException {
		for (int i= 0; i < sortedFiles.length; i++)
			if (sortedFiles[i].getFileNumber() == fileNum)
				return sortedFiles[i];
		return null;
	}
	/**
	 * @see IndexInput#getIndexedFile(IDocument)
	 */
	public IndexedFile getIndexedFile(IDocument document) throws IOException {
		String name= document.getName();
		for (int i= index.getNumFiles(); i >= 1; i--) {
			IndexedFile file= getIndexedFile(i);
			if (name.equals(file.getPath()))
				return file;
		}
		return null;
	}
	/**
	 * @see IndexInput#getNumFiles()
	 */
	public int getNumFiles() {
		return index.getNumFiles();
	}
	/**
	 * @see IndexInput#getNumWords()
	 */
	public int getNumWords() {
		return sortedWordEntries.length;
	}
	/**
	 * @see IndexInput#getSource()
	 */
	public Object getSource() {
		return index;
	}
	public void init() {
		index.init();

	}
	/**
	 * @see IndexInput#moveToNextFile()
	 */
	public void moveToNextFile() throws IOException {
		filePosition++;
		if (!hasMoreFiles()) {
			return;
		}
		currentFile= sortedFiles[filePosition - 1];
	}
	/**
	 * @see IndexInput#moveToNextWordEntry()
	 */
	public void moveToNextWordEntry() throws IOException {
		wordPosition++;
		if (hasMoreWords())
			currentWordEntry= sortedWordEntries[wordPosition - 1];
	}
	/**
	 * @see IndexInput#open()
	 */
	public void open() throws IOException {
		sortedWordEntries= index.getSortedWordEntries();
		sortedFiles= index.getSortedFiles();
		filePosition= 1;
		wordPosition= 1;
		setFirstFile();
		setFirstWord();
	}

	public IEntryResult[] queryEntriesPrefixedBy(char[] prefix) throws IOException {
		return null;
	}
	public IQueryResult[] queryFilesReferringToPrefix(char[] prefix) throws IOException {
			return null;
	}
	/**
	 * @see IndexInput#queryInDocumentNames(String)
	 */
	public IQueryResult[] queryInDocumentNames(String word) throws IOException {
		setFirstFile();
		ArrayList matches= new ArrayList();
		while (hasMoreFiles()) {
			IndexedFile file= getCurrentFile();
			if (file.getPath().indexOf(word) != -1)
				matches.add(file.getPath());
			moveToNextFile();
		}
		IQueryResult[] match= new IQueryResult[matches.size()];
		matches.toArray(match);
		return match;
	}
	/**
	 * @see IndexInput#setFirstFile()
	 */
	protected void setFirstFile() throws IOException {
		filePosition= 1;
		if (sortedFiles.length > 0) {
			currentFile= sortedFiles[0];
		}
	}
	/**
	 * @see IndexInput#setFirstWord()
	 */
	protected void setFirstWord() throws IOException {
		wordPosition= 1;
		if (sortedWordEntries.length > 0)
			currentWordEntry= sortedWordEntries[0];
	}
}
