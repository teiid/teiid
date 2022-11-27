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
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;

import org.teiid.core.index.IDocument;
import org.teiid.core.index.IEntryResult;
import org.teiid.core.index.IQueryResult;
import org.teiid.core.util.LRUCache;
import org.teiid.metadata.VDBResource;
import org.teiid.metadata.index.RuntimeMetadataPlugin;


/**
 * This input is used for reading indexes saved using a BlocksIndexOutput.
 */
public class BlocksIndexInput extends IndexInput {
    public static final int CACHE_SIZE= 16; // Cache 16 blocks of 8K each, for a cache size of 128K
    protected FileListBlock currentFileListBlock;
    protected int currentFileListBlockNum;
    protected int currentIndexBlockNum;
    protected IndexBlock currentIndexBlock;
    private RandomAccessFile raf;
    private VirtualRandomAccessFile vraf;
    protected VDBResource indexFile;
    protected LRUCache blockCache;

    protected IndexSummary summary;

    public BlocksIndexInput(VDBResource inputFile) {
        this.indexFile= inputFile;
        blockCache= new LRUCache(CACHE_SIZE);
    }

    /**
     * @see IndexInput#clearCache()
     */
    public void clearCache() {
        blockCache= new LRUCache(CACHE_SIZE);
    }

    /**
     * @see IndexInput#close()
     */
    public void close() throws IOException {
        if (isOpen()) {
            //System.out.println(" Closing Index File: " + this.indexFile.getName());
            summary= null;
            setOpen(false);
            if (raf != null) {
                raf.close();
            }
            if (vraf != null) {
                vraf.close();
            }
        }
    }

    /**
     * @see IndexInput#getCurrentFile()
     */
    public IndexedFile getCurrentFile() throws IOException {
        if (!hasMoreFiles())
            return null;
        IndexedFile file= null;
        if ((file= currentFileListBlock.getFile(filePosition)) == null) {
            currentFileListBlockNum= summary.getBlockNumForFileNum(filePosition);
            currentFileListBlock= getFileListBlock(currentFileListBlockNum);
            file= currentFileListBlock.getFile(filePosition);
        }
        return file;
    }

    /**
     * Returns the entry corresponding to the given word.
     */
    protected WordEntry getEntry(char[] word) throws IOException {
        int blockNum= summary.getBlockNumForWord(word);
        if (blockNum == -1) return null;
        IndexBlock block= getIndexBlock(blockNum);
        return block.findExactEntry(word);
    }

    /**
     * Returns the FileListBlock with the given number.
     */
    protected FileListBlock getFileListBlock(int blockNum) throws IOException {
        Integer key= new Integer(blockNum);
        Block block= (Block) blockCache.get(key);
        if (block != null && block instanceof FileListBlock)
            return (FileListBlock) block;
        FileListBlock fileListBlock= new FileListBlock(IIndexConstants.BLOCK_SIZE);
        fileListBlock.read(raf, blockNum);
        blockCache.put(key, fileListBlock);
        return fileListBlock;
    }

    /**
     * Returns the IndexBlock (containing words) with the given number.
     */
    protected IndexBlock getIndexBlock(int blockNum) throws IOException {
        Integer key= new Integer(blockNum);
        Block block= (Block) blockCache.get(key);
        if (block != null && block instanceof IndexBlock)
            return (IndexBlock) block;
        IndexBlock indexBlock= new GammaCompressedIndexBlock(IIndexConstants.BLOCK_SIZE);
        indexBlock.read(raf, blockNum);
        blockCache.put(key, indexBlock);
        return indexBlock;
    }

    /**
     * @see IndexInput#getIndexedFile(int)
     */
    public IndexedFile getIndexedFile(int fileNum) throws IOException {
        int blockNum= summary.getBlockNumForFileNum(fileNum);
        if (blockNum == -1)
            return null;
        FileListBlock block= getFileListBlock(blockNum);
        return block.getFile(fileNum);
    }

    /**
     * @see IndexInput#getIndexedFile(IDocument)
     */
    public IndexedFile getIndexedFile(IDocument document) throws java.io.IOException {
        setFirstFile();
        String name= document.getName();
        while (hasMoreFiles()) {
            IndexedFile file= getCurrentFile();
            String path= file.getPath();
            if (path.equals(name))
                return file;
            moveToNextFile();
        }
        return null;
    }

    /**
     * Returns the list of numbers of files containing the given word.
     */
    protected int[] getMatchingFileNumbers(char[] word) throws IOException {
        int blockNum= summary.getBlockNumForWord(word);
        if (blockNum == -1)
            return new int[0];
        IndexBlock block= getIndexBlock(blockNum);
        WordEntry entry= block.findExactEntry(word);
        return entry == null ? new int[0] : entry.getRefs();
    }

    /**
     * @see IndexInput#getNumFiles()
     */
    public int getNumFiles() {
        return summary.getNumFiles();
    }

    /**
     * @see IndexInput#getNumWords()
     */
    public int getNumWords() {
        return summary.getNumWords();
    }

    /**
     * @see IndexInput#getSource()
     */
    public Object getSource() {
        return indexFile;
    }

    /**
     * Initialises the blocksIndexInput
     */
    protected void init() throws IOException {
        clearCache();
        setFirstFile();
        setFirstWord();
    }
    /**
     * @see IndexInput#moveToNextFile()
     */
    public void moveToNextFile() throws IOException {
        filePosition++;
    }

    /**
     * @see IndexInput#moveToNextWordEntry()
     */
    public void moveToNextWordEntry() throws IOException {
        wordPosition++;
        if (!hasMoreWords()) {
            return;
        }
        //if end of the current block, we load the next one.
        boolean endOfBlock= !currentIndexBlock.nextEntry(currentWordEntry);
        if (endOfBlock) {
            currentIndexBlock= getIndexBlock(++currentIndexBlockNum);
            currentIndexBlock.nextEntry(currentWordEntry);
        }
    }

    /**
     * @see IndexInput#open()
     */
    public void open() throws IOException {
        if (!isOpen()) {
            vraf = new VirtualRandomAccessFile(indexFile, "r");
            raf= vraf.getSafeRandomAccessFile();
            String sig= raf.readUTF();
            if (!sig.equals(IIndexConstants.SIGNATURE))
                throw new IOException(RuntimeMetadataPlugin.Util.getString("exception.wrongFormat")); //$NON-NLS-1$
            int summaryBlockNum= raf.readInt();
            raf.seek(summaryBlockNum * (long) IIndexConstants.BLOCK_SIZE);
            summary= new IndexSummary();
            summary.read(raf);
            init();
            setOpen(true);
        }
    }

    public IQueryResult[] query(String word) throws IOException {
        open();
        int[] fileNums= getMatchingFileNumbers(word.toCharArray());
        int size= fileNums.length;
        IQueryResult[] files= new IQueryResult[size];
        for (int i= 0; i < size; ++i) {
            files[i]= getIndexedFile(fileNums[i]);
        }
        return files;
    }

    /**
     * Overloaded the method in BlocksIndexInput to allow a user to specify if the
     * query should be case sensitive.
     * @param pattern
     * @param isCaseSensitive
     * @return
     * @throws IOException
     */
    public IEntryResult[] queryEntriesMatching(char[] pattern, boolean isCaseSensitive) throws IOException {
        open();

        if (pattern == null || pattern.length == 0) return null;
        int[] blockNums = null;
        int firstStar = indexOf('*', pattern);
        switch (firstStar){
            case -1 :
                WordEntry entry = getEntry(pattern);
                if (entry == null) return null;
                return new IEntryResult[]{ new EntryResult(entry.getWord(), entry.getRefs()) };
            case 0 :
                blockNums = summary.getAllBlockNums();
                break;
            default :
                char[] prefix = new char[firstStar];
                System.arraycopy(pattern, 0, prefix, 0, firstStar);
                blockNums = summary.getBlockNumsForPrefix(prefix);
        }
        if (blockNums == null || blockNums.length == 0) return null;

        IEntryResult[] entries = new IEntryResult[5];
        int count = 0;
        for (int i = 0, max = blockNums.length; i < max; i++) {
            IndexBlock block = getIndexBlock(blockNums[i]);
            block.reset();
            WordEntry entry = new WordEntry();
            while (block.nextEntry(entry)) {
                // Switch argument order to fix bug in BlocksIndexInput
                if (CharOperation.match(pattern, entry.getWord(), isCaseSensitive)) {
                    if (count == entries.length){
                        System.arraycopy(entries, 0, entries = new IEntryResult[count*2], 0, count);
                    }
                    entries[count++] = new EntryResult(entry.getWord(), entry.getRefs());
                }
            }
        }
        if (count != entries.length){
            System.arraycopy(entries, 0, entries = new IEntryResult[count], 0, count);
        }
        return entries;
    }

    static final int indexOf(char toBeFound, char[] array) {
        for (int i = 0; i < array.length; i++)
            if (toBeFound == array[i])
                return i;
        return -1;
    }

    public IEntryResult[] queryEntriesPrefixedBy(char[] prefix) throws IOException {
        open();

        int blockLoc = summary.getFirstBlockLocationForPrefix(prefix);
        if (blockLoc < 0) return null;

        IEntryResult[] entries = new IEntryResult[5];
        int count = 0;
        while(blockLoc >= 0){
            IndexBlock block = getIndexBlock(summary.getBlockNum(blockLoc));
            block.reset();
            boolean found = false;
            WordEntry entry = new WordEntry();
            while (block.nextEntry(entry)) {
                if (CharOperation.prefixEquals(prefix, entry.getWord(), true)) {
                    if (count == entries.length){
                        System.arraycopy(entries, 0, entries = new IEntryResult[count*2], 0, count);
                    }
                    entries[count++] = new EntryResult(entry.getWord(), entry.getRefs());
                    found = true;
                } else {
                    if (found) break;
                }
            }
            /* consider next block ? */
            blockLoc = summary.getNextBlockLocationForPrefix(prefix, blockLoc);
        }
        if (count == 0) return null;
        if (count != entries.length){
            System.arraycopy(entries, 0, entries = new IEntryResult[count], 0, count);
        }
        return entries;
    }

    /**
     * Overloaded the method in BlocksIndexInput to allow a user to specify if the
     * query should be case sensitive.
     */
    public IEntryResult[] queryEntriesPrefixedBy(char[] prefix, boolean isCaseSensitive) throws IOException {
        open();

        int blockLoc = summary.getFirstBlockLocationForPrefix(prefix);
        if (blockLoc < 0) return null;

        IEntryResult[] entries = new IEntryResult[5];
        int count = 0;
        while(blockLoc >= 0){
            IndexBlock block = getIndexBlock(summary.getBlockNum(blockLoc));
            block.reset();
            boolean found = false;
            WordEntry entry = new WordEntry();
            while (block.nextEntry(entry)) {
                if (CharOperation.prefixEquals(prefix, entry.getWord(), isCaseSensitive)) {
                    if (count == entries.length){
                        System.arraycopy(entries, 0, entries = new IEntryResult[count*2], 0, count);
                    }
                    entries[count++] = new EntryResult(entry.getWord(), entry.getRefs());
                    found = true;
                } else {
                    if (found) break;
                }
            }
            /* consider next block ? */
            blockLoc = summary.getNextBlockLocationForPrefix(prefix, blockLoc);
        }
        if (count == 0) return null;
        if (count != entries.length){
            System.arraycopy(entries, 0, entries = new IEntryResult[count], 0, count);
        }
        return entries;
    }

    public IQueryResult[] queryFilesReferringToPrefix(char[] prefix) throws IOException {
        open();

        int blockLoc = summary.getFirstBlockLocationForPrefix(prefix);
        if (blockLoc < 0) return null;

        // each filename must be returned already once
        HashMap<Integer, IndexedFile> fileMatches = new HashMap<Integer, IndexedFile>();
        int count = 0;
        while(blockLoc >= 0){
            IndexBlock block = getIndexBlock(summary.getBlockNum(blockLoc));
            block.reset();
            boolean found = false;
            WordEntry entry = new WordEntry();
            while (block.nextEntry(entry)) {
                if (CharOperation.prefixEquals(prefix, entry.getWord(), true)) {
                    int [] refs = entry.getRefs();
                    for (int i = 0, max = refs.length; i < max; i++){
                        int ref = refs[i];
                        Integer key = Integer.valueOf(ref);
                        if (!fileMatches.containsKey(key)){
                            count++;
                            fileMatches.put(key, getIndexedFile(ref));
                        }
                    }
                    found = true;
                } else {
                    if (found) break;
                }
            }
            /* consider next block ? */
            blockLoc = summary.getNextBlockLocationForPrefix(prefix, blockLoc);
        }
        /* extract indexed files */
        IQueryResult[] files = fileMatches.values().toArray(new IQueryResult[count]);
        return files;
    }

    /**
     * @see IndexInput#queryInDocumentNames(String)
     */
    public IQueryResult[] queryInDocumentNames(String word) throws IOException {
        open();
        ArrayList matches= new ArrayList();
        setFirstFile();
        while (hasMoreFiles()) {
            IndexedFile file= getCurrentFile();
            if (file.getPath().indexOf(word) != -1)
                matches.add(file);
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
        if (getNumFiles() > 0) {
            currentFileListBlockNum= summary.getBlockNumForFileNum(1);
            currentFileListBlock= getFileListBlock(currentFileListBlockNum);
        }
    }

    /**
     * @see IndexInput#setFirstWord()
     */
    protected void setFirstWord() throws IOException {
        wordPosition= 1;
        if (getNumWords() > 0) {
            currentIndexBlockNum= summary.getFirstWordBlockNum();
            currentIndexBlock= getIndexBlock(currentIndexBlockNum);
            currentWordEntry= new WordEntry();
            currentIndexBlock.reset();
            currentIndexBlock.nextEntry(currentWordEntry);
        }
    }
}
