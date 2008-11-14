/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.modeler.internal.core.index;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.FileUtil;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.core.util.TempDirectory;
import com.metamatrix.internal.core.index.Index;
import com.metamatrix.metadata.runtime.RuntimeMetadataPlugin;
import com.metamatrix.modeler.internal.core.workspace.ModelFileUtil;

/**
 * <p> This selector reads in a bunch of index files that the QueryMetadataInterface implementation needs
 * to use for looking up metadata. The index files could be part of a directory, and archive file or
 * an index file by itself.</p>  
 */
public class RuntimeIndexSelector extends AbstractIndexSelector {

    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################    

    private static final Random random = new Random(System.currentTimeMillis());

    //############################################################################################################################
    //# Variables                                                                                                                #
    //############################################################################################################################

    protected Index[] indexes;
    // indexes variable may be null so lock on this object
    private Object indexesLock = new Object();
    protected File vdbFile;
    protected String indexDirectoryPath;
    // size of file being returned by content methods
    private long fileSize;

    private TempDirectory tempDirectory;

    // ==================================================================================
    //                        C O N S T R U C T O R S
    // ==================================================================================

    /**
     * Construct an instance of RuntimeIndexSelector. The given file path could point to
     * <OL>
     *  <LI> A jar/zip file containing index files.
     *  <LI> A directory containing index files
     *  <LI> An index file
     * </OL>
     * @param filePath The location where the archive/index files are.
     */
    public RuntimeIndexSelector(final String filePath) {
        ArgCheck.isNotNull(filePath);
        this.vdbFile = new File(filePath);
        Assertion.assertTrue(this.vdbFile.exists(), "No file/directory exists at the given location " +filePath); //$NON-NLS-1$
        checkForValidFile();
    }

    /**
     * Construct an instance of RuntimeIndexSelector. The given file path could point to
     * <OL>
     *  <LI> A jar/zip file containing index files.
     *  <LI> A directory containing index files
     *  <LI> An index file
     * </OL>
     * @param filePath The location where the archive/index files are.
     */
    public RuntimeIndexSelector(final String vdbName, byte[] contents) throws IOException {
        ArgCheck.isNotNull(vdbName);
        ArgCheck.isNotNull(contents);
        try {
	        save(vdbName, contents);
			checkForValidFile();
        } catch(IOException e) {
            clearVDB();
            throw e;
        }
    }

	private void checkForValidFile() {
		if(this.vdbFile.isFile()) {
			Assertion.assertTrue(checkValidType(this.vdbFile), "Invalid file type, expected an archive file or an index file "+ vdbFile); //$NON-NLS-1$
		}
	}

    public RuntimeIndexSelector(final String vdbName, InputStream contents) throws IOException {
        ArgCheck.isNotNull(vdbName);
        ArgCheck.isNotNull(contents);
        try {
	        save(vdbName, contents);
	        checkForValidFile();
        } catch(IOException e) {
            clearVDB();
            throw e;
        }
    }
    /**
     * Construct an instance of RuntimeIndexSelector. The given url to
     * a jar/zip file containing index files.
     * @param vdbUrl The location where the archive/index files are.
     */
    public RuntimeIndexSelector(final URL vdbUrl) throws IOException {
        ArgCheck.isNotNull(vdbUrl);
        try {
	        save(vdbUrl);
	        checkForValidFile();
        } catch(IOException e) {
            clearVDB();
            throw e;
        }			
    }

    private void save(String vdbName, byte[] contents) throws IOException {
        String vdbFilePath = getIndexDirectoryPath() + FileUtils.SEPARATOR + vdbName;
        vdbFile = new File(vdbFilePath);
        FileUtils.write(contents, vdbFile);
    }
    
    private void save(String vdbName, InputStream contents) throws IOException {
        String vdbFilePath = getIndexDirectoryPath() + FileUtils.SEPARATOR + vdbName;
        vdbFile = new File(vdbFilePath);
        FileUtils.write(contents, vdbFile);
        contents.close();
    }    

    private void save(URL vdbUrl) throws IOException {
        String vdbPath = vdbUrl.getPath();
        int index = vdbPath.lastIndexOf(FileUtils.SEPARATOR);
        String vdbName = vdbPath.substring(index+1);
        InputStream vdbStream = vdbUrl.openStream();

        String vdbFilePath = getIndexDirectoryPath() + FileUtils.SEPARATOR + vdbName;
        this.vdbFile = new File(vdbFilePath);
        FileUtils.write(vdbStream, this.vdbFile);
    }
    
    //==================================================================================
    //                     I N T E R F A C E   M E T H O D S
    //==================================================================================

    /*
     * @see com.metamatrix.modeler.core.index.IndexSelector#getIndexes()
     */
    public Index[] getIndexes() throws IOException {
        if(indexes == null) {
            // initialize by reading in the index files
            // double cheking if indexes are available
            synchronized(indexesLock) {
                if(indexes == null) {
                    init();
                }
            }
        }
        return indexes;        
    }

    // ==================================================================================
    //                    P R O T E C T E D   M E T H O D S
    // ==================================================================================

    /**
     * Clear index files and the temporary directory location
     */
    public void clearVDB() {
        if(this.tempDirectory != null) {
            this.tempDirectory.remove();
            this.tempDirectory = null;
            this.indexDirectoryPath = null;
        }
        if(this.indexDirectoryPath != null) {
            File indexDirectory = new File( this.indexDirectoryPath );
            clear(indexDirectory);
            this.indexDirectoryPath = null;
        }
        setValid(false);
    }

    /**
     * Recursively delete the contents or the given directory.
     * @param directory 
     */
    private static void clear(File file) {
        if(file.exists()) {
	        if(file.isDirectory()) {
	            File[] indexFiles = file.listFiles();
	            if(indexFiles != null) {
	                for(int i=0; i < indexFiles.length; i++) {
	                    File indexFile = indexFiles[i];
	                    clear(indexFile);
	                }
	            }
	        }
	        if(!file.delete()) {
	            clearOnExit(file);
	        }
        }
    }

    /**
     * Recursively delete the contents or the given directory.
     * @param directory
     */
    private static void clearOnExit(File file) {
        if(file.exists()) {
	        if(file.isDirectory()) {
	            File[] indexFiles = file.listFiles();
	            if(indexFiles != null) {
	                for(int i=0; i < indexFiles.length; i++) {
	                    File indexFile = indexFiles[i];
	                    clearOnExit(indexFile);
	                }
	            }
	        }
	        file.deleteOnExit();
        }
    }

    /**
     * Reads the index files that are part of a Archive/directory and created MtkIndex objects.
     */
    protected void init() throws IOException {
        if(isArchive(this.vdbFile)) {
            indexes = loadIndexesFromZip(this.vdbFile);
        }
        // read index files in the given directory and create MtkIndexes
        else if(this.vdbFile.isDirectory()) {
            indexes = loadIndexesFromFolder(this.vdbFile);
        }
        // the file is an index file 
        else {
            indexes = loadIndexesFromFile(this.vdbFile);
        }
    }

    /**
     * Return the MtkIndex[] containing the specified file if it is a index file.
     * @param file
     * @return
     * @throws IOException
     */    
    protected Index[] loadIndexesFromFile(final File file) throws IOException {
        List<Index> tmp = new ArrayList<Index>();

        if (SimpleIndexUtil.indexFileExists(file.getAbsolutePath())) {
            tmp.add( new Index(file.getAbsolutePath(),true) );
        } 
        return tmp.toArray(new Index[tmp.size()]);
    }

    /**
     * Return the MtkIndex[] constructed from index files found in the specified folder
     * @param folder
     * @return
     * @throws IOException
     */
    protected Index[] loadIndexesFromFolder(final File folder) throws IOException {
        List<Index> tmp = new ArrayList<Index>();
        
        File[] files = folder.listFiles();
        for (int i=0; i < files.length; i++) {
            if (SimpleIndexUtil.indexFileExists(files[i].getAbsolutePath())) {
                tmp.add( new Index(files[i].getAbsolutePath(),true) );
            } 
        }
        return tmp.toArray(new Index[tmp.size()]);
    }

    /**
     * Return the MtkIndex[] constructed from index files found in the specified archive
     * @param folder
     * @return
     * @throws IOException
     */
    protected Index[] loadIndexesFromZip(final File zip) throws IOException {
        // the zip file that would be initialized
        // if the file being read is an archive        
        ZipFile zipFile = null;
        // inputStream of the zip File
		InputStream zipInputStream = null;
        try {
            zipFile = new ZipFile(zip);

            // List of MtkIndexes, one for each entry in the zip file
            List<Index> tmp = new ArrayList<Index>();

            // Iterate over all entries in the zip file ...
            for(final Enumeration entries = zipFile.entries();entries.hasMoreElements();) {
                // nee to read the entry and write an index file
                // to a temporary location
                ZipEntry entry = (ZipEntry) entries.nextElement();
                // extract the entry only if it is an index file or XSD file
                if (this.shouldExtract(entry)) {
                    // read the contents of the entry
                    zipInputStream = zipFile.getInputStream(entry);
					// Buffer that would contain the contents of the entry
					int length= entry.getSize() >= 0 ? (int) entry.getSize() : FileUtils.DEFAULT_BUFFER_SIZE;

                    // create a file at the temporary location writing
                    // the contents of the zip entry to this file
                    File entryFile = new File(getIndexDirectoryPath(), entry.getName());
                    FileUtils.write(zipInputStream, entryFile, length);

                    if (SimpleIndexUtil.indexFileExists(entryFile)) {
                        tmp.add( new Index(entryFile.getAbsolutePath(), true) );
                    }
                    //else if (IndexUtil.isIndexFile(entryFile)) {
                        //TODO: May need to log an error if index file is of zero length 
                    //}
                }
            }

            return tmp.toArray(new Index[tmp.size()]);
        } finally {
            if(zipFile != null) {
                zipFile.close();
            }
            if(zipInputStream != null) {
				zipInputStream.close();
            }
        }
    }

    /**
     * Return true if the speciifed entry should be extracted from
     * the archive
     * @param entry
     * @return
     */
    private boolean shouldExtract(final ZipEntry entry) {
//        if (entry != null) {
//            // get the entry that is an index file or XSD file
//            final IPath zipEntryPath = new Path(entry.getName());
//            final String extension   = zipEntryPath.getFileExtension();
//            if (extension.equalsIgnoreCase(IndexConstants.INDEX_EXT) || extension.equalsIgnoreCase(ModelUtil.EXTENSION_XSD)) {
//                return true;
//            }
//        }
        // we need to be able to access any file in the vdb
        // so extract all entries
        return true;
    }

    /** 
     * @see com.metamatrix.modeler.core.index.IndexSelector#getFilePaths()
     * @since 4.2
     */
    public String[] getFilePaths() {
        try {
	        ZipFile zipFile = new ZipFile(this.vdbFile);
	        List filePaths = new ArrayList();
	        // Iterate over all entries in the zip file ...
	        for(final Enumeration entries = zipFile.entries();entries.hasMoreElements();) {
	            // nee to read the entry and write an index file
	            // to a temporary location
	            ZipEntry entry = (ZipEntry) entries.nextElement();
	            String entryName = entry.getName();
	            // alays starts with file seperator
	            char firstChar = entryName.charAt(0);
	            if(firstChar != FileUtils.SEPARATOR) {
	                entryName = FileUtils.SEPARATOR+entryName;
	            }
	            filePaths.add(entryName);
	        }
	        // sort it just to be consistent on the order we return
	        Collections.sort(filePaths);
	        // get an array of paths from the collection
            return (String []) filePaths.toArray(new String[filePaths.size()]);
        } catch(IOException e) {
        	LogManager.logWarning(RuntimeMetadataPlugin.PLUGIN_ID, e, e.getMessage());
        }
        return super.getFilePaths();
    }

    /**
     * Read the contents of the files at the specefied paths in the index directory
     * and return the contents as String in a collection.
     */
    public List getFileContentsAsString(List paths) {
        ArgCheck.isNotEmpty(paths);
        List contents = new ArrayList(paths.size());
        for(final Iterator pathIter = paths.iterator();pathIter.hasNext();) {
            String relativePath = (String) pathIter.next();
            String fileContent = getFileContentAsString(relativePath);
            if(fileContent != null) {
                contents.add(fileContent);
            }
        }
        return contents;
    }

    /** 
     * @see com.metamatrix.modeler.core.index.IndexSelector#getFileContent(java.lang.String)
     * @since 4.2
     */
    public InputStream getFileContent(String path) {
        ArgCheck.isNotNull(path);
        File file = new File(getIndexDirectoryPath(), path);
        if(file.exists()) {
            this.fileSize = file.length();
            try {
	            // return contents of the file as inputstream
	            return new FileInputStream(file);
	        } catch(IOException e) {
	            LogManager.logWarning(RuntimeMetadataPlugin.PLUGIN_ID, e, e.getMessage());
	        }            
        }
        return null;
    }

    /** 
     * @see com.metamatrix.modeler.core.index.IndexSelector#getFile(java.lang.String)
     * @since 4.2
     */
    public File getFile(String path) {
        ArgCheck.isNotNull(path);
        File file = new File(getIndexDirectoryPath(), path);
        if(file.exists()) {
            return file;
        }
        return null;
    }

    /** 
     * @see com.metamatrix.modeler.core.index.IndexSelector#getFileContentAsString(java.lang.String)
     * @since 4.2
     */
    public String getFileContentAsString(String path) {
        ArgCheck.isNotNull(path);
        File file = new File(getIndexDirectoryPath(), path);
        if(file.exists()) {
            // Read the contents of a file into a string
            FileUtil fileUtil = new FileUtil(file.getPath());
            try {
                String fileContent = fileUtil.readSafe();
                this.fileSize = fileContent.length();
                return fileContent;
            } catch(Exception e) {
                // return null if file does not exist
            }
        }
        return null;
    }

    /** 
     * @see com.metamatrix.modeler.core.index.IndexSelector#getFileContent(java.lang.String, java.lang.String[], java.lang.String[])
     * @since 4.2
     */
    public InputStream getFileContent(final String path, final String[] tokens, final String[] tokenReplacements) {
        ArgCheck.isNotNull(tokens);
        ArgCheck.isNotNull(tokenReplacements);
        Assertion.isEqual(tokens.length, tokenReplacements.length);
        String fileContents = getFileContentAsString(path);
        if(fileContents != null) {
	        for(int i=0; i < tokens.length; i++) {
	            final String token = tokens[i];
	            final String tokenReplacement = tokenReplacements[i];
	            fileContents = StringUtil.replaceAll(fileContents, token, tokenReplacement);
	        }
	        this.fileSize = fileContents.length();
	        return new ByteArrayInputStream(fileContents.getBytes());
        }
        return null;
    }

    /** 
     * @see com.metamatrix.modeler.core.index.IndexSelector#getFileSize(java.lang.String)
     * @since 4.2
     */
    public long getFileSize(String path) {
        return this.fileSize;
    }

    /**
     * Returns the location of the directory into which index files will be written
     * @return
     */
    protected String getIndexDirectoryPath() {
        if (this.indexDirectoryPath == null) {
			this.tempDirectory = new TempDirectory(System.currentTimeMillis(), random.nextLong());
            this.tempDirectory.create();
            this.indexDirectoryPath = this.tempDirectory.getPath();
        }
        return this.indexDirectoryPath;
    }

    protected void setIndexDirectoryPath(final String path) {
        this.indexDirectoryPath = path;
        this.tempDirectory = null;        
    }

    /**
     * Check if the given file is of a valid file type. The valid types are
     * A zip/jar file or a MetaMatrix model file.
     * @param file The file whose type is being checked.
     * @return true if it a valid type
     */
    protected boolean checkValidType(File file) {
        String fileName = file.getName();
        if(isArchive(file) || SimpleIndexUtil.isIndexFile(fileName)) {
            return true;
        }
        return false;
    }

    /**
     * Check if the given file is an archive, by checking the type and extension.
     * @param file The File instance to check. 
     * @return true if it is an archive file.
     */
    protected boolean isArchive(final File file) {
        if (file != null && file.isFile() && file.exists()) {
            String fileName      = file.getName();
            String fileExtension = FileUtils.getExtension(file);
            if (ModelFileUtil.EXTENSION_VDB.equalsIgnoreCase(fileExtension) || FileUtils.isArchiveFileName(fileName)) {
                return true;
            }
        }
        return false;    
    }
    
    /**
     * Clean up the vdb if for some reason cleanVDB has not been
     * called explicitly. 
     * @see java.lang.Object#finalize()
     * @since 4.2
     */
    public void finalize() {
        this.clearVDB();
    }

}