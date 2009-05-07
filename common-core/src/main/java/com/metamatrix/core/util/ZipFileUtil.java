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

package com.metamatrix.core.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import com.metamatrix.core.CorePlugin;

/**
 * This class provides utilities to manipulate Zip files.
 */
public final class ZipFileUtil {

    // ===========================================================================================================================
    // Constants

    // specify buffer size for extraction
    static final int BUFFER = 16384;

    static final String TMP_PFX = "ZipFileUtil"; //$NON-NLS-1$
    static final String TMP_SFX = ".zip"; //$NON-NLS-1$

    // ===========================================================================================================================
    // Static Controller Methods

    /**
     * Adds the file with the specified name to the zip file with the specified name.
     * 
     * @param zipFileName
     *            The name of the zip file to which the file with the specified name will be added.
     * @param fileName
     *            The name of the file to be added.
     * @return True if the file with the specified name was successfully added to the zip file with the specified name.
     * @throws IOException
     *             If an I/O error occurs updating the zip file.
     * @test {@link TestZipFileUtil#testAddRemove()}
     * @since 4.3
     */
    public static boolean add(final String zipFileName,
                              final String fileName)  throws IOException {
        ArgCheck.isNotEmpty(zipFileName);
        ArgCheck.isNotEmpty(fileName);
        
        List fileNames = new ArrayList();
        fileNames.add(fileName);
        return addAll(new File(zipFileName), fileNames, fileNames);
    }
    
    /**
     * Adds the file with the specified name to the zip file with the specified name.
     * 
     * @param zipFileName
     *            The name of the zip file to which the file with the specified name will be added.
     * @param entryName
     *            The name to call the entry in the zip file.            
     * @param fileName
     *            The name of the file to be added.
     * @return True if the file with the specified name was successfully added to the zip file with the specified name.
     * @throws IOException
     *             If an I/O error occurs updating the zip file.
     * @test {@link TestZipFileUtil#testAddRemove()}
     * @since 4.3
     */
    public static boolean add(final String zipFileName,
                              final String entryName,
                              final String fileName) throws IOException {
        ArgCheck.isNotEmpty(zipFileName);
        ArgCheck.isNotEmpty(entryName);
        ArgCheck.isNotEmpty(fileName);
        
        List fileNames = new ArrayList();
        fileNames.add(fileName);
        List entryNames = new ArrayList();
        entryNames.add(entryName);
        return addAll(new File(zipFileName), entryNames, fileNames);
    }    
    
    /**
     * Adds the file with the specified name to the specified zip file.
     * 
     * @param zipFile
     *            The zip file to which the file with the specified name will be added.
     * @param fileName
     *            The name of the file to be added.
     * @return True if the file with the specified name was successfully added to the specified zip file.
     * @throws IOException
     *             If an I/O error occurs updating the zip file.
     * @test {@link TestZipFileUtil#testAddRemove()}
     * @since 4.3
     */
    public static boolean add(final File zipFile,
                              final String fileName)  throws IOException {
        ArgCheck.isNotNull(zipFile);
        ArgCheck.isNotEmpty(fileName);
        
        List fileNames = new ArrayList();
        fileNames.add(fileName);
        return addAll(zipFile, fileNames, fileNames);
    }
    
    
    
    /**
     * Adds all of the files in the specified directory to the specified zip file.
     * 
     * @param zipFile
     *            The zip file to which the file with the specified name will be added.
     * @param dirName
     *            The name of the directory which contains the files to be added.
     * @return True if the files wes successfully added to the specified zip file.
     * @throws IOException
     *             If an I/O error occurs updating the zip file.
     * @since 4.3
     */
    public static boolean addAll(final File zipFile,
                              final String dirName) throws IOException {
        return addAll(zipFile, dirName, ""); //$NON-NLS-1$
    }
    
    /**
     * Adds all of the files in the specified directory to the specified zip file.
     * 
     * @param zipFile
     *            The zip file to which the file with the specified name will be added.
     * @param dirName
     *            The name of the directory which contains the files to be added.
     * @param pathInZip
     *            Path in which the files should be added in the zip.
     * @return True if the files wes successfully added to the specified zip file.
     * @throws IOException
     *             If an I/O error occurs updating the zip file.
     * @since 4.3
     */
    public static boolean addAll(final File zipFile,
                              final String dirName,
                              String pathInZip) throws IOException {
        ArgCheck.isNotNull(zipFile);
        ArgCheck.isNotEmpty(dirName);
        
        
        File dir = new File(dirName);
        //get the entrynames (relative path)
        if (pathInZip.length() > 0) {
            pathInZip = pathInZip + File.separator;
        }
        List entryNames = listRecursively(dir, pathInZip);
        //get the filenames (full path)
        List fileNames = listRecursively(dir, dir.getAbsolutePath() + File.separator);
        
        return addAll(zipFile, entryNames, fileNames);
    }
    

    /**
     * List recursively from the <code>sourceDirectory</code> all its contents. 
     * @param sourceDirectory
     * @param pathSoFar Path tp append to the beginning of each result.
     * @return List of Strings, the filenames of the contents of the directory.
     * @since 4.3
     */
    private static List listRecursively(File sourceDirectory, String pathSoFar) throws FileNotFoundException {
        
        if (!sourceDirectory.exists()) {
            throw new FileNotFoundException(CorePlugin.Util.getString("FileUtils.File_does_not_exist._1", sourceDirectory)); //$NON-NLS-1$
        }        
        if (!sourceDirectory.isDirectory()) {
            throw new FileNotFoundException(CorePlugin.Util.getString("FileUtils.Not_a_directory", sourceDirectory)); //$NON-NLS-1$
        }
        
        return listRecursivelySub(sourceDirectory, pathSoFar); 
    }
    
    private static List listRecursivelySub(File sourceDirectory, String pathSoFar) {
        List results = new ArrayList();
        
        File[] sourceFiles = sourceDirectory.listFiles();
        for (int i = 0; i < sourceFiles.length; i++) {
            File srcFile = sourceFiles[i];
            String newPathSoFar = pathSoFar + srcFile.getName();
            if (srcFile.isDirectory()) {
                results.addAll(listRecursivelySub(srcFile, newPathSoFar + File.separator));
            } else {
                results.add(newPathSoFar);
            }
        }
        return results;
    }
    
    
    

    /**
     * Adds the files with the specified names to the specified zip file.
     * 
     * @param zipFile
     *            The zip file to which the file with the specified name will be added.
     * @param entryNames
     *            List of Strings.  The names to call the entries in the zip file.  Must be in the same order as <code>fileNames</code>.
     * @param fileNames
     *            List of Files.  The names of the file to be added.  Must be in the same order as <code>entryNames</code>.           
     * @return True if the file with the specified name was successfully added to the specified zip file.
     * @throws IOException
     *             If an I/O error occurs updating the zip file.
     * @test {@link TestZipFileUtil#testAddRemove()}
     * @since 4.3
     */
    private static boolean addAll(final File zipFile,
                              final List entryNames,
                              final List fileNames) throws IOException {
        FileOutputStream fos = null;
        ZipOutputStream out = null;
        File tmpFile = File.createTempFile(TMP_PFX, TMP_SFX);
        try {
            // Create temp zip file
            fos = new FileOutputStream(tmpFile);
            out = new ZipOutputStream(fos);
            // Create buffer to use to write data to temp zip file
            final byte[] buf = new byte[BUFFER];
            // Copy specified zip file to temp zip file
            if (zipFile.exists()) {
                ZipFile zipFileZip = null;
                FileInputStream fis = null;
                ZipInputStream in = null;
                try {
                    zipFileZip = new ZipFile(zipFile);
                    fis = new FileInputStream(zipFile);
                    in = new ZipInputStream(fis);
                    for (ZipEntry entry = in.getNextEntry(); entry != null; entry = in.getNextEntry()) {
                        writeEntry(entry, zipFileZip.getInputStream(entry), out, buf);
                    }
                } catch (final ZipException ignored) {
                    // Happens when zip file empty or contains no entries
                } finally {
                    // Close input streams so we can later replace specified file with temp file
                    if (zipFileZip != null) {
                        zipFileZip.close();
                    }
                    cleanup(in);
                    cleanup(fis);
                }
            }
            
            // Add specified entries to temp zip file
            Iterator entryNamesIter = entryNames.iterator();
            Iterator fileNamesIter = fileNames.iterator();
            while (entryNamesIter.hasNext() && fileNamesIter.hasNext()) {                
                String entryName = (String) entryNamesIter.next();                
                String fileName = (String) fileNamesIter.next();
                long modified = new File(fileName).lastModified();
                
                FileInputStream fis = new FileInputStream(fileName);
                ZipEntry entry = new ZipEntry(entryName);
                entry.setTime(modified);
                writeEntry(entry, fis, out, buf);
                // Close output stream so we can replace specified file with temp file
                // (Also set variable to null so we don't close again in finally block)
                cleanup(fis);
            }

            
            out.close();
            out = null;
            fos.close();
            fos = null;
            // Replace specified file with temp file
            FileUtils.rename(tmpFile.getAbsolutePath(), zipFile.getAbsolutePath(), true);

            
            return true;
        } finally {
            cleanup(out);
            cleanup(fos);
            if (tmpFile != null && tmpFile.exists() && !tmpFile.delete()) {
            	Logger.getLogger("org.teiid.common-core").log(Level.INFO, "Could not delete temp file " + tmpFile.getAbsolutePath()); //$NON-NLS-1$ //$NON-NLS-2$
            }
        }
    }

    /**
     * Extract the given zip file to the given destination directory base.
     * 
     * @param zipFileName
     *            The full path and file name of the Zip file to extract.
     * @param destinationDirectory
     *            The root directory to extract to.
     * @throws IOException
     */
    public static void extract(final String zipFileName,
                               final String destinationDirectory) throws IOException {
        try {
            File sourceZipFile = new File(zipFileName);
            File unzipDestinationDirectory = new File(destinationDirectory);

            // Open Zip file for reading
            ZipFile zipFile = new ZipFile(sourceZipFile, ZipFile.OPEN_READ);

            // Create an enumeration of the entries in the zip file
            Enumeration zipFileEntries = zipFile.entries();

            // Process each entry
            while (zipFileEntries.hasMoreElements()) {
                // grab a zip file entry
                ZipEntry entry = (ZipEntry)zipFileEntries.nextElement();

                String currentEntry = entry.getName();

                File destFile = new File(unzipDestinationDirectory, currentEntry);

                // grab file's parent directory structure
                File destinationParent = destFile.getParentFile();

                // create the parent directory structure if needed
                destinationParent.mkdirs();

                // extract file if not a directory
                if (!entry.isDirectory()) {
                    BufferedInputStream is = new BufferedInputStream(zipFile.getInputStream(entry));
                    int currentByte;
                    // establish buffer for writing file
                    byte data[] = new byte[BUFFER];

                    // write the current file to disk
                    FileOutputStream fos = new FileOutputStream(destFile);
                    BufferedOutputStream dest = new BufferedOutputStream(fos, BUFFER);

                    // read and write until last byte is encountered
                    while ((currentByte = is.read(data, 0, BUFFER)) != -1) {
                        dest.write(data, 0, currentByte);
                    }
                    dest.flush();
                    dest.close();
                    fos.close();
                    is.close();
                }
            }
            zipFile.close();
        } catch (IOException ioe) {
            throw ioe;
        }
    }
    
    /**
     * Removes the file with the specified name from the zip file with the specified name.
     * 
     * @param zipFileName
     *            The name of the zip file from which the file with the specified name will be removed.
     * @param fileName
     *            The name of the file to be removed.
     * @param ignoreCase
     *            Indicates whether the case of the filename should be ignored when finding the match.           
     * @return True if the file with the specified name was successfully removed from the zip file with the specified name.
     * @throws IOException
     *             If an I/O error occurs updating the zip file.
     * @test {@link TestZipFileUtil#testAddRemove()}
     * @since 4.3
     */
    public static boolean remove(final String zipFileName,
                                 final String fileName,
                                 boolean ignoreCase) throws IOException {
        ArgCheck.isNotEmpty(zipFileName);
        return remove(new File(zipFileName), fileName, ignoreCase);
    }

    /**
     * Removes the file with the specified name from the zip file with the specified name.
     * 
     * @param zipFileName
     *            The name of the zip file from which the file with the specified name will be removed.
     * @param fileName
     *            The name of the file to be removed.
     * @return True if the file with the specified name was successfully removed from the zip file with the specified name.
     * @throws IOException
     *             If an I/O error occurs updating the zip file.
     * @test {@link TestZipFileUtil#testAddRemove()}
     * @since 4.3
     */
    public static boolean remove(final String zipFileName,
                                 final String fileName) throws IOException {
        ArgCheck.isNotEmpty(zipFileName);
        return remove(new File(zipFileName), fileName, false);
    }

    /**
     * Removes the file with the specified name to the specified zip file.
     * 
     * @param zipFile
     *            The zip file from which the file with the specified name will be removed.
     * @param fileName
     *            The name of the file to be removed.
     * @return True if the file with the specified name was successfully removed from the specified zip file.
     * @throws IOException
     *             If an I/O error occurs updating the zip file.
     * @test {@link TestZipFileUtil#testAddRemove()}
     * @since 4.3
     */
    public static boolean remove(final File zipFile,
                                 final String fileName)  throws IOException {
        ArgCheck.isNotNull(zipFile);
        ArgCheck.isNotEmpty(fileName);
        
        return remove(zipFile, fileName, false);
    }
    
    /**
     * Removes the file with the specified name to the specified zip file.
     * 
     * @param zipFile
     *            The zip file from which the file with the specified name will be removed.
     * @param fileName
     *            The name of the file to be removed.
     * @param ignoreCase
     *            Indicates whether the case of the filename should be ignored when finding the match.           
     * @return True if the file with the specified name was successfully removed from the specified zip file.
     * @throws IOException
     *             If an I/O error occurs updating the zip file.
     * @test {@link TestZipFileUtil#testAddRemove()}
     * @since 4.3
     */
    public static boolean remove(final File zipFile,
                                 final String fileName,
                                 boolean ignoreCase)  throws IOException {
        ArgCheck.isNotNull(zipFile);
        ArgCheck.isNotEmpty(fileName);   
        FileOutputStream fos = null;
        ZipOutputStream out = null;
        File tmpFile = File.createTempFile(TMP_PFX, TMP_SFX);
        try {
            // Create temp zip file
            fos = new FileOutputStream(tmpFile);
            out = new ZipOutputStream(fos);
            // Create buffer to use to write data to temp zip file
            final byte[] buf = new byte[BUFFER];
            // Copy specified zip file to temp zip file, skipping entry with specified file name
            final ZipFile zipFileZip = new ZipFile(zipFile);
            FileInputStream fis = null;
            ZipInputStream in = null;
            try {
                fis = new FileInputStream(zipFile);
                in = new ZipInputStream(fis);
                for (ZipEntry entry = in.getNextEntry(); entry != null; entry = in.getNextEntry()) {
                    if (ignoreCase) {
                        if (!entry.getName().equalsIgnoreCase(fileName)) {
                            writeEntry(entry, zipFileZip.getInputStream(entry), out, buf);
                        }
                    } else {
                        if (!entry.getName().equals(fileName)) {
                            writeEntry(entry, zipFileZip.getInputStream(entry), out, buf);
                        }
                        
                    }
                }
            } finally {
                // Close input streams so we can later replace specified file with temp file
                zipFileZip.close();
                cleanup(in);
                cleanup(fis);
            }
            // Close output stream so we can replace specified file with temp file
            // (Also set variable to null so we don't close again in finally block)
            out.close();
            out = null;
            fos.close();
            fos = null;
            // Replace specified file with temp file
            FileUtils.rename(tmpFile.getAbsolutePath(), zipFile.getAbsolutePath(), true);
            return true;

        } finally {
            cleanup(out);
            cleanup(fos);
        }        
    }
    
    
    public static boolean replace (final File zipFile, final String replaceName, InputStream replaceStream) throws IOException {
		ArgCheck.isNotNull(zipFile);
		ArgCheck.isNotEmpty(replaceName);
		
		FileOutputStream fos = null;
		ZipOutputStream out = null;
		File tmpFile = File.createTempFile(TMP_PFX, TMP_SFX);
		
		try {
			// Create temp zip file
			fos = new FileOutputStream(tmpFile);
			out = new ZipOutputStream(fos);
		
			// Create buffer to use to write data to temp zip file
			final byte[] buf = new byte[BUFFER];

			// Copy specified zip file to temp zip file, skipping entry with
			// specified file name
			final ZipFile zipFileZip = new ZipFile(zipFile);
			FileInputStream fis = null;
			ZipInputStream in = null;
			boolean replaced = false;
			
			try {
				fis = new FileInputStream(zipFile);
				in = new ZipInputStream(fis);
				for (ZipEntry entry = in.getNextEntry(); entry != null; entry = in.getNextEntry()) {
					boolean replace = entry.getName().equals(replaceName);
					if (replace) {
						writeEntry(new ZipEntry(entry.getName()), replaceStream, out, buf);		
						replaced = true;
					} else {
						writeEntry(new ZipEntry(entry.getName()), zipFileZip.getInputStream(entry),out, buf);   
					}
				}				
			} finally {
				// Close input streams so we can later replace specified file
				// with temp file
				zipFileZip.close();
				cleanup(in);
				cleanup(fis);
			}
			
			// if the original was not there then add a new entry
			if (!replaced) {
				ZipEntry entry = new ZipEntry(replaceName);
				writeEntry(entry, replaceStream, out, buf);
			}
			
			// Close output stream so we can replace specified file with temp
			// file
			// (Also set variable to null so we don't close again in finally
			// block)
			out.close();
			out = null;
			fos.close();
			fos = null;
			
			// Replace specified file with temp file
			try {
				FileUtils.rename(tmpFile.getAbsolutePath(), zipFile.getAbsolutePath(), true);
			} catch (Exception e) {
				IOException ex = new IOException();
				ex.initCause(e);
				throw ex;
			}
			return true;

		} finally {
			cleanup(out);
			cleanup(fos);
		}
	}
    
    /**
     * Returns the file with the specified name to the specified zip file.
     * 
     * @param zipFile
     *            The zip file from which the file with the specified name will be retrieved from.
     * @param fileName
     *            The name of the file to be returned.
     * @param ignoreCase
     *            Indicates whether the case of the filename should be ignored when finding the match.           
     * @return True if the file with the specified name was successfully removed from the specified zip file.
     * @throws IOException
     *             If an I/O error occurs updating the zip file.
     * @test {@link TestZipFileUtil#testGet()}
     * @since 4.3
     */
    public static InputStream get(final File zipFile,
                                 final String fileName,
                                 boolean ignoreCase) throws IOException {
        ArgCheck.isNotNull(zipFile);
        ArgCheck.isNotEmpty(fileName);   
        ByteArrayOutputStream out = null;
        try {
            
            out = new ByteArrayOutputStream();
            // Create temp zip file
            // Create buffer to use to write data to temp zip file
            final byte[] buf = new byte[BUFFER];
            // Copy specified zip file to temp zip file, skipping entry with specified file name
            final ZipFile zipFileZip = new ZipFile(zipFile);
            FileInputStream fis = null;
            ZipInputStream in = null;
            try {
                fis = new FileInputStream(zipFile);
                in = new ZipInputStream(fis);
                boolean match = false;
                for (ZipEntry entry = in.getNextEntry(); entry != null; entry = in.getNextEntry()) {
                    if (ignoreCase) {                        
                        if (entry.getName().equalsIgnoreCase(fileName)) {
                            match = true;
                        }
                    } else {
                        if (entry.getName().equals(fileName)) {
                            match = true;
                        }
                    }
                    
                    if (match) {
                        copyEntry(zipFileZip.getInputStream(entry), out, buf );
                        
                        byte[] data = out.toByteArray();
                        
                        out.close();
                        
                        return new ByteArrayInputStream(data);
                        
                    }
                }
            } finally {
                // Close input streams so we can later replace specified file with temp file
                zipFileZip.close();
                cleanup(in);
                cleanup(fis);
            }

        } finally {
            cleanup(out);

        }
        
        return null;
    }
    
    /**
     * Will find all entries in the jar based on the expression specified and based on
     * if case is ignored.
     * 
     * @param zipFile
     *            The zip file from which the find will be performed.
     * @param regexExpression
     *            The regex expression supported by {@link java.util.regex}
     * @param ignoreCase
     *            Indicates whether the case of an entry in the jar file should be ignored when finding the match.           
     * @return List of entry names that were found to match the regex expression
     * @throws IOException
     *             If an I/O error occurs reading the zip file.
     * @test {@link TestZipFileUtil#testFind()}
     * @test {@link TestZipFileUtil#testFindIgnoreCase()}
     * @test {@link TestZipFileUtil#testNotFind()}
     * @since 6.0
     */
    public static List<String> find(final File zipFile,
                                 final String regexExpression,
                                 boolean ignoreCase)  throws IOException {
        ArgCheck.isNotNull(zipFile);
        ArgCheck.isNotNull(regexExpression);

        Pattern pattern = null;
        if (ignoreCase) {
        	pattern = Pattern.compile(regexExpression, Pattern.CASE_INSENSITIVE);
        } else {
        	pattern = Pattern.compile(regexExpression);
        }
        List<String> finds = new ArrayList();
            final ZipFile zipFileZip = new ZipFile(zipFile);
            FileInputStream fis = null;
            ZipInputStream in = null;
            try {
                fis = new FileInputStream(zipFile);
                in = new ZipInputStream(fis);
                for (ZipEntry entry = in.getNextEntry(); entry != null; entry = in.getNextEntry()) {
                	Matcher matcher = pattern.matcher(entry.getName());
                	if (matcher.find()) {
                		finds.add(entry.getName());
                	}
                	
                }
            } finally {
                // Close input streams so we can later replace specified file with temp file
                zipFileZip.close();
                cleanup(in);
                cleanup(fis);
            }
         return finds;
    }    
    
    /**
     * Attempts to obtain the manifest file from the specified file, which
     * must be either a .jar or .zip file.
     *
     * @param path
     * @param VENDOR
     * @return Manifest, if one exist, other returns null
     */
    
    public static Manifest getManifest(File jarfile){
        JarFile jfile = null;
            String path = jarfile.getAbsolutePath();
            if(!jarfile.isDirectory()){
                if((path.indexOf(".jar")>0) || ((path.indexOf(".zip")>0))){ //$NON-NLS-1$ //$NON-NLS-2$
                    //This is a jar file so look for it's Manifest information
                    try{
                        jfile = new JarFile(jarfile);
                        Manifest manifest = jfile.getManifest();
                        if(manifest != null){
                            return manifest;
                        }
                    } catch(IOException io){
                        // ignore
                    }
                }
            }
        return null;
    }    
    

    /**
     * Writes the specified entry (including its contents) to the specified output stream.
     * 
     * @param entry
     *            The zip entry to be added.
     * @param in
     *            The inputstream from which the zip entry's contents wil be read.
     * @param out
     *            The zip output stream to which the specified zip entry will be written.
     * @param buffer
     *            The buffer used to transfer data from the specified zip entry to the specified zip output stream.
     * @throws IOException
     *             If an I/O error occurs writing to the specified zip output stream.
     * @since 4.3
     */
    private static void writeEntry(final ZipEntry entry,
                                   final InputStream in,
                                   final ZipOutputStream out,
                                   final byte[] buffer) throws IOException {
        // Add specified entry to stream
        out.putNextEntry(entry);
        // Write contents of entry to stream
        final BufferedInputStream zipEntryIn = new BufferedInputStream(in);
        for (int count = zipEntryIn.read(buffer); count >= 0; count = zipEntryIn.read(buffer)) {
            out.write(buffer, 0, count);
        }
        zipEntryIn.close();
    }
    
    private static void copyEntry(final InputStream in,
                                   final OutputStream out,
                                   final byte[] buffer) throws IOException {
        BufferedInputStream zipEntryIn = null;
        try {
            // Write contents of entry to stream
            zipEntryIn = new BufferedInputStream(in);
            for (int count = zipEntryIn.read(buffer); count >= 0; count = zipEntryIn.read(buffer)) {
                out.write(buffer, 0, count);
            }
        } finally {
            zipEntryIn.close();
        }

    }    
    
    
    private static void cleanup(InputStream stream) {
        if (stream != null) {
            try {
                stream.close();
            } catch (Exception e) {
            }
        }
    }
    private static void cleanup(OutputStream stream) {
        if (stream != null) {
            try {
                stream.close();
            } catch (Exception e) {
            }
        }
    }
    

    // ===========================================================================================================================
    // Constructors

    /**
     * Prevent initialization
     */
    private ZipFileUtil() {
    }
    
}
