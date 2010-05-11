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

package org.teiid.core.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.teiid.core.CorePlugin;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;



public final class FileUtils {

    public interface Constants {
        char CURRENT_FOLDER_SYMBOL_CHAR    = '.';
        char DRIVE_SEPARATOR_CHAR          = ':';
        char FILE_EXTENSION_SEPARATOR_CHAR = '.';
        char FILE_NAME_WILDCARD_CHAR       = '*';
        
        String CURRENT_FOLDER_SYMBOL    = String.valueOf(CURRENT_FOLDER_SYMBOL_CHAR);
        String DRIVE_SEPARATOR          = String.valueOf(DRIVE_SEPARATOR_CHAR);
        String FILE_EXTENSION_SEPARATOR = String.valueOf(FILE_EXTENSION_SEPARATOR_CHAR);
        String FILE_NAME_WILDCARD       = String.valueOf(FILE_NAME_WILDCARD_CHAR);
        String PARENT_FOLDER_SYMBOL     = ".."; //$NON-NLS-1$
    }

    public static final char SEPARATOR = '/';
    
	public static int DEFAULT_BUFFER_SIZE = 2048;
	public static String TEMP_DIRECTORY;
    public static String LINE_SEPARATOR = System.getProperty("line.separator"); //$NON-NLS-1$
    public static char[] LINE_SEPARATOR_CHARS = LINE_SEPARATOR.toCharArray();
    
    public final static String JAVA_IO_TEMP_DIR="java.io.tmpdir";//$NON-NLS-1$
    public final static char[] SUFFIX_class = ".class".toCharArray(); //$NON-NLS-1$
    public final static char[] SUFFIX_CLASS = ".CLASS".toCharArray(); //$NON-NLS-1$
    public final static char[] SUFFIX_java = ".java".toCharArray(); //$NON-NLS-1$
    public final static char[] SUFFIX_JAVA = ".JAVA".toCharArray(); //$NON-NLS-1$
    public final static char[] SUFFIX_jar = ".jar".toCharArray(); //$NON-NLS-1$
    public final static char[] SUFFIX_JAR = ".JAR".toCharArray(); //$NON-NLS-1$
    public final static char[] SUFFIX_zip = ".zip".toCharArray(); //$NON-NLS-1$
    public final static char[] SUFFIX_ZIP = ".ZIP".toCharArray(); //$NON-NLS-1$
    
    
    private static final String TEMP_FILE = "delete.me"; //$NON-NLS-1$
    private static final String TEMP_FILE_RENAMED = "delete.me.old"; //$NON-NLS-1$
    
    
    
    static {
        String tempDirPath = System.getProperty(JAVA_IO_TEMP_DIR); 
        TEMP_DIRECTORY = (tempDirPath.endsWith(File.separator) ? tempDirPath : tempDirPath + File.separator);
    }

    private FileUtils() {}

    /**<p>
     * Convert the specified file name to end with the specified extension if it doesn't already end with an extension.
     * </p>
     * @param name
     *              The file name.
     * @param extension
     *              The extension to append to the file name.
     * @return The file name with an extension.
     * @since 4.0
     */
    public static String toFileNameWithExtension(final String name,
                                                 final String extension) {
        return toFileNameWithExtension(name, extension, false);
    }

    /**<p>
     * Convert the specified file name to end with the specified extension if it doesn't already end with an extension.  If force
     * is true, the specified extension will be appended to the name if the name doesn't end with that particular extension.
     * </p>
     * @param name
     *              The file name.
     * @param extension
     *              The extension to append to the file name.
     * @param force
     *              Indicates whether to force the specified extension as the extension of the file name.
     * @return The file name with an extension.
     * @since 4.0
     */
    public static String toFileNameWithExtension(final String name,
                                                 final String extension,
                                                 final boolean force) {
        if (name == null) {
            final String msg = CorePlugin.Util.getString("FileUtils.The_name_of_the_file_may_not_be_null"); //$NON-NLS-1$
            throw new IllegalArgumentException(msg);
        }
        if (extension == null) {
            final String msg = CorePlugin.Util.getString("FileUtils.The_file_extension_may_not_be_null"); //$NON-NLS-1$
            throw new IllegalArgumentException(msg);
        }
        if (name.endsWith(extension)) {
            return name;
        }
        if (!force  &&  name.indexOf(Constants.FILE_EXTENSION_SEPARATOR_CHAR) >= 0) {
            return name;
        }
        final int nameLen = name.length() - 1;
        final int extLen = extension.length();
        final boolean nameEndsWithExtChr = (nameLen >= 0  &&  name.charAt(nameLen) == Constants.FILE_EXTENSION_SEPARATOR_CHAR);
        final boolean extBeginsWithExtChr = (extLen > 0  &&  extension.charAt(0) == Constants.FILE_EXTENSION_SEPARATOR_CHAR);
        if (nameEndsWithExtChr  &&  extBeginsWithExtChr) {
            return name.substring(0, nameLen) + extension;
        }
        if (!nameEndsWithExtChr  &&  !extBeginsWithExtChr) {
            return name + Constants.FILE_EXTENSION_SEPARATOR + extension;
        }
        return name + extension;
    }

    /**
     * Determine whether the specified name is valid for a file or folder on the current file system.
     * @param newName the new name to be checked
     * @return true if the name is null or contains no invalid characters for a folder or file, or false otherwise
     */
    public static boolean isFilenameValid( String newName ) {
    	return true; //TODO: just catch an exception when the file is accessed or created
    }

    /**
     * Copy a file.  Overwrites the destination file if it exists. 
     * @param fromFileName
     * @param toFileName
     * @throws Exception
     * @since 4.3
     */
    public static void copy(String fromFileName, String toFileName) throws IOException {
		copy(fromFileName, toFileName, true);
    }
    
    /**
     * Copy a file 
     * @param fromFileName
     * @param toFileName
     * @param overwrite whether to overwrite the destination file if it exists.
     * @throws TeiidException
     * @since 4.3
     */
    public static void copy(String fromFileName, String toFileName, boolean overwrite) throws IOException {
        File toFile = new File(toFileName);
        
        if (toFile.exists()) {
            if (overwrite) {
                toFile.delete();
            } else {
                final String msg = CorePlugin.Util.getString("FileUtils.File_already_exists", toFileName); //$NON-NLS-1$            
                throw new IOException(msg);
            }
        }
        
        File fromFile = new File(fromFileName);
        if (!fromFile.exists()) {
            throw new FileNotFoundException(CorePlugin.Util.getString("FileUtils.File_does_not_exist._1", fromFileName)); //$NON-NLS-1$
        }
        
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(fromFile);
            write(fis, toFileName);    
        } finally {
            if (fis != null) {
                fis.close();
            }
        }
    }
    
    /**
     * Copy recursively the contents of <code>sourceDirectory</code> to 
     * the <code>targetDirectory</code>. Note that <code>sourceDirectory</code>
     * will <b>NOT</b> be copied.  If <code>targetDirectory</code> does not exist, it will be created.
     * @param sourceDirectory The source directory to copy
     * @param targetDirectory The target directory to copy to
     * @throws Exception If the source directory does not exist.
     * @since 4.3
     */
    public static void copyDirectoryContentsRecursively(File sourceDirectory, File targetDirectory) throws Exception {
        copyRecursively(sourceDirectory, targetDirectory, false);
    }
    
    /**
     * Copy file from orginating directory to the destination directory.
     * 
     * @param orginDirectory
     * @param destDirectory
     * @param fileName
     * @throws Exception
     * @since 4.4
     */
    public static void copyFile(String orginDirectory,
                          String destDirectory,
                          String fileName) throws Exception {

        copyFile(orginDirectory, fileName, destDirectory, fileName);
    }

    /**
     * Copy file from orginating directory to the destination directory.
     * 
     * @param orginDirectory
     * @param orginFileName
     * @param destDirectory
     * @param destFileName
     * @throws Exception
     * @since 4.4
     */
    public static void copyFile(String orginDirectory,
                          String orginFileName,
                          String destDirectory,
                          String destFileName) throws Exception {

        FileUtils.copy(orginDirectory + File.separator + orginFileName, destDirectory + File.separator + destFileName);
    }    

    /**
     * Copy recursively the <code>sourceDirectory</code> and all its contents 
     * to the <code>targetDirectory</code>.  If <code>targetDirectory</code>
     * does not exist, it will be created.
     * @param sourceDirectory The source directory to copy
     * @param targetDirectory The target directory to copy to
     * @throws Exception If the source directory does not exist.
     * @since 4.3
     */
    public static void copyDirectoriesRecursively(File sourceDirectory, File targetDirectory) throws Exception {
        copyRecursively(sourceDirectory, targetDirectory, true);
    }

    /**
     * Copy recursively from the <code>sourceDirectory</code> all its contents 
     * to the <code>targetDirectory</code>.  if 
     * <code>includeSourceRoot<code> == <code>true</code>, copy <code>sourceDirectory</code>
     * itself, else only copy <code>sourceDirectory</code>'s contents.
     * If <code>targetDirectory</code> does not exist, it will be created.
     * @param sourceDirectory
     * @param targetDirectory
     * @throws FileNotFoundException
     * @throws Exception
     * @since 4.3
     */
    private static void copyRecursively(File sourceDirectory,
                                        File targetDirectory,
                                        boolean includeSourceRoot) throws FileNotFoundException,
                                                             Exception {
        if (!sourceDirectory.exists()) {
            throw new FileNotFoundException(CorePlugin.Util.getString("FileUtils.File_does_not_exist._1",sourceDirectory)); //$NON-NLS-1$
        }
        
        if (!sourceDirectory.isDirectory()) {
            throw new FileNotFoundException(CorePlugin.Util.getString("FileUtils.Not_a_directory",sourceDirectory)); //$NON-NLS-1$
        }
        
        File targetDir = new File(targetDirectory.getAbsolutePath() + File.separatorChar + sourceDirectory.getName());
        if (includeSourceRoot) {
            // copy source directory
            targetDir.mkdir();
        } else {
            // copy only source directory contents
            targetDir = new File(targetDirectory.getAbsolutePath() + File.separatorChar);
        }
        File[] sourceFiles = sourceDirectory.listFiles();
        for (int i = 0; i < sourceFiles.length; i++) {
            File srcFile = sourceFiles[i];
            if (srcFile.isDirectory()) {
                File childTargetDir = new File(targetDir.getAbsolutePath());
                copyRecursively(srcFile, childTargetDir, true);
            } else {
                copy(srcFile.getAbsolutePath(), targetDir.getAbsolutePath() + File.separatorChar + srcFile.getName());
            }
        }
    }
    
   /**
    *  Write an InputStream to a file.
    */
    public static void write(InputStream is, String fileName) throws IOException {
        File f = new File(fileName);
        write(is,f);
    }

   /**
    *  Write an InputStream to a file.
    */
    public static void write(InputStream is, File f) throws IOException {
		write(is, f, DEFAULT_BUFFER_SIZE);    	
    }

	/**
	 *  Write an InputStream to a file.
	 */
	 public static void write(InputStream is, File f, int bufferSize) throws IOException {
		f.delete();
		final File parentDir = f.getParentFile();
		if (parentDir !=null) {
			parentDir.mkdirs();
		}

		FileOutputStream fio = null;
		BufferedOutputStream bos = null;
        try {
            fio = new FileOutputStream(f);
            bos = new BufferedOutputStream(fio);
            if (bufferSize > 0) {
        		byte[] buff = new byte[bufferSize];
        		int bytesRead;
        
        		// Simple read/write loop.
        		while(-1 != (bytesRead = is.read(buff, 0, buff.length))) {
        			bos.write(buff, 0, bytesRead);
        		}
            }
            bos.flush();
        } finally {
            if (bos != null) {
                bos.close();
            }
            if (fio != null) {
                fio.close();
            }
        }
	 }
     
     
     /**
      *  Write an File to an OutputStream
      *  Note: this will not close the outputStream;
      */
        public static void write(File f, OutputStream outputStream) throws IOException {
            write(f, outputStream, DEFAULT_BUFFER_SIZE);      
        }
        
        /**
         *  Write an File to an OutputStream
         *  Note: this will not close the outputStream;
         */
         public static void write(File f, OutputStream outputStream, int bufferSize) throws IOException {
            FileInputStream fis = null;
            try {
                fis = new FileInputStream(f);
                write(fis, outputStream, bufferSize);
            } finally {
                if (fis != null) {
                    fis.close();
                }
            }
         }     
         
         /**
          * Write the given input stream to outputstream
          * Note: this will not close in/out streams 
          */
         public static void write(InputStream fis, OutputStream outputStream, int bufferSize) throws IOException {             
             byte[] buff = new byte[bufferSize];
             int bytesRead;
 
             // Simple read/write loop.
             while(-1 != (bytesRead = fis.read(buff, 0, buff.length))) {
                 outputStream.write(buff, 0, bytesRead);
             } 
             outputStream.flush();
         }           

   /**
    *  Write a byte array to a file.
    */
  	public static void write(byte[] data, String fileName) throws IOException {
        ByteArrayInputStream bais = null;
        InputStream is = null;
        try {
            bais = new ByteArrayInputStream(data);
            is = new BufferedInputStream(bais);
    
            write(is, fileName);  
        } finally {
            if (is != null) {
                is.close();
            }
            if (bais != null) {
                bais.close();
            }
        }
  	}

	/**
	 *  Write a byte array to a file.
	 */
	 public static void write(byte[] data, File file) throws IOException {
         ByteArrayInputStream bais = null;
         InputStream is = null;
         try {
    		 bais = new ByteArrayInputStream(data);
    		 is = new BufferedInputStream(bais);
    
    		 write(is, file);  
         } finally {
             if (is != null) {
                 is.close();
             }
             if (bais != null) {
                 bais.close();
             }
         }
	 }

    /**
     * Returns a <code>File</code> array that will contain all the files that 
     * exist in the specified directory or any nested directories
     * @return File[] of files in the directory
     */
    public static File[] findAllFilesInDirectoryRecursively(final String dir) {

        // Recursively navigate through the contents of this directory
        // gathering up all the files
        List allFiles = new ArrayList();
        File directory = new File(dir);
        addFilesInDirectoryRecursively(directory, allFiles);

        return (File[])allFiles.toArray(new File[allFiles.size()]);

    }
    
    private static void addFilesInDirectoryRecursively(final File directory, final List allFiles) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (int i=0; i < files.length; i++) {
                File file = files[i];
                if (file.isDirectory()) {
                    addFilesInDirectoryRecursively(file, allFiles);
                } else {
                    allFiles.add(file);
                }
            }
        }        
    }

	/**
     * Returns a <code>File</code> array that will contain all the files that exist in the directory
     * 
     * @return File[] of files in the directory
     */
    public static File[] findAllFilesInDirectory(String dir) {

        // Find all files in the specified directory
        File modelsDirFile = new File(dir);
        FileFilter fileFilter = new FileFilter() {

            public boolean accept(File file) {
                if (file.isDirectory()) {
                    return false;
                }

                String fileName = file.getName();

                if (fileName == null || fileName.length() == 0) {
                    return false;
                }

                return true;

            }
        };

        File[] modelFiles = modelsDirFile.listFiles(fileFilter);

        return modelFiles;

    }

      /**
       * Returns a <code>File</code> array that will contain all the files that
       * exist in the directory that have the specified extension.
       * @return File[] of files having a certain extension
       */
      public static File[] findAllFilesInDirectoryHavingExtension(String dir, final String extension) {

        // Find all files in that directory that end in XML and attempt to
        // load them into the runtime metadata database.
        File modelsDirFile = new File(dir);
        FileFilter fileFilter = new FileFilter() {
            public boolean accept(File file) {
                if(file.isDirectory()) {
                    return false;
                }


                String fileName = file.getName();

                if (fileName==null || fileName.length()==0) {
                    return false;
                }

                // here we check to see if the file is an .xml file...
                int index = fileName.lastIndexOf("."); //$NON-NLS-1$

                if (index<0 || index==fileName.length()) {
                    return false;
                }

                if (fileName.substring(index, fileName.length()).equalsIgnoreCase(extension)) {
                    return true;
                }
                return false;
            }
        };

        File[] modelFiles = modelsDirFile.listFiles(fileFilter);

        return modelFiles;

    }

    /**
     * @param string
     * @return
     */
    public static String getFilenameWithoutExtension( final String filename ) {
        if ( filename == null || filename.length() == 0 ) {
            return filename;
        }
        final int extensionIndex = filename.lastIndexOf(Constants.FILE_EXTENSION_SEPARATOR_CHAR);
        if ( extensionIndex == -1 ) {
            return filename;    // not found
        }
        if ( extensionIndex == 0 ) {
            return ""; //$NON-NLS-1$
        }
        return filename.substring(0,extensionIndex);
    }
    
    public static String getBaseFileNameWithoutExtension(String path) {
    	return StringUtil.getFirstToken(StringUtil.getLastToken(path, "/"), "."); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /**
     * Obtains the file extension of the specified <code>File</code>. The extension is considered to be all the
     * characters after the last occurrence of {@link Constants#FILE_EXTENSION_SEPARATOR_CHAR} in the pathname
     * of the input.
     * @param theFile the file whose extension is being requested
     * @return the extension or <code>null</code> if not found
     * @since 4.2
     */
    public static String getExtension(File theFile) {
        return getExtension(theFile.getPath());
    }
    
    /**
     * Obtains the file extension of the specified file name. The extension is considered to be all the
     * characters after the last occurrence of {@link Constants#FILE_EXTENSION_SEPARATOR_CHAR}.
     * @param theFileName the file whose extension is being requested
     * @return the extension or <code>null</code> if not found
     * @since 4.2
     */
    public static String getExtension(String theFileName) {
        String result = null;
        final int index = theFileName.lastIndexOf(Constants.FILE_EXTENSION_SEPARATOR_CHAR);
        
        // make sure extension char is found and is not the last char in the path
        if ((index != -1) && ((index + 1) != theFileName.length())) {
            result = theFileName.substring(index + 1);
        }
        
        return result;
    }
    
    /**
     * Returns true iff str.toLowerCase().endsWith(".jar") || str.toLowerCase().endsWith(".zip")
     * implementation is not creating extra strings.
     */
    public final static boolean isArchiveFileName(String name) {
        int nameLength = name == null ? 0 : name.length();
        int suffixLength = SUFFIX_JAR.length;
        if (nameLength < suffixLength) return false;

        // try to match as JAR file
        for (int i = 0; i < suffixLength; i++) {
            char c = name.charAt(nameLength - i - 1);
            int suffixIndex = suffixLength - i - 1;
            if (c != SUFFIX_jar[suffixIndex] && c != SUFFIX_JAR[suffixIndex]) {

                // try to match as ZIP file
                suffixLength = SUFFIX_ZIP.length;
                if (nameLength < suffixLength) return false;
                for (int j = 0; j < suffixLength; j++) {
                    c = name.charAt(nameLength - j - 1);
                    suffixIndex = suffixLength - j - 1;
                    if (c != SUFFIX_zip[suffixIndex] && c != SUFFIX_ZIP[suffixIndex]) return false;
                }
                return true;
            }
        }
        return true;        
    }  
     
    /**
     * Returns true iff str.toLowerCase().endsWith(".class")
     * implementation is not creating extra strings.
     */
    public final static boolean isClassFileName(String name) {
        int nameLength = name == null ? 0 : name.length();
        int suffixLength = SUFFIX_CLASS.length;
        if (nameLength < suffixLength) return false;

        for (int i = 0; i < suffixLength; i++) {
            char c = name.charAt(nameLength - i - 1);
            int suffixIndex = suffixLength - i - 1;
            if (c != SUFFIX_class[suffixIndex] && c != SUFFIX_CLASS[suffixIndex]) return false;
        }
        return true;        
    } 
      
    /**
     * Returns true iff str.toLowerCase().endsWith(".java")
     * implementation is not creating extra strings.
     */
    public final static boolean isJavaFileName(String name) {
        int nameLength = name == null ? 0 : name.length();
        int suffixLength = SUFFIX_JAVA.length;
        if (nameLength < suffixLength) return false;

        for (int i = 0; i < suffixLength; i++) {
            char c = name.charAt(nameLength - i - 1);
            int suffixIndex = suffixLength - i - 1;
            if (c != SUFFIX_java[suffixIndex] && c != SUFFIX_JAVA[suffixIndex]) return false;
        }
        return true;        
    }
    

    public static File convertByteArrayToFile(final byte[] contents, final String parentDirectoryPath, final String fileName) {
        if (contents != null) {
            FileOutputStream os = null;
            try {
                final File temp = new File(parentDirectoryPath,fileName);
                os = new FileOutputStream(temp);
                os.write(contents);
                return temp;
            } catch (Exception e) {
                throw new TeiidRuntimeException(e);
            } finally {
                if (os != null) {
                    try {
                        os.close();
                    } catch (IOException e1) {
                       // do nothing
                    }
                }
            }
        }
        return null;
    }
    
    public static void removeDirectoryAndChildren(File directory) {
        removeChildrenRecursively(directory);        
        if(!directory.delete()) {
            directory.deleteOnExit();
        }
    }

    public static void removeChildrenRecursively(File directory) {
        File[] files = directory.listFiles();
        if(files != null) {
            for(int i=0; i < files.length; i++) {
                File file = files[i];
                if (file.isDirectory()) {
                    removeDirectoryAndChildren(file);
                } else {
                    if(!file.delete()) {
                        file.deleteOnExit();
                    }   
                }
            }
        }        
    }

    /**
     * Builds a file directory path from a physical location and a location that needs
     * to be appended to the physical.  This is used so that the correct File.separator 
     * is used and that no additional separator is added when not needed. 
     * @param physicalDirectory
     * @param appendLocation
     * @return
     * @since 4.3
     */
    public static String buildDirectoryPath(String[] paths) {
//      String physicalDirectory, String appendLocation) {

        if (paths == null || paths.length == 0) {
            return ""; //$NON-NLS-1$           
        }
        if (paths.length == 1) {
            return (paths[0]!=null?paths[0]:"");//$NON-NLS-1$ 
        }
        int l = paths.length;
        StringBuffer sb = new StringBuffer();
        for (int cur=0;cur<l; cur++) {
            int next = cur+1;
            
            String value = paths[cur]!=null?paths[cur]:"";//$NON-NLS-1$ 
            if (value.equals("")) {//$NON-NLS-1$ 
                continue;
            }
            sb.append(value);
            
            if (next < l) {
                String nextValue = paths[next]!=null?paths[next]:"";//$NON-NLS-1$ 
                if (value.endsWith(File.separator)) {
                
                } else if (!nextValue.startsWith(File.separator)) {
                    sb.append(File.separator);
                }
            
            }
        }
        
        return sb.toString();
    }
    
    /**
     * Test whether it's possible to read and write files in the specified directory. 
     * @param dirPath Name of the directory to test
     * @throws TeiidException
     * @since 4.3
     */
    public static void testDirectoryPermissions(String dirPath) throws TeiidException {
        
        //try to create a file
        File tmpFile = new File(dirPath + File.separatorChar + TEMP_FILE);
        boolean success = false;
        try {
            success = tmpFile.createNewFile();
        } catch (IOException e) {
        }
        if (!success) {
            final String msg = CorePlugin.Util.getString("FileUtils.Unable_to_create_file_in", dirPath); //$NON-NLS-1$            
            throw new TeiidException(msg);
        }
        

        //test if file can be written to
        if (!tmpFile.canWrite()) {
            final String msg = CorePlugin.Util.getString("FileUtils.Unable_to_write_file_in", dirPath); //$NON-NLS-1$            
            throw new TeiidException(msg);
        }

        //test if file can be read
        if (!tmpFile.canRead()) {
            final String msg = CorePlugin.Util.getString("FileUtils.Unable_to_read_file_in", dirPath); //$NON-NLS-1$            
            throw new TeiidException(msg);
        }

        //test if file can be renamed
        File newFile = new File(dirPath + File.separatorChar + TEMP_FILE_RENAMED);
        success = false;
        try {
            success = tmpFile.renameTo(newFile);
        } catch (Exception e) {
        }
        if (!success) {
            final String msg = CorePlugin.Util.getString("FileUtils.Unable_to_rename_file_in", dirPath); //$NON-NLS-1$            
            throw new TeiidException(msg);
        }

        //test if file can be deleted
        success = false;
        try {
            success = newFile.delete();
        } catch (Exception e) {
        }
        if (!success) {
            final String msg = CorePlugin.Util.getString("FileUtils.Unable_to_delete_file_in", dirPath); //$NON-NLS-1$            
            throw new TeiidException(msg);
        }
    }

    /**
     * Rename a file. 
     * @param oldFilePath
     * @param newFilePath
     * @param overwrite If true, overwrite the old file if it exists.  If false, throw an exception if the old file exists.  
     * @throws TeiidException
     * @since 4.3
     */
    public static void rename(String oldFilePath, String newFilePath, boolean overwrite) throws IOException {
        File oldFile = new File(oldFilePath);
        File newFile = new File(newFilePath);

        if (newFile.exists()) {
            if (overwrite) {
                newFile.delete();
            } else {
                final String msg = CorePlugin.Util.getString("FileUtils.File_already_exists", newFilePath); //$NON-NLS-1$            
                throw new IOException(msg);
            }
        }

        boolean renamed = oldFile.renameTo(newFile);

        //Sometimes file.renameTo will silently fail, for example attempting to rename from different UNIX partitions.
        //Try to copy instead.
        if (!renamed) {
            copy(oldFilePath, newFilePath);
            oldFile.delete();
        }
    }
    
    
    
    

    public static void remove(String filePath) throws IOException {
        File file = new File(filePath);
        if (file.exists()) {
            file.delete();
        }  
    }
   
}