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

package org.teiid.metadata.index;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

import org.teiid.metadata.RuntimeMetadataPlugin;


import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.internal.core.xml.xmi.XMIHeader;
import com.metamatrix.internal.core.xml.xmi.XMIHeaderReader;

public class ModelFileUtil {
	
	public interface XmiHeaderCache {
		XMIHeader getCachedXmiHeader(File resource);
		void setXmiHeaderToCache(File resource, XMIHeader header);
	}
	
	public static final String MANIFEST_MODEL_NAME = "MetaMatrix-VdbManifestModel.xmi"; //$NON-NLS-1$
	public static final String DOT_PROJECT = ".project"; //$NON-NLS-1$
	public static final String FILE_COLON = "file:";  //$NON-NLS-1$
	public static final String EXTENSION_XML = "xml"; //$NON-NLS-1$
	public static final String EXTENSION_XMI = "xmi"; //$NON-NLS-1$
	public static final String EXTENSION_XSD = "xsd"; //$NON-NLS-1$
	public static final String EXTENSION_VDB = "vdb"; //$NON-NLS-1$
	public static final String EXTENSION_ECORE = "ecore"; //$NON-NLS-1$
	public static final String EXTENSION_WSDL = "wsdl"; //$NON-NLS-1$
	    
    private static XmiHeaderCache CACHE;

	public static void setCache(XmiHeaderCache cache) {
		ModelFileUtil.CACHE = cache;
	}

	/**
	 * Return true if the File represents a MetaMatrix model file or an xsd file 
	 * this method does not check if the file exists in a project with
	 * model nature.  Returns a false for vdb files.
	 * @param resource The file that may be a model file
	 * @return true if it is a ModelFile.
	 */
	public static boolean isModelFile( final File resource ) {
	    if (resource == null) {
	        return false;
	    }
	    
	    // If the file does not yet exist then the only thing
	    // we can do is to check the name and extension.  
	    if (!resource.exists()) {
	        final String extension = FileUtils.getExtension(resource.getAbsolutePath());
	        return isModelFileExtension(extension, true);
	    }
	
	    //If this is an xsd resource return true
	    if(isXsdFile(resource) ){
	        return true;
	    }
	
	    //If this is an vdb resource return false
	    if(isVdbArchiveFile(resource) ){
	        return false;
	    }
	    
	    //If the resource does not have the correct lower-case extension then return false
	    if ( !EXTENSION_XMI.equals(getFileExtension(resource)) ) {
	        return false;
	    }
	
	    XMIHeader header = getXmiHeader(resource);
	    // If the header is not null then we know the file is, at least,
	    // a well formed xml document.  
	    if (header != null) {
	        // If the XMI version for the header is not null, then return
	        // false if the file represents an older 1.X model file
	        if (header.getXmiVersion() != null && header.getXmiVersion().startsWith("1.")) { //$NON-NLS-1$
	            return false;
	        }
	        // If the UUID for the header is not null, then the file is a 
	        // MetaMatrix model file containing a ModelAnnotation element.
	        if (header.getUUID() != null) {
	            return true;
	        }
	    }
	
	    return false;
	}
    
    /**
     * Return true if the IResource represents a xsd file.
     * @param resource The file that may be a xsd file
     * @return true if it is a xsd
     */
    public static boolean isXsdFile( final File resource ) {
        // Check that the resource has the correct lower-case extension
        if ( EXTENSION_XSD.equals(getFileExtension(resource)) ) {
            return true;
        }
        return false;
    }
    /**<p>
     * </p>
     * @since 4.0
     */
    public static boolean isVdbArchiveFile( final File resource ) {
        // Check that the resource has the correct lower-case extension
        if ( EXTENSION_VDB.equals(getFileExtension(resource)) ) {
            return true;
        }
        return false;
    }
    
    /**
     * Returns the file extension portion of this file, or an empty string if there is none.
     * <p>
     * The file extension portion is defined as the string
     * following the last period (".") character in the file name.
     * If there is no period in the file name, the file has no
     * file extension portion. If the name ends in a period,
     * the file extension portion is the empty string.
     * </p>
     * @param resource
     * @return the file extension or <code>null</code>
     * @since 4.3
     */
    public static String getFileExtension( final File resource ) {
        if ( resource != null ) {
            return FileUtils.getExtension(resource);
        }
        return ""; //$NON-NLS-1$
    }
    
    /**
     * Return true if a file with the specified name and extension 
     * represents a MetaMatrix model file.
     * @param name
     * @param extension
     * @return
     */
    public static boolean isModelFileExtension(final String extension, boolean caseSensitive) {
        // Check if the extension is one of the well-known extensions
        // The method assumes the extension is lower-case.  Relaxing
        // this assumption may cause the Modeler to work incorrectly.
        if (extension == null) {
            return false;
        }
        
        final String exten = (caseSensitive ? extension : extension.toLowerCase());
        if (  EXTENSION_XMI.equals(exten) ) {
            return true;
        }
        if ( EXTENSION_XSD.equals(exten) ) {
            return true;
        }
        if ( EXTENSION_VDB.equals(exten) ) {
            return false;
        }
        return false;
    }
    
    /**
     * Return the XMIHeader for the specified File or null
     * if the file does not represent a MetaMatrix model file.
     * @param resource The file of a metamatrix model file.
     * @return The XMIHeader for the model file
     */
    public static XMIHeader getXmiHeader( final File resource ) {
        if (resource != null && resource.isFile() && resource.exists() && resource.canRead() ) {
            //check cache
            if(CACHE != null) {
                XMIHeader header = CACHE.getCachedXmiHeader(resource);
                if(header != null) {
                    return header;
                }
            }
            
            if(isVdbArchiveFile(resource)) {
                return getXmiHeaderForVdbArchive(resource);
            }
            try {
                XMIHeader header = XMIHeaderReader.readHeader(resource);
                //add to cache
                if(CACHE != null) {
                    CACHE.setXmiHeaderToCache(resource, header);
                }
                return header;
            } catch (MetaMatrixCoreException e) {
            	LogManager.logWarning(RuntimeMetadataPlugin.PLUGIN_ID, e, e.getMessage());
            } catch (IllegalArgumentException iae ) {
                // Swallowing this exception because we're doing all three checks that would produce it.
                // If this exception is caught, it's because the files really were closed/deleted in another thread and this
                // thread didn't know about it.
                // Fixes Defect 22117
            }
        }

        return null;
    }
    
    /**
     * Return the XMIHeader for the given vdb file or null
     * if the file does not represent a vdb.
     * @param vdbArchiveJar The file for the vdb.
     * @return The XMIHeader for the vdb manifest file
     */  
    public static XMIHeader getXmiHeaderForVdbArchive( final File vdbArchiveJar ) {
        if (isVdbArchiveFile(vdbArchiveJar)) {
            
            // vdb file is empty there is nothing to read
            if(vdbArchiveJar.length() == 0) {
                return null;
            }
            
            ZipFile zipFile     = null;
            XMIHeader header = null;
            InputStream manifestStream = null;
            try {
                zipFile = new ZipFile(vdbArchiveJar);
                manifestStream = getManifestModelContentsFromVdbArchive(zipFile);
                header = getXmiHeader(manifestStream);
            } catch (ZipException e) {
            	LogManager.logWarning(RuntimeMetadataPlugin.PLUGIN_ID, e, e.getMessage());
            } catch (IOException e) {
            	LogManager.logWarning(RuntimeMetadataPlugin.PLUGIN_ID, e, e.getMessage());
            } finally {
                if (manifestStream != null) {
                    try {
                        manifestStream.close();
                    } catch (IOException err) {
                        // do nothing
                    }
                }
                if (zipFile != null) {
                    try {
                        zipFile.close();
                    } catch (IOException e) {
                        // do nothing
                    }
                }
            }

            return header;
        }
        return null;
    }
    
    /**
     * Return a java.io.InputStream reference for the MetaMatrix-VdbManifestModel.xmi model file
     * contained within the Vdb archive.  If the specified file is not a Vdb archive
     * file or the archive does not contain a manifest model then null is returned.
     * @param zip the Vdb archive 
     * @return the inputstream for the manifest file entry
     */    
    public static InputStream getManifestModelContentsFromVdbArchive( final ZipFile zipFile ) {
        return ModelFileUtil.getFileContentsFromArchive(zipFile, MANIFEST_MODEL_NAME);
    }

    /**
     * Return a java.io.InputStream reference for the specified zip entry name
     * @param zip
     * @param zipEntryName the fully qualified name of the zip entry
     * @return the inputstream for the zipfile entry
     */    
    public static InputStream getFileContentsFromArchive( final ZipFile zipFile, final String zipEntryName ) {
        ArgCheck.isNotNull(zipFile);
        ArgCheck.isNotEmpty(zipEntryName);
        try {
            // Iterate over all entries in the zip file ...
            for(final Enumeration entries = zipFile.entries();entries.hasMoreElements();) {
                ZipEntry entry = (ZipEntry) entries.nextElement();
                if (entry == null) {
                    continue;
                }
                // If the specified zip entry is found ...
                if (entry.getName().equalsIgnoreCase(zipEntryName)) {
                    // return the contents of the entry
                    return zipFile.getInputStream(entry);
                }
            }
        } catch(IOException e) {
        	LogManager.logWarning(RuntimeMetadataPlugin.PLUGIN_ID, e, e.getMessage());
        }
        return null;
    }
    
    /**
     * Return the XMIHeader for the specified inputstream of a model file.
     * @param resourceStream The inputStream of a metamatrix model file.
     * @return The XMIHeader for the model file
     */
    public static XMIHeader getXmiHeader( final InputStream resourceStream ) {
        if (resourceStream != null) {
            try {
                return XMIHeaderReader.readHeader(resourceStream);
            } catch (MetaMatrixCoreException e) {
                LogManager.logWarning(RuntimeMetadataPlugin.PLUGIN_ID, e, e.getMessage());
            }
        }
        return null;
    }

}
