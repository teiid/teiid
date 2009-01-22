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

package com.metamatrix.common.extensionmodule;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.extensionmodule.exception.DuplicateExtensionModuleException;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleNotFoundException;
import com.metamatrix.common.extensionmodule.exception.InvalidExtensionModuleTypeException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.ByteArrayHelper;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.FileUtils;

/**
 * <p>Standalone client for using {@link ExtensionModuleManager} to
 * install extension modules during installation.</p>
 *
 * <p>This class can initialize {@link ExtensionModuleManager} with properties
 * to override the resource properties gotten from
 * {@link com.metamatrix.common.config.CurrentConfiguration CurrentConfiguration}.
 * (This assumes the use of
 * {@link com.metamatrix.platform.extension.spi.jdbc.JDBCExtensionModuleTransactionFactory}.)
 *
 * </p>
 *
 * <p>The following property should be overriden, though it is available via
 * CurrentConfiguration.
 * {@link com.metamatrix.common.config.CurrentConfiguration#getResourceProperties getResourceProperties},
 * <ul>
 * <li>{@link ExtensionModulePropertyNames#MESSAGING_MESSAGE_BUS_CLASS} this resource
 * property is set in the database to
 * {@link com.metamatrix.common.messaging.VMMessageBus}, but to run in standalone
 * without a JMS server VMMessageBus will produce an error; use the classname of
 * {@link com.metamatrix.common.messaging.NoOpMessageBus} instead.</li>
 * </ul></p>
 *
 * <p>The following properties should be available via
 * CurrentConfiguration.
 * {@link com.metamatrix.common.config.CurrentConfiguration#getResourceProperties getResourceProperties},
 * but can be overriden with the Properties argument to this class:
 * <ul>
 * <li>{@link ExtensionModulePropertyNames#CONNECTION_FACTORY}
 *  the managed connection factory class; most likely use
 *  "com.metamatrix.platform.extension.spi.jdbc.JDBCExtensionModuleTransactionFactory"</li>
 * <li>{@link ExtensionModulePropertyNames#CONNECTION_DATABASE} the JDBC connection database</li>
 * <li>{@link ExtensionModulePropertyNames#CONNECTION_DRIVER} the JDBC connection driver full classname</li>
 * <li>{@link ExtensionModulePropertyNames#CONNECTION_USERNAME} the JDBC connection username</li>
 * <li>{@link ExtensionModulePropertyNames#CONNECTION_PASSWORD} the JDBC connection password</li>
 * <li>{@link ExtensionModulePropertyNames#CONNECTION_PROTOCOL} the JDBC connection protocol</li>
 * </ul></p>
 *
 */
public final class ExtensionModuleInstallUtil {

    private static final String LOG_CONTEXT = LogCommonConstants.CTX_EXTENSION_SOURCE;

	ExtensionModuleManager manager = null;

	/**
	 * Instantiates this class with any Properties that need to override
	 * Properties from CurrentConfiguration.
	 * @param overrideResourceProps Properties that will override any
	 * properties gotten from CurrentConfiguration by
	 * ExtensionModuleManager.  Can be null or empty.
	 * See javadoc for this class.
	 */
	public ExtensionModuleInstallUtil(Properties overrideResourceProps){
        Properties props = overrideResourceProps;
        if (props == null){
            props = new Properties();
        }

		manager = ExtensionModuleManager.getInstance(props);

	}

	/**
	 * Adds an extension module using {@link ExtensionModuleManager#addSource}.
	 * A <code>null</code> description will be added, and the fileName parameter
	 * will be used as the name of the extension module.  The newly-added
	 * extension module will be enabled for search, by default.
	 * @param fileName Name of the file from which the extension module will be
	 * loaded
	 * @param parentPath The directory containing the file named fileName
	 * @param type The type of the extension module; see {@link ExtensionModulePropertyNames}
	 * @param principal Name of principal requesting this addition
     * @return ExtensionModuleDescriptor describing the newly-added
     * extension module
     * @throws DuplicateExtensionModuleException if an extension module
     * with the same sourceName already exists
     * @throws InvalidExtensionTypeException if the indicated type is not one
     * of the currently-supported extension module types
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws IllegalArguementException if any required args are null (args are
     * required unless otherwise noted)
	 */
	public ExtensionModuleDescriptor installExtensionModule(String fileName, String parentPath, String type, String principal)
    throws DuplicateExtensionModuleException, InvalidExtensionModuleTypeException, MetaMatrixComponentException{
		return installExtensionModule(fileName, parentPath, type, principal, null, fileName);
	}

	/**
	 * Adds an extension module using {@link ExtensionModuleManager#addSource}.
	 * The newly-added
	 * extension module will be enabled for search, by default.
	 * @param fileName Name of the file from which the extension module will be
	 * loaded
	 * @param parentPath The directory containing the file named fileName
	 * @param type The type of the extension module; see {@link ExtensionModulePropertyNames}
	 * @param principal Name of principal requesting this addition
	 * @param description optional (may be null) description of the extension module
	 * @param alternateName optional (may be null) alternate name for the
	 * extension module to be created.  If this is null, fileName will be used
	 * as the name.
     * @return ExtensionModuleDescriptor describing the newly-added
     * extension module
     * @throws DuplicateExtensionModuleException if an extension module
     * with the same sourceName already exists
     * @throws InvalidExtensionTypeException if the indicated type is not one
     * of the currently-supported extension module types
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws IllegalArgumentException  if any required args are null (args are
     * required unless otherwise noted)
	 */
	public ExtensionModuleDescriptor installExtensionModule(String fileName, String parentPath, String type, String principal, String description, String alternateName)
    throws DuplicateExtensionModuleException, InvalidExtensionModuleTypeException, MetaMatrixComponentException{
        ArgCheck.isNotNull(fileName);
        ArgCheck.isNotNull(parentPath);
        ArgCheck.isNotNull(principal);
        ArgCheck.isNotNull(type);
        ArgCheck.isNotZeroLength(type);
       
        ArgCheck.isNotZeroLength(principal);
        ArgCheck.isNotZeroLength(fileName);
        ArgCheck.isNotZeroLength(parentPath);
        

        if (alternateName == null){
            alternateName = fileName;
        }
        
        InputStream stream=null;
        byte[] data = null;

        ExtensionModuleDescriptor esd = null;
        try{

            File aFile = new File(parentPath, fileName);

            stream= new FileInputStream(aFile);
            int size = (int)aFile.length();

            data = ByteArrayHelper.toByteArray(stream, size+1);

        	if (manager.isSourceInUse(alternateName)) {
		    	esd = manager.setSource(principal, alternateName, data);
        	} else {


				esd = manager.addSource(principal, type, alternateName, data, description, true);
        	}
        } catch (FileNotFoundException e) {
			throw new MetaMatrixComponentException(e);
		} catch (IOException e) {
			throw new MetaMatrixComponentException(e);
		} catch (ExtensionModuleNotFoundException e) {
			throw new MetaMatrixComponentException(e);
		} finally {
            try{
                if (stream != null){
                    stream.close();
                }
            } catch (IOException e){
				LogManager.logWarning(LOG_CONTEXT, e, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0003));
            }
        }

        return esd;
	}

	/**
	 * Adds an extension module using {@link ExtensionModuleManager#addSource}.
	 * The newly-added
	 * extension module will be enabled for search, by default.
	 * @param fileFullPath is the full path location of the file from which the extension module will be
	 * loaded
	 * @param type The type of the extension module; see {@link ExtensionModulePropertyNames}
	 * @param principal Name of principal requesting this addition
	 * @param description optional (may be null) description of the extension module
	 * @param assignedName is the assigned name for the
	 * extension module to be created.
     * @return ExtensionModuleDescriptor describing the newly-added
     * extension module
     * @throws DuplicateExtensionModuleException if an extension module
     * with the same sourceName already exists
     * @throws InvalidExtensionTypeException if the indicated type is not one
     * of the currently-supported extension module types
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws IllegalArgumentException  if any required args are null (args are
     * required unless otherwise noted)
	 */
	public ExtensionModuleDescriptor installExtensionModule(String fileFullPath, String type, String principal, String description, String assignedName)
    throws DuplicateExtensionModuleException, InvalidExtensionModuleTypeException, MetaMatrixComponentException{
        ArgCheck.isNotNull(fileFullPath);
        ArgCheck.isNotNull(type);
        ArgCheck.isNotNull(principal);
        ArgCheck.isNotNull(assignedName);
        ArgCheck.isNotNull(description);

        ArgCheck.isNotZeroLength(fileFullPath);
        ArgCheck.isNotZeroLength(type);
        ArgCheck.isNotZeroLength(principal);
        ArgCheck.isNotZeroLength(assignedName);
        ArgCheck.isNotZeroLength(description);




        InputStream stream=null;
        byte[] data = null;
        ExtensionModuleDescriptor esd = null;

       try{

            File aFile = new File(fileFullPath);

            stream= new FileInputStream(aFile);
            int size = (int)aFile.length();

            data = ByteArrayHelper.toByteArray(stream, size+1);

	        if (manager.isSourceInUse(assignedName)) {
	        	esd = manager.setSource(principal, assignedName, data);
	        } else {

				esd = manager.addSource(principal, type, assignedName, data, description, true);
	        }

        } catch (FileNotFoundException e) {
			throw new MetaMatrixComponentException(e);
		} catch (IOException e) {
			throw new MetaMatrixComponentException(e);
		} catch (ExtensionModuleNotFoundException e) {
			throw new MetaMatrixComponentException(e);
		} finally {
            try{
                if (stream != null){
                    stream.close();
                }
            } catch (IOException e){
				LogManager.logWarning(LOG_CONTEXT, e, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0003));
            }
        }
		return esd;
	}
    
    /**
     * UPdates an existing extension module using {@link ExtensionModuleManager#setSource}.
     * @param fileFullPath is the full path location of the file from which the extension module will be
     * loaded
     * @param assignedName is the assigned name for the
     * extension module to be created.
     * @param principal Name of principal requesting this addition
     * @throws ExtensionModuleNotFoundException if an extension module is not found using
     * the indicated sourceName
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws IllegalArgumentException  if any required args are null (args are
     * required unless otherwise noted)
     */
    
    public void updateExtensionModule(String fileFullPath, String sourceName, String principal)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        ArgCheck.isNotNull(fileFullPath);
        ArgCheck.isNotNull(principal);
        ArgCheck.isNotNull(sourceName);

        ArgCheck.isNotZeroLength(fileFullPath);
        ArgCheck.isNotZeroLength(principal);
        ArgCheck.isNotZeroLength(sourceName);

        InputStream stream=null;
        byte[] data = null;

       try{

            File aFile = new File(fileFullPath);

            data = ByteArrayHelper.toByteArray(aFile);
            
            if (manager.isSourceInUse(sourceName)) {
                manager.setSource(principal, sourceName, data);
             } else {
                throw new ExtensionModuleNotFoundException(ErrorMessageKeys.EXTENSION_0004, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0004, sourceName));

            }

       	} catch (FileNotFoundException e) {
			throw new MetaMatrixComponentException(e);
		} catch (IOException e) {
			throw new MetaMatrixComponentException(e);
		} catch (ExtensionModuleNotFoundException e) {
			throw new MetaMatrixComponentException(e);
		} finally {
            try{
                if (stream != null){
                    stream.close();
                }
            } catch (IOException e){
                LogManager.logWarning(LOG_CONTEXT, e, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0003));
            }
        }

    }
    

	/**
	 * Exports an extension module to the specified output stream
	 * @param outputStream is the output stream to write the module to
	 * @param sourceName is the name of the extension model to export
     * @return ExtensionModuleDescriptor describing the newly-added
     * extension module
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws IllegalArguementException if any required args are null (args are
     * required unless otherwise noted)
	 */
	public ExtensionModuleDescriptor exportExtensionModule(OutputStream outputStream, String sourceName)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{

        ExtensionModuleDescriptor source = null;
        source = manager.getSourceDescriptor(sourceName);

    	byte[] data = manager.getSource(sourceName);

		if (data == null || data.length == 0) {
			throw new ExtensionModuleNotFoundException(ErrorMessageKeys.EXTENSION_0004, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0004, sourceName));
		}

        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        InputStream is = new BufferedInputStream(bais);

		try {
	          byte[] buff = new byte[2048];
	          int bytesRead;

	          // Simple read/write loop.
	          while(-1 != (bytesRead = is.read(buff, 0, buff.length))) {
	              outputStream.write(buff, 0, bytesRead);
	          }

	         outputStream.flush();
	         outputStream.close();
		} catch(Exception e) {
			throw new MetaMatrixComponentException(e, ErrorMessageKeys.EXTENSION_0005, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0005, sourceName));
		}

		return source;
	}

	/**
	 * Exports an extension module to the specified output stream
	 * @param outputFileName is the output file to write the module to
	 * @param sourceName is the name of the extension model to export
     * @return ExtensionModuleDescriptor describing the newly-added
     * extension module
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws IllegalArguementException if any required args are null (args are
     * required unless otherwise noted)
	 */
	public ExtensionModuleDescriptor exportExtensionModule(String outputFileName, String sourceName)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        ExtensionModuleDescriptor source = null;
        source = manager.getSourceDescriptor(sourceName);
    	byte[] data = manager.getSource(sourceName);

		if (data == null || data.length == 0) {
			throw new ExtensionModuleNotFoundException(ErrorMessageKeys.EXTENSION_0004, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0004, sourceName));
		}

		try {
			FileUtils.write(data, outputFileName);
		} catch(Exception e) {
			throw new MetaMatrixComponentException(e, ErrorMessageKeys.EXTENSION_0006, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0006, sourceName, outputFileName));
		}

		return source;
	}
    
    /**
     * Exports all extension modules to the specified output directory
     * @param type is the type of extension modules to export 
     * @param outputDirectory is the output diretory to write the modules to
     * @param descriptorFileName is the name of the xml file that will be written that describes each extension module
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws IllegalArguementException if any required args are null (args are
     * required unless otherwise noted)
     */
    public List exportExtensionModulesOfType(String type, String outputDirectory)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{

        ExtensionModuleDescriptor source = null;
        String sourceName = "";//$NON-NLS-1$
        String outputfile = "";//$NON-NLS-1$
        try {
            
            List sds = manager.getSourceDescriptors(type);
            if (sds == null) {
                return Collections.EMPTY_LIST;
            }
            Iterator it = sds.iterator();
            while(it.hasNext()) {
                source = (ExtensionModuleDescriptor) it.next();
                sourceName = source.getName();
                
                outputfile = FileUtils.buildDirectoryPath(new String[] { (outputDirectory!=null?outputDirectory:""), sourceName});//$NON-NLS-1$

              
                exportExtensionModule(outputfile, source.getName());
            }
            
            return sds;

        } catch (ExtensionModuleNotFoundException notFound) {
            throw notFound;  
        } catch (MetaMatrixComponentException compException) {
             throw compException;              
        } catch(Exception e) {
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.EXTENSION_0006, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0006, sourceName, outputfile));
        } 

    }  

	/**
	 * Adds an extension module using {@link ExtensionModuleManager#addSource}.
	 * The newly-added
	 * extension module will be enabled for search, by default.
	 * @param fileFullPath is the full path location of the file from which the extension module will be
	 * loaded
	 * @param type The type of the extension module; see {@link ExtensionModulePropertyNames}
	 * @param principal Name of principal requesting this addition
	 * @param description optional (may be null) description of the extension module
	 * @param assignedName is the assigned name for the
	 * extension module to be created.
     * @return ExtensionModuleDescriptor describing the newly-added
     * extension module
     * @throws DuplicateExtensionModuleException if an extension module
     * with the same sourceName already exists
     * @throws InvalidExtensionTypeException if the indicated type is not one
     * of the currently-supported extension module types
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws IllegalArgumentException  if any required args are null (args are
     * required unless otherwise noted)
	 */
	public void removeExtensionModule(String extensionName,String principal)
    throws ExtensionModuleNotFoundException, InvalidExtensionModuleTypeException, MetaMatrixComponentException{
        ArgCheck.isNotNull(extensionName);

        ArgCheck.isNotZeroLength(extensionName);

       try{

	        if (manager.isSourceInUse(extensionName)) {
	        	manager.removeSource(principal, extensionName);

	        } else {
 				throw new ExtensionModuleNotFoundException(ErrorMessageKeys.EXTENSION_0004, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0004, extensionName));
	        }

        } catch (ExtensionModuleNotFoundException ne){
        	throw ne;

        } catch (Exception e){
			throw new MetaMatrixComponentException(e, ErrorMessageKeys.EXTENSION_0071, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0071, extensionName));

        }

	}





}
