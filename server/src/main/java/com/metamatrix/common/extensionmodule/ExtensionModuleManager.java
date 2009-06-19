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

package com.metamatrix.common.extensionmodule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.connection.ManagedConnectionException;
import com.metamatrix.common.connection.TransactionMgr;
import com.metamatrix.common.extensionmodule.exception.DuplicateExtensionModuleException;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleNotFoundException;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleOrderingException;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleRuntimeException;
import com.metamatrix.common.extensionmodule.exception.InvalidExtensionModuleTypeException;
import com.metamatrix.common.extensionmodule.spi.ExtensionModuleTransaction;
import com.metamatrix.common.extensionmodule.spi.jdbc.JDBCExtensionModuleWriter;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.platform.admin.api.ExtensionSourceAdminAPI;
import com.metamatrix.server.ResourceFinder;

/**
 * <p>This class is the mechanism to manage the MetaMatrix extension modules.</p>
 *
 * <p>An "extension module" (formerly known as an "extension source")
 * is a utility file or other source of information.
 * Different {@link #getSourceTypes types}
 * will be supported: JAR files containing Java class files, an XML file of
 * user-defined function meta data, and others.  The current implementation
 * will be to store files in binary form centrally in the MetaMatrix platform.</p>
 *
 * <p>A module "type" is needed to distinguish JAR modules from other modules.
 * </p>
 *
 * <p>The CurrentConfiguration and Logging are assumed to be available by the
 * time an instance of this class is first initialized.</p>
 *
 * <p>This object can run standalone (which it needs to be during MetaMatrix
 * installation).  To do this, the
 * {@link com.metamatrix.common.config.bootstrap.SystemCurrentConfigBootstrap SystemCurrentConfigBootstrap}
 * needs to be used.  To do this, first set the required property (the first one
 * listed below).  Second, add all required properties to (or replace) the
 * system properties.  Third, use the {@link #getInstance} method as usual.
 * it is recommended that this be done in it's own process, since it won't work
 * if CurrentConfiguration has already been initialized with a different
 * bootstrap strategy.  Alternately, a different bootstrap strategy can
 * be used for CurrentConfiguration, as long as the properties listed
 * below are available to this object via CurrentConfiguration.getInstance().</p>
 *
 * <p>The following properties are required:
 * <ul>
 * <li>{@link ExtensionModulePropertyNames#CONNECTION_FACTORY}
 *  the managed connection factory class; most likely use
 *  "com.metamatrix.platform.extension.spi.jdbc.JDBCExtensionModuleTransactionFactory"</li>
 * </ul></p>
 *
 * <p>The following properties are required with use of JDBCExtensionModuleTransactionFactory:
 * <ul>
 * <li>{@link ExtensionModulePropertyNames#CONNECTION_DATABASE} the JDBC connection database</li>
 * <li>{@link ExtensionModulePropertyNames#CONNECTION_DRIVER} the JDBC connection driver full classname</li>
 * <li>{@link ExtensionModulePropertyNames#CONNECTION_USERNAME} the JDBC connection username</li>
 * <li>{@link ExtensionModulePropertyNames#CONNECTION_PASSWORD} the JDBC connection password</li>
 * <li>{@link ExtensionModulePropertyNames#CONNECTION_PROTOCOL} the JDBC connection protocol</li>
 * </ul></p>
 *
 * History:
 *  6/14/02 - Changed to use ResourcePooling instead of the ManagedConnectionPool
 */
public class ExtensionModuleManager {

    /**
     * The limit to the number of characters an extension module name
     * can be.
     */
    public static final int SOURCE_NAME_LENGTH_LIMIT = ExtensionSourceAdminAPI.SOURCE_NAME_LENGTH_LIMIT;

    /**
     * The limit to the number of characters an extension module description
     * can be.
     */
    public static final int SOURCE_DESCRIPTION_LENGTH_LIMIT = ExtensionSourceAdminAPI.SOURCE_DESCRIPTION_LENGTH_LIMIT;

    /**
     * The limit, in bytes, to the size of a single extension module.
     */
    public static final int SOURCE_CONTENTS_LENGTH_LIMIT = 1000000000;  //1Gb

    private static final String LOG_CONTEXT = LogConstants.CTX_EXTENSION_SOURCE;

  /**
    * The transaction mgr for ManagedConnections.
    */
    private TransactionMgr transMgr;

    /**
     * A reference (not a singleton) maintained by this class for convenience
     * of access
     */
    private static ExtensionModuleManager extensionModuleManager;

    //===================================================================
    //PUBLIC INTERFACE
    //===================================================================

    /**
     * <p>Return a cached ExtensionModuleManager instance for this
     * process, fully initialized and ready for use.  This is not
     * a singleton, it is merely cached for convenience</p>
     */
    public static synchronized ExtensionModuleManager getInstance(){
        if (extensionModuleManager == null){
			extensionModuleManager = new ExtensionModuleManager();
        }
        return extensionModuleManager;
    }

    static synchronized void reInit() {
    	extensionModuleManager = null;
    }

    //===================================================================
    //PUBLIC INTERFACE administrative
    //===================================================================

    /**
     * Adds an extension module to the end of the list of modules.
     * <i>All caches (of Class objects) are cleared.</i>
     * @param principalName name of principal requesting this addition
     * @param type one of the known types of extension file
     * @param sourceName name (e.g. filename) of extension module
     * @param source actual contents of module
     * @param description (optional) description of the extension module -
     * may be null
     * @param enabled indicates whether each extension module is enabled for
     * being searched or not (for convenience, a module can be disabled
     * without being removed)
     * @return ExtensionModuleDescriptor describing the newly-added
     * extension module
     * @throws DuplicateExtensionModuleException if an extension module
     * with the same sourceName already exists
     * @throws InvalidExtensionTypeException if the indicated type is not one
     * of the currently-supported extension module types
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws ExtensionModuleRuntimeException if this object wasn't initialized properly
     * @throws IllegalArgumentException if any required parameters are null or
     * invalid
     */
    public ExtensionModuleDescriptor addSource(String principalName, String type, String sourceName, byte[] source, String description, boolean enabled)
    throws DuplicateExtensionModuleException, InvalidExtensionModuleTypeException, MetaMatrixComponentException{
        ArgCheck.isNotNull(principalName);
        ArgCheck.isNotNull(type);
        ArgCheck.isNotNull(sourceName);
        ArgCheck.isNotNull(source);
        ArgCheck.isNotZeroLength(principalName);
        ArgCheck.isNotZeroLength(type);
        ArgCheck.isNotZeroLength(sourceName);
        ArgCheck.isTrue(sourceName.length() <= SOURCE_NAME_LENGTH_LIMIT, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0009, SOURCE_NAME_LENGTH_LIMIT));

// changed to truncate,instead of throw an error because now VDB descriptions are being
// passed and they could be longer
//        ArgCheck.isTrue(description.length() <= SOURCE_DESCRIPTION_LENGTH_LIMIT, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0010, SOURCE_DESCRIPTION_LENGTH_LIMIT));
        ArgCheck.isTrue(source.length <= SOURCE_CONTENTS_LENGTH_LIMIT, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0011, SOURCE_CONTENTS_LENGTH_LIMIT));
        ExtensionModuleTypes.checkTypeIsValid(type);
        
        description = adjustLengthToFit(description);        

        LogManager.logDetail(LOG_CONTEXT, new Object[] {"Adding module", sourceName,"of type",type,"for principal",principalName} ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$


        ExtensionModuleDescriptor result = null;
        ExtensionModuleTransaction transaction = null;
        try {
            transaction = getWriteTransaction();
            result = transaction.addSource(principalName, type, sourceName, source, getChecksum(source), description, enabled);
            transaction.commit();
            
            notifyFileChanged();
        } catch (ManagedConnectionException e) {
			throw new MetaMatrixComponentException(e);
		} finally {
            if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    LogManager.logWarning(LOG_CONTEXT, txne, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0014));
                }
                transaction = null;
            }
        }
        return result;
    }


    private String adjustLengthToFit(String oriString){
        if(oriString != null && oriString.length() > SOURCE_DESCRIPTION_LENGTH_LIMIT){
            oriString = oriString.substring(0, SOURCE_DESCRIPTION_LENGTH_LIMIT);
        }
        return oriString;
    }
    /**
     * Deletes a module from the list of modules.  <i>All caches (of Class
     * objects) are cleared.</i>
     * @param principalName name of principal requesting this addition
     * @param sourceName name (e.g. filename) of extension module
     * @throws ExtensionModuleNotFoundException if no extension module with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws ExtensionModuleRuntimeException if this object wasn't initialized properly
     * @throws IllegalArgumentException if any required parameters are null
     */
    public void removeSource(String principalName, String sourceName)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        ArgCheck.isNotNull(principalName);
        ArgCheck.isNotNull(sourceName);
        ArgCheck.isNotZeroLength(principalName);
        ArgCheck.isNotZeroLength(sourceName);

        LogManager.logTrace(LOG_CONTEXT, new Object[] {"Removing module", sourceName, "for principal", principalName} ); //$NON-NLS-1$ //$NON-NLS-2$
        ExtensionModuleTransaction transaction = null;
        try {
            transaction = getWriteTransaction();
            transaction.removeSource(principalName, sourceName);
            transaction.commit();
            
            notifyFileChanged();
        } catch ( ManagedConnectionException e ) {
            throw new MetaMatrixComponentException(e,ErrorMessageKeys.EXTENSION_0015, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0015, sourceName, principalName));
        } finally {
            if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
					LogManager.logWarning(LOG_CONTEXT, txne, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0014));
                }
                transaction = null;
            }
        }
    }

    /**
     * Returns List (of Strings) of all extension module types currently
     * supported.
     * @return unmodifiable List of the String names of the currently-supported
     * extension module types
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws ExtensionModuleRuntimeException if this object wasn't initialized properly
     */
    public Collection getSourceTypes() throws MetaMatrixComponentException{
        return ExtensionModuleTypes.ALL_TYPES;
    }

    /**
     * Returns List (of Strings) of all extension module names, in order of
     * their search ordering (empty List if there are none)
     * @return List (of Strings) of all extension module names, in order of
     * their search ordering (empty List if there are none)
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws ExtensionModuleRuntimeException if this object wasn't initialized properly
     */
    public List getSourceNames() throws MetaMatrixComponentException{
        List result = null;
        ExtensionModuleTransaction transaction = null;
        try {
            transaction = getReadTransaction();
            result = transaction.getSourceNames();
            transaction.commit();
        } catch ( ManagedConnectionException e ) {
            throw new MetaMatrixComponentException(e,ErrorMessageKeys.EXTENSION_0016, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0016));
        } finally {
            if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    LogManager.logWarning(LOG_CONTEXT, txne, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0014));
                }
                transaction = null;
            }
        }
        return result;
    }

    /**
     * Returns List of ExtensionModuleDescriptor objects, in order
     * of their search ordering, or empty List if no extension modules
     * exist
     * @return List of ExtensionModuleDescriptor objects, in order
     * of their search ordering, or empty List if no extension modules
     * exist
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws ExtensionModuleRuntimeException if this object wasn't initialized properly
     */
    public List getSourceDescriptors() throws MetaMatrixComponentException{
        List result = null;
        ExtensionModuleTransaction transaction = null;
        try {
            transaction = getReadTransaction();
            result = transaction.getSourceDescriptors();
            transaction.commit();
		} catch (ManagedConnectionException e ) {
			throw new MetaMatrixComponentException(e,ErrorMessageKeys.EXTENSION_0017, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0017));
        } finally {
            if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    LogManager.logWarning(LOG_CONTEXT, txne, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0014));
                }
                transaction = null;
            }
        }
        return result;
    }

    /**
     * Returns List of ExtensionModuleDescriptor objects of indicated type,
     * in order of their search ordering, or empty List if no descriptors
     * exist for that type.
     * @param type one of the known types of extension file
     * @return List of ExtensionModuleDescriptor objects of indicated type,
     * in order of their search ordering, or empty List if no descriptors
     * exist for that type.
     * @throws InvalidExtensionTypeException if the indicated type is not one
     * of the currently-supported extension module types
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws ExtensionModuleRuntimeException if this object wasn't initialized properly
     * @throws IllegalArgumentException if any required parameters are null
     */
    public List getSourceDescriptors(String type)
    throws InvalidExtensionModuleTypeException, MetaMatrixComponentException{
        ArgCheck.isNotNull(type);
        ArgCheck.isNotZeroLength(type);
        ExtensionModuleTypes.checkTypeIsValid(type);

        List result = null;
        ExtensionModuleTransaction transaction = null;
        try {
            transaction = getReadTransaction();
            boolean includeDisabled = true;
            result = transaction.getSourceDescriptors(type, includeDisabled);
            transaction.commit();
		} catch ( ManagedConnectionException e ) {
            throw new MetaMatrixComponentException(e,ErrorMessageKeys.EXTENSION_0018, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0018, type));
        } finally {
            if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    LogManager.logWarning(LOG_CONTEXT, txne, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0014));
                }
                transaction = null;
            }
        }
        return result;
    }

    /**
     * Returns the ExtensionModuleDescriptor object for the extension
     * module indicated by sourceName
     * @param sourceName name (e.g. filename) of extension module
     * @return the ExtensionModuleDescriptor object for the extension
     * module indicated by sourceName
     * @throws ExtensionModuleNotFoundException if no extension module with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws ExtensionModuleRuntimeException if this object wasn't initialized properly
     * @throws IllegalArgumentException if any required parameters are null
     */
    public boolean isSourceInUse(String sourceName)
    throws MetaMatrixComponentException{
        ArgCheck.isNotNull(sourceName);
        ArgCheck.isNotZeroLength(sourceName);
        ExtensionModuleTransaction transaction = null;
        try {
            transaction = getReadTransaction();
            boolean result = transaction.isNameInUse(sourceName);
            transaction.commit();
            return  result;
		} catch ( ManagedConnectionException e ) {
			throw new MetaMatrixComponentException(e,ErrorMessageKeys.EXTENSION_0019, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0019, sourceName));
        } finally {
            if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    LogManager.logWarning(LOG_CONTEXT, txne, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0014));
                }
                transaction = null;
            }
        }
    }


    /**
     * Returns the ExtensionModuleDescriptor object for the extension
     * module indicated by sourceName
     * @param sourceName name (e.g. filename) of extension module
     * @return the ExtensionModuleDescriptor object for the extension
     * module indicated by sourceName
     * @throws ExtensionModuleNotFoundException if no extension module with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws ExtensionModuleRuntimeException if this object wasn't initialized properly
     * @throws IllegalArgumentException if any required parameters are null
     */
    public ExtensionModuleDescriptor getSourceDescriptor(String sourceName)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        ArgCheck.isNotNull(sourceName);
        ArgCheck.isNotZeroLength(sourceName);
        ExtensionModuleDescriptor result = null;
        ExtensionModuleTransaction transaction = null;
        try {
            transaction = getReadTransaction();
            result = transaction.getSourceDescriptor(sourceName);
            transaction.commit();
        } catch (ManagedConnectionException e) {
        	throw new MetaMatrixComponentException(e);
		} finally {
            if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    LogManager.logWarning(LOG_CONTEXT, txne, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0014));
                }
                transaction = null;
            }
        }
        return result;
    }

    /**
     * Sets the positions in the search order of all modules (all modules
     * must be included or an ExtensionModuleOrderingException will be thrown)
     * The sourceNames List parameter should indicate the new desired order.
     * <i>All caches (of Class objects) are cleared.</i>
     * @param principalName name of principal requesting this addition
     * @param sourceNames Collection of String names of existing
     * extension modules whose search position is to be set
     * @return updated List of ExtensionModuleDescriptor objects, in order
     * of their search ordering
     * @throws ExtensionModuleOrderingException if the extension files could
     * not be ordered as requested because another administrator had
     * concurrently added or removed an extension file or files, or because
     * an indicated position is out of bounds.
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws ExtensionModuleRuntimeException if this object wasn't initialized properly
     * @throws IllegalArgumentException if any required parameters are null
     */
    public List setSearchOrder(String principalName, List sourceNames)
    throws ExtensionModuleOrderingException, MetaMatrixComponentException{
        ArgCheck.isNotNull(principalName);
        ArgCheck.isNotNull(sourceNames);
        ArgCheck.isNotZeroLength(principalName);
        LogManager.logTrace(LOG_CONTEXT, new Object[] {"Setting search order for module(s)", sourceNames, "for principal", principalName} ); //$NON-NLS-1$ //$NON-NLS-2$
        List result = null;
        ExtensionModuleTransaction transaction = null;
        try {
            transaction = getWriteTransaction();
            transaction.setSearchOrder(principalName, sourceNames);
            transaction.commit();
            
            // the reads are done outside the write transaction
            // this is done for DB2
            transaction = this.getReadTransaction();
            result = transaction.getSourceDescriptors();
            transaction.commit();
            
            notifyFileChanged();
            
		} catch ( ManagedConnectionException e ) {
			throw new MetaMatrixComponentException(e,ErrorMessageKeys.EXTENSION_0020, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0020, principalName));
        } finally {
            if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    LogManager.logWarning(LOG_CONTEXT, txne, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0014));
                }
                transaction = null;
            }
        }
        return result;
    }

    /**
     * Sets the "enabled" (for searching) property of all of the indicated
     * extension modules.  <i>All caches (of Class
     * objects) are cleared.</i>
     * @param principalName name of principal requesting this addition
     * @param sourceNames Collection of String names of existing
     * extension modules whose "enabled" status is to be set
     * @param enabled indicates whether each extension module is enabled for
     * being searched or not (for convenience, a module can be disabled
     * without being removed)
     * @return updated List of ExtensionModuleDescriptor objects, in order
     * of their search ordering
     * @throws ExtensionModuleNotFoundException if no extension module with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws ExtensionModuleRuntimeException if this object wasn't initialized properly
     * @throws IllegalArgumentException if any required parameters are null
     */
    public List setEnabled(String principalName, Collection sourceNames, boolean enabled)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        ArgCheck.isNotNull(principalName);
        ArgCheck.isNotNull(sourceNames);
        ArgCheck.isNotZeroLength(principalName);
        LogManager.logTrace(LOG_CONTEXT, new Object[] {"Setting 'enabled' attribute of module(s)", sourceNames, "to", (enabled?Boolean.TRUE:Boolean.FALSE) ,"for principal", principalName} ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        List result = null;
        ExtensionModuleTransaction transaction = null;
        try {
            transaction = getWriteTransaction();
            transaction.setEnabled(principalName, sourceNames, enabled);
            transaction.commit();
            
            // the reads are done outside the write transaction
            // this is done for DB2
            transaction = this.getReadTransaction();

            List descriptors = transaction.getSourceDescriptors();
            transaction.commit();

            notifyFileChanged();

            
            // return only the descriptors that were changed
            result = new ArrayList(descriptors.size());
            ExtensionModuleDescriptor descriptor = null;
            for (Iterator i = descriptors.iterator(); i.hasNext(); ){
                descriptor = (ExtensionModuleDescriptor)i.next();
                if (sourceNames.contains(descriptor.getName())) {
                    result.add(descriptor);
                }
            }
		} catch ( ManagedConnectionException e ) {
			throw new MetaMatrixComponentException(e,ErrorMessageKeys.EXTENSION_0021, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0021, principalName));
        } finally {
            if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    LogManager.logWarning(LOG_CONTEXT, txne, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0014));
                }
                transaction = null;
            }
        }
        return result;
    }

    /**
     * Retrieves an extension module in byte[] form
     * @param sourceName name (e.g. filename) of extension module
     * @return actual contents of module in byte[] array form
     * @throws ExtensionModuleNotFoundException if no extension module with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws ExtensionModuleRuntimeException if this object wasn't initialized properly
     * @throws IllegalArgumentException if any required parameters are null
     */
    public byte[] getSource(String sourceName)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        ArgCheck.isNotNull(sourceName);
        ArgCheck.isNotZeroLength(sourceName);
        LogManager.logDetail(LOG_CONTEXT, new Object[]{"Attempting to load extension module", sourceName}); //$NON-NLS-1$
        byte[] result = null;
        ExtensionModuleTransaction transaction = null;
        try {
            transaction = getReadTransaction();
            result = transaction.getSource(sourceName);
            transaction.commit();
        } catch ( ExtensionModuleNotFoundException e ) {
            throw e;
        } catch ( MetaMatrixComponentException e ) {
			throw e;
		} catch ( Exception e ) {
			throw new MetaMatrixComponentException(e,ErrorMessageKeys.EXTENSION_0022, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0022, sourceName));
        } finally {
            if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    LogManager.logWarning(LOG_CONTEXT, txne, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0014));
                }
                transaction = null;
            }
        }
        return result;
    }

    /**
     * Updates the indicated extension module.  <i>All caches (of Class
     * objects) are cleared.</i>
     * @param principalName name of principal requesting this addition
     * @param sourceName name (e.g. filename) of extension module
     * @param source actual contents of module
     * @return ExtensionModuleDescriptor describing the newly-updated
     * extension module
     * @throws ExtensionModuleNotFoundException if no extension module with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws ExtensionModuleRuntimeException if this object wasn't initialized properly
     * @throws IllegalArgumentException if any required parameters are null or invalid
     */
    public ExtensionModuleDescriptor setSource(String principalName, String sourceName, byte[] source)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        ArgCheck.isNotNull(principalName);
        ArgCheck.isNotNull(sourceName);
        ArgCheck.isNotZeroLength(principalName);
        ArgCheck.isNotZeroLength(sourceName);
        ArgCheck.isTrue(source.length <= SOURCE_CONTENTS_LENGTH_LIMIT, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0011, SOURCE_CONTENTS_LENGTH_LIMIT));

        LogManager.logTrace(LOG_CONTEXT, new Object[] {"Setting content of module", sourceName, "for principal", principalName} ); //$NON-NLS-1$ //$NON-NLS-2$
        ExtensionModuleDescriptor result = null;
        ExtensionModuleTransaction transaction = null;
        try {
            transaction = getWriteTransaction();
            result = transaction.setSource(principalName, sourceName, source, getChecksum(source));
            transaction.commit();
            
            notifyFileChanged();
		} catch ( ManagedConnectionException e ) {
			throw new MetaMatrixComponentException(e,ErrorMessageKeys.EXTENSION_0012, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0012, sourceName));
        } finally {
            if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    LogManager.logWarning(LOG_CONTEXT, txne, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0014));
                }
                transaction = null;
            }
        }
        return result;
    }

    /**
     * Updates the indicated extension module's source name
     * @param principalName name of principal requesting this addition
     * @param sourceName name (e.g. filename) of extension module
     * @param newName new name for the module
     * @return ExtensionModuleDescriptor describing the newly-updated
     * extension module
     * @throws ExtensionModuleNotFoundException if no extension module with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws ExtensionModuleRuntimeException if this object wasn't initialized properly
     * @throws IllegalArgumentException if any required parameters are null or invalid
     */
    public ExtensionModuleDescriptor setSourceName(String principalName, String sourceName, String newName)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        ArgCheck.isNotNull(principalName);
        ArgCheck.isNotNull(sourceName);
        ArgCheck.isNotNull(newName);
        ArgCheck.isNotZeroLength(principalName);
        ArgCheck.isNotZeroLength(sourceName);
        ArgCheck.isNotZeroLength(newName);
        ArgCheck.isTrue(sourceName.length() <= SOURCE_NAME_LENGTH_LIMIT, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0009, SOURCE_NAME_LENGTH_LIMIT));

        LogManager.logTrace(LOG_CONTEXT, new Object[] {"Changing name of module from", sourceName, "to", newName, "for principal", principalName} ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ExtensionModuleDescriptor result = null;
        ExtensionModuleTransaction transaction = null;
        try {
            transaction = getWriteTransaction();
            result = transaction.setSourceName(principalName, sourceName, newName);
            transaction.commit();
            
            notifyFileChanged();
		} catch ( ManagedConnectionException e ) {
			throw new MetaMatrixComponentException(e,ErrorMessageKeys.EXTENSION_0023, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0023, sourceName));
        } finally {
            if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    LogManager.logWarning(LOG_CONTEXT, txne, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0014));
                }
                transaction = null;
            }
        }
        return result;
    }

    /**
     * Updates the indicated extension module's description
     * @param principalName name of principal requesting this addition
     * @param sourceName name (e.g. filename) of extension module
     * @param description (optional) description of the extension module.
     * <code>null</code> can be passed in to indicate no description.
     * @return ExtensionModuleDescriptor describing the newly-updated
     * extension module
     * @throws ExtensionModuleNotFoundException if no extension module with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws ExtensionModuleRuntimeException if this object wasn't initialized properly
     * @throws IllegalArgumentException if any required parameters are null or invalid
     */
    public ExtensionModuleDescriptor setSourceDescription(String principalName, String sourceName, String description)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        ArgCheck.isNotNull(principalName);
        ArgCheck.isNotNull(sourceName);
        ArgCheck.isNotZeroLength(principalName);
        ArgCheck.isNotZeroLength(sourceName);
//        ArgCheck.isTrue(description.length() <= SOURCE_DESCRIPTION_LENGTH_LIMIT, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0010, SOURCE_DESCRIPTION_LENGTH_LIMIT));

        description = adjustLengthToFit(description);        

        LogManager.logTrace(LOG_CONTEXT, new Object[] {"Setting description of module", sourceName, "for principal", principalName} ); //$NON-NLS-1$ //$NON-NLS-2$
        ExtensionModuleDescriptor result = null;
        ExtensionModuleTransaction transaction = null;
        try {
            transaction = getWriteTransaction();
            result = transaction.setSourceDescription(principalName, sourceName, description);
            transaction.commit();
		} catch ( ManagedConnectionException e ) {
			throw new MetaMatrixComponentException(e,ErrorMessageKeys.EXTENSION_0024, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0024, sourceName));
        } finally {
            if ( transaction != null ) {
                try {
                    transaction.close();
                } catch ( Exception txne ) {
                    LogManager.logWarning(LOG_CONTEXT, txne, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0014));
                }
                transaction = null;
            }
        }
        return result;
    }

    //===================================================================
    //initialization
    //===================================================================

    /**
     * constructor
     * @throws ManagedConnectionException 
     */
    public ExtensionModuleManager() {
    	Properties resourceProps = new Properties();
        String key = ExtensionModulePropertyNames.CONNECTION_FACTORY;
        resourceProps.setProperty(key, CurrentConfiguration.getInstance().getBootStrapProperties().getProperty(key, ExtensionModulePropertyNames.DEFAULT_CONNECTION_FACTORY_CLASS));
        init(resourceProps);
    }
    
    public ExtensionModuleManager(Properties p) {
    	init(p);
    }

    /**
     * Initializes this object, given the necessary Properties.
     * @param env the necessary Properties to initialize this class,
     * see {@link ExtensionModulePropertyNames}
     * @throws ManagedConnectionException 
     */
    protected void init(Properties env) {
    	if (env.getProperty(ExtensionModulePropertyNames.CONNECTION_FACTORY) == null) {
    		env.setProperty(ExtensionModulePropertyNames.CONNECTION_FACTORY, ExtensionModulePropertyNames.DEFAULT_CONNECTION_FACTORY_CLASS);
    	}
        env.setProperty(TransactionMgr.FACTORY, env.getProperty(ExtensionModulePropertyNames.CONNECTION_FACTORY));
        try {
			transMgr = new TransactionMgr(env, "ExtensionModuleManager"); //$NON-NLS-1$
		} catch (ManagedConnectionException e) {
			throw new MetaMatrixRuntimeException(e);
		} 
    }

    //===================================================================
    //NON-PUBLIC caching and class loading
    //===================================================================

	/**
	 * Retrieves a checksum value for the contents of an extension module
	 */
	public static long getChecksum(byte[] data){
	    Checksum algorithm = new CRC32();
	    algorithm.update(data, 0, data.length);
    	return algorithm.getValue();
	}

    public ExtensionModuleTransaction getReadTransaction() throws ManagedConnectionException {
        return (ExtensionModuleTransaction) this.transMgr.getReadTransaction();
    }

    public ExtensionModuleTransaction getWriteTransaction() throws ManagedConnectionException {
        return (ExtensionModuleTransaction) this.transMgr.getWriteTransaction();
    }

    /**
     * Notifies listeners when JDBCNames.ExtensionFilesTable.ColumnName.FILE_TYPE
     * has changed.   
     * 
     * @since 4.2
     */
    public void notifyFileChanged() {
        try {
        	MessageBus bus = ResourceFinder.getMessageBus();
        	if (bus != null) {
        		bus.processEvent(new ExtensionModuleEvent(JDBCExtensionModuleWriter.class, ExtensionModuleEvent.TYPE_FILE_CHANGED));
        	}
        } catch (Exception e) {
            LogManager.logError(LOG_CONTEXT, e, e.getMessage()); 
        }
    }

}

