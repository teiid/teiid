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

package com.metamatrix.common.extensionmodule.spi.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.connection.BaseTransaction;
import com.metamatrix.common.connection.ManagedConnection;
import com.metamatrix.common.connection.ManagedConnectionException;
import com.metamatrix.common.connection.jdbc.JDBCMgdResourceConnection;
import com.metamatrix.common.extensionmodule.ExtensionModuleDescriptor;
import com.metamatrix.common.extensionmodule.exception.DuplicateExtensionModuleException;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleNotFoundException;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleOrderingException;
import com.metamatrix.common.extensionmodule.spi.ExtensionModuleTransaction;
import com.metamatrix.common.util.ErrorMessageKeys;

public class JDBCExtensionModuleTransaction extends BaseTransaction implements ExtensionModuleTransaction {

    private Connection jdbcConnection;

    /**
     * Create a new instance of a transaction for a managed connection.
     * @param connectionPool the pool to which the transaction should return the connection when completed
     * @param connection the connection that should be used and that was created using this
     * factory's <code>createConnection</code> method (thus the transaction subclass may cast to the
     * type created by the <code>createConnection</code> method.
     * @param readonly true if the transaction is to be readonly, or false otherwise
     * @throws ManagedConnectionException if there is an error creating the transaction.
     */
    JDBCExtensionModuleTransaction(ManagedConnection connection, boolean readonly)
    throws ManagedConnectionException{
        super(connection, readonly);

        try {
            JDBCMgdResourceConnection jdbcManagedConnection = (JDBCMgdResourceConnection) connection;
            this.jdbcConnection = jdbcManagedConnection.getConnection();

        } catch ( Exception e ) {
            throw new ManagedConnectionException(ErrorMessageKeys.EXTENSION_0051, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0051));
        }


    }


    //===================================================================
    //PUBLIC INTERFACE
    //===================================================================

    /**
     * Adds an extension module to the end of the list of modules
     * @param principalName name of principal requesting this addition
     * @param type one of the known types of extension file
     * @param sourceName name (e.g. filename) of extension module
     * @param data actual contents of module
	 * @param checksum Checksum of file contents
     * @param description (optional) description of the extension module
     * @param enabled indicates whether each extension module is enabled for
     * being searched or not (for convenience, a module can be disabled
     * without being removed)
     * @return ExtensionModuleDescriptor describing the newly-added
     * extension module
     * @throws DuplicateExtensionModuleException if an extension module
     * with the same sourceName already exists
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public ExtensionModuleDescriptor addSource(String principalName, String type, String sourceName, byte[] data, long checksum, String description, boolean enabled)
    throws DuplicateExtensionModuleException, MetaMatrixComponentException{
        return JDBCExtensionModuleWriter.addSource(principalName, type, sourceName, data, checksum, description, enabled, jdbcConnection);

    }

    /**
     * Deletes a module from the list of modules
     * @param principalName name of principal requesting this addition
     * @param sourceName name (e.g. filename) of extension module
     * @throws ExtensionModuleNotFoundException if no extension module with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public void removeSource(String principalName, String sourceName)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        JDBCExtensionModuleWriter.removeSource(principalName, sourceName, jdbcConnection);
    }

    /**
     * Returns List (of Strings) of all extension module names, in order of
     * their search ordering
     * @return List (of Strings) of all extension module names, in order of
     * their search ordering
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public List getSourceNames() throws MetaMatrixComponentException{
        return JDBCExtensionModuleReader.getSourceNames(jdbcConnection);
            }

    /**
     * Returns List of ExtensionModuleDescriptor objects, in order
     * of their search ordering
     * @return List of ExtensionModuleDescriptor objects, in order
     * of their search ordering
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public List getSourceDescriptors() throws MetaMatrixComponentException{
        return this.getSourceDescriptors(null, true);
    }

    /**
     * Returns List of ExtensionModuleDescriptor objects of indicated type,
     * in order of their search ordering
     * @param type one of the known types of extension file
     * @param includeDisabled if "false", only descriptors for <i>enabled</i>
     * extension modules will be returned; otherwise all modules will be.
     * @return List of ExtensionModuleDescriptor objects of indicated type,
     * in order of their search ordering
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public List getSourceDescriptors(String type, boolean includeDisabled) throws MetaMatrixComponentException{
        return JDBCExtensionModuleReader.getSourceDescriptors(type, includeDisabled, jdbcConnection);
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
     */
    public ExtensionModuleDescriptor getSourceDescriptor(String sourceName)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        return JDBCExtensionModuleReader.getSourceDescriptor(sourceName, jdbcConnection);
    }

    /**
     * Sets the positions in the search order of all modules (all modules
     * must be included or an ExtensionModuleOrderingException will be thrown)
     * The sourceNames List parameter should indicate the new desired order.
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
     */
    public void setSearchOrder(String principalName, List sourceNames)
    throws ExtensionModuleOrderingException, MetaMatrixComponentException{
        JDBCExtensionModuleWriter.setSearchOrder(principalName, sourceNames, jdbcConnection);
    }

    /**
     * Sets the "enabled" (for searching) property of all of the indicated
     * extension modules.
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
     */
    public void setEnabled(String principalName, Collection sourceNames, boolean enabled)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        JDBCExtensionModuleWriter.setEnabled(principalName, sourceNames, enabled, jdbcConnection);
    }

    /**
     * Retrieves an extension module in byte[] form
     * @param sourceName name (e.g. filename) of extension module
     * @return actual contents of module in byte[] array form
     * @throws ExtensionModuleNotFoundException if no extension module with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public byte[] getSource(String sourceName)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{

        try {
            return JDBCExtensionModuleReader.getSource(sourceName, jdbcConnection);
        } catch (SQLException se){
            throw new MetaMatrixComponentException(se, ErrorMessageKeys.EXTENSION_0047, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0047, sourceName));
        }

    }

    public ExtensionModuleDescriptor setSource(String principalName, String sourceName, byte[] data, long checksum)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        JDBCExtensionModuleWriter.setSource(principalName, sourceName, data, checksum, jdbcConnection);
        return JDBCExtensionModuleReader.getSourceDescriptor(sourceName, jdbcConnection);

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
     */
    public ExtensionModuleDescriptor setSourceName(String principalName, String sourceName, String newName)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        return JDBCExtensionModuleWriter.setSourceName(principalName, sourceName, newName, jdbcConnection);
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
     */
    public ExtensionModuleDescriptor setSourceDescription(String principalName, String sourceName, String description)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        return JDBCExtensionModuleWriter.setSourceDescription(principalName, sourceName, description, jdbcConnection);
    }

	/**
	 * Indicates that ExtensionModuleManager should clear its cache and refresh itself because
	 * the data this object fronts has changed (optional operation).  A service provider
	 * is not required to keep track of whether data has changed by outside means, in fact
	 * it may not even make sense.
	 * @return whether data has changed since ExtensionModuleManager last accessed this data
	 * store.
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
	 * @throws UnsupportedOperationException - this operation is not supported by this Transaction.
	 */
    public boolean needsRefresh() throws MetaMatrixComponentException, UnsupportedOperationException{
        throw new UnsupportedOperationException();
    }

    public boolean isNameInUse(String sourceName) throws MetaMatrixComponentException {
            return JDBCExtensionModuleReader.isNameInUse(sourceName, jdbcConnection);
    }

}




