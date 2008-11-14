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

package com.metamatrix.metadata.runtime.spi;

import java.util.*;

import com.metamatrix.metadata.runtime.api.*;
import com.metamatrix.metadata.runtime.exception.*;

public interface MetaBaseConnector extends RuntimeBaseConnector {

/**
 * Set the <code>VirtualDatabase</code> status.
 * @param virtualDBID represents the VirtualDatabase to be updated
 * @param userName of the person requesting the change
 * @param status is the status the VirtualDatabase should be set to
 * @exception VirtualDatabaseException if unable to perform update.
 */
    void setStatus(VirtualDatabaseID virtualDBID, short status, String userName) throws InvalidStateException, VirtualDatabaseException;

/**
 * Insert models into the <code>VirtualDatabase</code>.
 * @param metadataObjects is a collection of models to be inserted.
 * @exception VirtualDatabaseException if unable to perform insertion.
 */
    void insertModels(Collection metadataObjects, VirtualDatabaseID vdbI) throws VirtualDatabaseException;

/**
 * Insert groups into the <code>VirtualDatabase</code>.
 * @param metadataObjects is a collection of groups to be inserted.
 * @exception VirtualDatabaseException if unable to perform insertion.
 */
//    void insertGroups(Collection metadataObjects) throws VirtualDatabaseException;

/**
 * Insert elements into the <code>VirtualDatabase</code>.
 * @param metadataObjects is a collection of elements to be inserted.
 * @exception VirtualDatabaseException if unable to perform insertion.
 */
 //   void insertElements(List metadataObjects) throws VirtualDatabaseException;

/**
 * Insert keys into the <code>VirtualDatabase</code>.
 * @param metadataObjects is a collection of keys to be inserted.
 * @exception VirtualDatabaseException if unable to perform insertion
 * @throws InvalidStateException is thrown if the VirtualDatabase is not in the proper state to change to active state.
 */
//    void insertKeys(Collection metadataObjects) throws VirtualDatabaseException;

/**
 * Insert elements associated with the specified key into the <code>VirtualDatabase</code>.
 * @param keyID is the identifier for the key.
 * @param elementIDs a collection of tyep {@link Key} for this key.
 * @exception VirtualDatabaseException if unable to perform insertion
 * @throws InvalidStateException is thrown if the VirtualDatabase is not in the proper state to change to active state.
 */
//    void insertKeyElements(KeyID keyID, Collection elementIDs) throws VirtualDatabaseException;

/**
 * Insert procedures into the <code>VirtualDatabase</code>.
 * @param metadataObjects is a collection of procedures to be inserted.
 * @exception VirtualDatabaseException if unable to perform insertion.
 */
//    void insertProcedures(Collection metadataObjects) throws VirtualDatabaseException;


/**
 * Insert models into the <code>VirtualDatabase</code>.
 * @param metadataObjects is a collection of models to be inserted.
 * @exception VirtualDatabaseException if unable to perform insertion.
 *
    void insertKeyElements(KeyID key, Collection elements) throws VirtualDatabaseException;
*/

/**
 * Insert data types into the <code>VirtualDatabase</code>.
 * @param metadataObjects is a collection of data types to be inserted.
 * @exception VirtualDatabaseException if unable to perform insertion.
 */
//    void insertDataTypes(Collection dataTypes)  throws VirtualDatabaseException;
    
/**
 * Renames an existing model identified by this {@link ModelID} to the specified name. 
 * @param modelID {@link ModelID} of the existing model
 * @param newModelName new name for the model
 * @throws VirtualDatabaseException if errors were encountered renaming
 * the model in the virtual database
 */
//    void renameModel(ModelID modelID, String newModelName) throws VirtualDatabaseException;

/**
 * Delete models from the <code>VirtualDatabase</code>.
 * @param metadataObjects is a collection of models to be deleted
 * @exception VirtualDatabaseException if unable to perform delete.
 */
//    void deleteModels(Collection metadataObjects) throws VirtualDatabaseException;

/**
 * Delete VDB-models from the <code>VirtualDatabase</code>.
 * @param modelIDs is a collection of model IDs to be deleted.
 * @param vdbID is the ID of the virtual database.
 * @exception VirtualDatabaseException if unable to perform insertion.
 * @throws InvalidStateException is thrown if the VirtualDatabase is not in the proper state to change to active state.
 */
//    void deleteVDBModels(Collection modelIDs, VirtualDatabaseID vdbID) throws VirtualDatabaseException;

/**
 * Delete groups from the <code>VirtualDatabase</code>.
 * @param metadataObjects is a collection of groups to be deleted
 * @exception VirtualDatabaseException if unable to perform delete.
 */
//    void deleteGroups(Collection metadataObjects) throws VirtualDatabaseException;

/**
 * Delete elements from the <code>VirtualDatabase</code>.
 * @param metadataObjects is a collection of elements to be deleted.
 * @exception VirtualDatabaseException if unable to perform detete.
 */
//    void deleteElements(List metadataObjects) throws VirtualDatabaseException;

/**
 * Delete keys from the <code>VirtualDatabase</code>.
 * @param metadataObjects is a collection of keys to be deleted.
 * @exception VirtualDatabaseException if unable to perform delete
 */
//    void deleteKeys(Collection metadataObjects) throws VirtualDatabaseException;

/**
 * Delete ElementID references from the <code>VirtualDatabase</code>
 * associated with the given collection of {@link Key} instances.
 * @param metadataObjects is a collection of {@link Key} whose ElementID
 * references are to be deleted.
 * @exception VirtualDatabaseException if unable to perform delete
 */
 //   void deleteKeyElements(Collection metadataObjects) throws VirtualDatabaseException;

/**
 * Delete procedures from the <code>VirtualDatabase</code>.
 * @param metadataObjects is a collection of procedures to be deleted.
 * @exception VirtualDatabaseException if unable to perform delete.
 */
 //   void deleteProcedures(Collection metadataObjects) throws VirtualDatabaseException;

/**
 * Delete data types from the <code>VirtualDatabase</code>.
 * @param metadataObjects is a collection of data types to be deleted.
 * @exception VirtualDatabaseException if unable to perform delete.
 */
//    void deleteDataTypes(Collection dataTypes)  throws VirtualDatabaseException;
    
/**
 * Insert  the <code>VirtualDatabase</code>.
 * @param vdb is the virtual database to be inserted.
 * @exception VirtualDatabaseException if unable to perform insertion.
 * @throws InvalidStateException is thrown if the VirtualDatabase is not in the proper state to change to active state.
 */
    void insertVirtualDatabase(VirtualDatabase vdb)  throws VirtualDatabaseException;

/**
 * Insert VDB-models into the <code>VirtualDatabase</code>.
 * @param modelIDs is a collection of model IDs to be inserted.
 * @param vdbID is the ID of the virtual database.
 * @exception VirtualDatabaseException if unable to perform insertion.
 * @throws InvalidStateException is thrown if the VirtualDatabase is not in the proper state to change to active state.
 */
//    void insertVDBModels(Collection modelIDs, VirtualDatabaseID vdbID) throws VirtualDatabaseException;

/**
 * Used only by the RuntimeMetadataCatalog to find the active id.
 */
    VirtualDatabaseID getActiveVirtualDatabaseID(String vdbName, String vdbVersion) throws VirtualDatabaseException, VirtualDatabaseDoesNotExistException;

/**
 * Returns the VirtualDatabase for the specified VirtualdatabaseID.
 * @return VirtualDatabase
 */

    VirtualDatabase getVirtualDatabase(VirtualDatabaseID vdbID) throws VirtualDatabaseException;
/**
 * Used only by the RuntimeMetadataCatalog to find the virtual database id.
 */
    VirtualDatabaseID getVirtualDatabaseID(String vdbName, String vdbVersion) throws VirtualDatabaseException, VirtualDatabaseDoesNotExistException;

/**
 * Returns a <code>Collection</code> of type <code>VirtualDatabase</code> that represents all the virtual databases in the system.
 * @return Collection of type VirtualDatabase
 * @throws VirtualDatabaseException an error occurs while trying to read the data.
 */
    Collection getVirtualDatabases() throws VirtualDatabaseException;

/**
 * Returns a <code>Collection</code> of type <code>VirtualDatabase</code> that represents all the virtual databases that marked for deletion in the system.
 * @return Collection of type VirtualDatabase
 * @throws VirtualDatabaseException an error occurs while trying to read the data.
 */
    Collection getDeletedVirtualDatabaseIDs() throws VirtualDatabaseException;

/**
 * Returns a <code>Collection</code> of type <code>ElementID</code> that represents all the elements in the key.
 * @param keyID is the ID of the key.
 * @param vdbID is the ID of the virtual database.
 * @return Collection of type ElementID
 * @throws VirtualDatabaseException an error occurs while trying to read the data.
 */
 //   List getElementIDsInKey(KeyID keyID, VirtualDatabaseID vdbID) throws VirtualDatabaseException;
    
    /**
     * Return the number of Groups associated with the specified model
     * @param modelID
     * @param vdbID is the ID of the virtual database.
     * @return
     * @throws VirtualDatabaseException
     */
//    int getGroupCount(ModelID modelID, VirtualDatabaseID vdbID) throws VirtualDatabaseException;
    
    /**
     * Return the number of Groups associated with the specified model
     * for which metadata has been loaded
     * @param modelID
     * @param vdbID is the ID of the virtual database.
     * @return
     * @throws VirtualDatabaseException
     */
//    int getLoadedGroupCount(ModelID modelID, VirtualDatabaseID vdbID) throws VirtualDatabaseException;

/**
 * Set a cache, which is a type of <code>MetadataSourceAPI</code>, to the <code>MetaBastConnector</code>.
 * @param cache is the cache that implements MetadataSourceAPI.
 * @return Collection of type ElementID
 */
 //   void setCache(MetadataSourceAPI cache);

/**
 * Delete the <code>VirtualDatabase</code>.
 * @param vdbID is the ID of the virtual database to be deleted.
 * @throws VirtualDatabaseException an error occurs while trying to delete the data.
 */
    void deleteVirtualDatabase(VirtualDatabaseID vdbID)throws VirtualDatabaseException;

/**
 * Returns a <code>Collection</code> of type <code>ModelID</code> that represents all the models in the MetaBase.
 * @return List of type ModelID
 * @throws VirtualDatabaseException an error occurs while trying to read the data.
 */
    List getAllModelIDs() throws VirtualDatabaseException;

    Collection getModels(VirtualDatabaseID vdbID) throws VirtualDatabaseException;
    
/**
 *
 */
    void setConnectorBindingNames(VirtualDatabaseID vdbID, Collection models, Map modelAndCBNames) throws VirtualDatabaseException;

    /**
     * Set visibility levels for models in a virtual database. Two visibility levels
     * are legal at this time: "Public" and "Private", which are defined in MetadataConstants.VISIBILITY_TYPES.
     * Defaults to "Public" if null.
     * @param vdbID is the VirtualDatabaseID
     * @param modelAndVisibilities contains Model name and visibility pare.
     * @throws VirtualDatabaseException an error occurs while trying to read or write the data.
     */
//    void setModelVisibilityLevels(VirtualDatabaseID vdbID, Map modelAndVisibilities) throws VirtualDatabaseException;

    /**
     * Update VDB attributes. Only the attributes defined in <code>VirtualDatabase.ModifiableAttributes</code>
     * can be modefied. Call VirtualDatabase.update(String attribute, Object value)
     * to update each attribute of the VDB before calling this method.
     * @param vdb VDB to be updated.
     * @param userName of the person updating the virtual database.
     */
    public void updateVirtualDatabase(VirtualDatabase vdb, String userName) throws VirtualDatabaseException;

	/**
	 * returns a <code>Collection</code> of type <code>DataType</code> that represents all the datatypes that have been defined for this VirtualDatabase.  This includes all the MetaMatrix standard types.
	 * @param vdbID is the VirtualDatabaseID
	 * @return Collection of type DataType
	 * @throws VirtualDatabaseException an error occurs while trying to read the data.
	 */
//    Collection getDataTypes() throws VirtualDatabaseException;

//    void updateVDBModels(VirtualDatabaseID vdbID, Map modelIDs) throws VirtualDatabaseException;

//    Map getCBNamesForRuntimeModels() throws VirtualDatabaseException;

//    Map getVNamesForRuntimeModels() throws VirtualDatabaseException;

//    void updateSystemModels() throws VirtualDatabaseException;
    
    /**
	 * returns all the <code>DataType</code> in Runtime Metadata. 
	 * @return Collection of DataType instances.
	 * @throws VirtualDatabaseException an error occurs while trying to read the data.
	 */
//    Collection getAllDataTypes() throws VirtualDatabaseException;
    
//    Collection getAllDataTypes(Collection modelIDs) throws VirtualDatabaseException;
}
