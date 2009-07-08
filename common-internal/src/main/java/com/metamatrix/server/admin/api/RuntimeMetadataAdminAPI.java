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

package com.metamatrix.server.admin.api;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.vdb.api.VDBDefn;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseException;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.platform.admin.api.EntitlementMigrationReport;
import com.metamatrix.platform.admin.api.PermissionDataNode;
import com.metamatrix.platform.admin.api.SubSystemAdminAPI;
import com.metamatrix.platform.security.api.AuthorizationPolicyID;

public interface RuntimeMetadataAdminAPI extends SubSystemAdminAPI {

    /**
     * Update a modified <code>VirtualDataBase</code>.
     *
     * @param vdb the modified VirtualDatabase.
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws VirtualDatabaseException if an error occurs while updating vdb.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    void updateVirtualDatabase(VirtualDatabase vdb)
        throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException;

    /**
     * Mark a <code>VirtualDataBase</code> (VDB) for deletion.
     *<p></p>
     * The VDB will not actually be deleted until there are no more users using it.
     * @param vdbID the ID of the VirtualDatabase that is to be deleted.
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws VirtualDatabaseException if an error occurs while updating vdb.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    void markVDBForDelete(VirtualDatabaseID vdbID)
        throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException;

    /**
     * Migrate connector binding names from models in a virtual database, filling in connector
     * binding names possible for the models in the DTCInfo.
     *
     * @param sourceVDB The source VirtualDatabase from which to search for models and thus,
     * connector binding names.
     * @param vdb represents a specific VDB Version
     * @return A Map of ModelNames to RoutingIDs.
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws VirtualDatabaseException if an error occurs while setting the state.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    Map migrateConnectorBindingNames(VirtualDatabase sourceVDB, VDBDefn vdb)
        throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException;

    /**
     * Set connector binding names for models in a virtual database.
     *
     * @param vdbID ID of the VirtualDatabase.
     * @param modelnamesAndConnectorBindingNames Map of <code>ModelID</code>s to String Connector Binding names.      
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws VirtualDatabaseException if an error occurs while setting the state.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    void setConnectorBindingNames(VirtualDatabaseID vdbID, Map modelnamesAndConnectorBindingNames)
        throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException;

    /**
     * Given a routing ID, find all VDBs whose models use the connector binding.
     *
     * @param routingID ID of the connector binding.
     * @return Collection of VDBs where at least one Model is using given connector binding.
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws VirtualDatabaseException if an error occurs while setting the state.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    Collection getVDBsForConnectorBinding(String routingID)
        throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException;

    /**
     * Returns the <code>VirtualDatabase</code> specified by the <code>VirtualDatabaseID</code>.
     *
     * @param vdbID ID of the VirtualDatabase.
     * @return VirtualDatabase
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws VirtualDatabaseException if an error occurs while setting the state.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    VirtualDatabase getVirtualDatabase(VirtualDatabaseID vdbID)
        throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException;

    /**
     * Returns the latest<code>VirtualDatabase</code> for the specified vdbName.
     *
     * @param callerSessionID ID of the caller's current session.
     * @param vdbName is the name of the VirtualDatabase.
     * @return VirtualDatabase
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws VirtualDatabaseException if an error occurs while setting the state.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    VirtualDatabase getLatestVirtualDatabase(String vdbName)
    throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException;


    /**
     * Returns a <code>Collection</code> of all the VirtualDatabases in the system.
     * This would include all virtual databases flagged as incomplete, active, inactive or deleted.
     *
     * @return a Collection of all Virtualdatabases in the system.
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws VirtualDatabaseException if an error occurs while setting the state.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    Collection getVirtualDatabases()
        throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException;

    /**
     * Returns a sorted <code>Collection</code> of type <code>Model</code> that represents
     * all the models that where deployed in the specified virtual database.
     *
     * @param vdbID ID of the VirtualDatabase.
     * @return a Collection of all Models deployed in the VirtualDatabase.
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws VirtualDatabaseException if an error occurs while setting the state.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    Collection getVDBModels(VirtualDatabaseID vdbID)
        throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException;

    /**
     * Updates the <code>VirtualDatabase</code> state.
     * The following four states are valid.
     * <p>Incomplete = 1: The virtula database is not fully created yet. Set by runtime metadata during the creation of a new virtual database.</p>
     * <p>Inactive = 2: Not ready for use.</p>
     * <p>Active = 3: Ready for use.</p>
     * <p>Deleted =4: Ready for deletion.</p>
     *
     * @param vdbID ID of the VirtualDatabase.
     * @param state the state the VirtualDatabase should be set to.
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws VirtualDatabaseException if an error occurs while setting the state.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    void setVDBState(VirtualDatabaseID vdbID, short state)
        throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException;

    /**
     * Migrate as many existing entitlements as possible to a newly deployed VDB.
     * @param sourceVDB The existing VDB from which to copy entitlements.
     * @param targetVDB The new VDB to which to copy entitlements.
     * @return The report that contains a result for each attempted copy.
     * @throws InvalidSessionException if the administrative session is invalid
     * @throws AuthorizationException if admninistrator does not have the authority to perform the requested operation.
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    EntitlementMigrationReport migrateEntitlements(VirtualDatabase sourceVDB, VirtualDatabase targetVDB)
    throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException;

    /**
     * Migrate as many existing entitlements as possible from a supplied roles file to a newly deployed VDB.
     * @param targetVDB The new VDB to which to copy entitlements.
     * @param dataRoleContents Data Roles xml file contents.
     * @param overwriteExisting flag to determine if any existing roles should be overwritten.
     * @return The report that contains a result for each attempted copy.
     * @throws InvalidSessionException if the administrative session is invalid
     * @throws AuthorizationException if admninistrator does not have the authority to perform the requested operation.
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    EntitlementMigrationReport migrateEntitlements(VirtualDatabase targetVDB, char[] dataRoleContents, boolean overwriteExisting)
		throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException;
    
    /**
     * Returns the entitlement tree for a given VDB version.
     * @param vDBName The name of a VDB for which to get its model info.
     * @param vDBVersion The name of a VDB version for which to get its model info.
     * @param policyID The policy (entitlement) for which to constrain the search for permissions.
     * @return The root of the entitlement tree for the given VEB version.
     * @throws InvalidSessionException if the administrative session is invalid
     * @throws AuthorizationException if admninistrator does not have the authority to perform the requested operation.
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    PermissionDataNode getEntitlementTree(String vDBName, String vDBVersion, AuthorizationPolicyID policyID)
    throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException;

    /**
     * Get the tree of data nodes that make op a VDB. This tree represents <i>just</i>
     * the data node hierarchy and does not contain authorization information.
     * @param vdbName The name of the VDB for which data nodes are sought.
     * @param vdbVersion The version of the VDB for which data nodes are sought.
     * @return The root of the tree for the VDB and version.
     * @throws InvalidSessionException if the administrative session is invalid
     * @throws AuthorizationException if admninistrator does not have the authority to perform the requested operation.
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    PermissionDataNode getDataNodes(String vDBName, String vDBVersion)
    throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException;

    /**
     * Get all data element paths for a VDB version.
     * @param vdbName The VDB name.
     * @param vdbVersion The version for the VDB.
     * @return All element paths in the given VDB version.
     */
    Set getAllDataNodeNames(String vdbName, String vdbVersion)
    throws InvalidSessionException, MetaMatrixComponentException;

    /**
     * Set the visibility levels for models in a virtual database.
     *
     * @param vdbID ID of the VirtualDatabase.
     * @param modelAndVLevels Map of model names to Short visibility levels.
     * Visibility levels are defined in MetadataConstants.VISIBILITY_TYPES.
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws VirtualDatabaseException if an error occurs while setting the state.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
//    public void setModelVisibilityLevels(VirtualDatabaseID vdbID, Map modelAndVLevels)
//        throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException;

    /**
     * Call to obtain the VDB Definition that can be used
     * for importing or exporting.
     *
     * @param vdbID ID of the VirtualDatabase.
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws VirtualDatabaseException if an error occurs while setting the state.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    public byte[] getVDB(VirtualDatabaseID vdbID )
        throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException;


    /**
     * Call to import a VDB
     * @param vdbDefn is the VDB to be imported, this includes any new connector or 
     * connector bindings.
     * @returns VirtualDatabase that was imported
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws VirtualDatabaseException if an error occurs while setting the state.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    public VirtualDatabase importVDB(byte[] vdbStream)
        throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException;

    /**
     * Validate the given connector binding is capable for connectivity to a Materialization. 
     * @param materializationConnector The connector that will be validated.
     * @return <code>true</code> iff the given connector binding passes validation.
     * @throws AuthorizationException
     * @throws InvalidSessionException
     * @throws VirtualDatabaseException
     * @throws MetaMatrixComponentException
     * @since 4.2
     */
    boolean validateConnectorBindingForMaterialization(ConnectorBinding materializationConnector)
        throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException;

    /**
     * Get the <code>MaterializationLoadScripts</code> for all materialized views in the materialization model
     * in this VDB. 
     * @param materializationConnector the connector that will be used to connect to the Materialization.
     * @param vdb the VDB to which the Materialization belongs.
     * @param mmHost The host that the MetaMatrix server is running on.
     * @param mmPort The MetaMatrix server port.
     * @param materializationUserName The name of the user that will be logging in to the database that
     * holds this particular materialization and the user that the load/refresh scripts will run under.
     * @param materializationUserPwd The password of the materialization user.
     * @param metamatrixUserName User that will log into the MetaMatrix server to perform the refresh.
     * @param metamatrixUserPwd Password of the MetaMatrix user.
     * @return The object that represents all information nessecary to save the materialization load scripts.
     * @throws AuthorizationException
     * @throws InvalidSessionException
     * @throws VirtualDatabaseException
     * @throws MetaMatrixComponentException
     * @since 4.2
     */
    MaterializationLoadScripts getMaterializationScripts(ConnectorBinding materializationConnector, VDBDefn vdb, 
                                                         String mmHost, String mmPort, String materializationUserName, String materializationUserPwd, String metamatrixUserName, String metamatrixUserPwd)
        throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException;
    
}
