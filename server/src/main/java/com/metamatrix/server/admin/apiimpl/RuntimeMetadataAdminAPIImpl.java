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

package com.metamatrix.server.admin.apiimpl;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.teiid.adminapi.AdminRoles;
import org.teiid.metadata.RuntimeMetadataPlugin;
import org.teiid.transport.SSLConfiguration;

import com.metamatrix.admin.RolesAllowed;
import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.common.util.crypto.CryptoUtil;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.common.vdb.api.SystemVdbUtility;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.common.vdb.api.VDBDefn;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.vdb.VDBStatus;
import com.metamatrix.metadata.runtime.RuntimeMetadataCatalog;
import com.metamatrix.metadata.runtime.RuntimeVDBDeleteUtility;
import com.metamatrix.metadata.runtime.api.Model;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseException;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.metadata.runtime.vdb.defn.VDBCreation;
import com.metamatrix.metadata.runtime.vdb.defn.VDBDefnFactory;
import com.metamatrix.metadata.util.ErrorMessageKeys;
import com.metamatrix.platform.admin.api.EntitlementMigrationReport;
import com.metamatrix.platform.admin.api.PermissionDataNode;
import com.metamatrix.platform.admin.apiimpl.AdminAPIHelper;
import com.metamatrix.platform.admin.apiimpl.PermissionDataNodeImpl;
import com.metamatrix.platform.security.api.AuthorizationPolicyID;
import com.metamatrix.platform.security.api.AuthorizationRealm;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.security.api.service.AuthorizationServiceInterface;
import com.metamatrix.platform.util.PlatformProxyHelper;
import com.metamatrix.server.admin.api.MaterializationLoadScripts;
import com.metamatrix.server.admin.api.RuntimeMetadataAdminAPI;
import com.metamatrix.server.admin.api.ServerAdminLogConstants;

@RolesAllowed(value=AdminRoles.RoleName.ADMIN_READONLY)
public class RuntimeMetadataAdminAPIImpl implements RuntimeMetadataAdminAPI {


    
    private static RuntimeMetadataAdminAPI runtimeMetadataAdminAPI;

    /**
     * ctor
     * Only defined here so that it doesn't get generated.
     */
    private RuntimeMetadataAdminAPIImpl() {
        
    }

    public synchronized static RuntimeMetadataAdminAPI getInstance() {
        if (runtimeMetadataAdminAPI == null) {
            runtimeMetadataAdminAPI = new RuntimeMetadataAdminAPIImpl();
        }
        return runtimeMetadataAdminAPI;
    }

    
    
    /**
     * Update a modified <code>VirtualDataBase</code>.
     *
     * @param vdb the modified VirtualDatabase.
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws VirtualDatabaseException if an error occurs while setting the state.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    @RolesAllowed(value=AdminRoles.RoleName.ADMIN_PRODUCT)
    public void updateVirtualDatabase(VirtualDatabase vdb)
        throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException {

        // Validate caller's session
        SessionToken callerToken = AdminAPIHelper.validateSession();
        RuntimeMetadataCatalog.getInstance().updateVirtualDatabase(vdb, callerToken.getUsername());
    }

    /**
     * Mark a <code>VirtualDataBase</code> (VDB) for deletion.
     * <p></p>
     * The VDB will not actually be deleted until there are no more users using it.
     * @param callerSessionID ID of the caller's current session.
     * @param vdbID the ID of the VirtualDatabase that is to be deleted.
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws VirtualDatabaseException if an error occurs while updating vdb.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    @RolesAllowed(value=AdminRoles.RoleName.ADMIN_PRODUCT)
    public void markVDBForDelete(VirtualDatabaseID vdbID)
        throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException {

        // Validate caller's session
        SessionToken callerToken = AdminAPIHelper.validateSession();

        // Get VDB's current state
        VirtualDatabase theVDB = RuntimeMetadataCatalog.getInstance().getVirtualDatabase(vdbID);
        short vdbStatus = theVDB.getStatus();

        // If it's already marked for delete, do nothing
        if (vdbStatus == VDBStatus.DELETED) {
            return;
        }

        // Can only change state to DELETED from states INACTIVE or INCOMPLETE.
        if (vdbStatus == VDBStatus.INACTIVE
            || vdbStatus == VDBStatus.INCOMPLETE) {

            // Setting status to deleted marks it as a candidate for deletion.
            RuntimeMetadataCatalog.getInstance().setVDBStatus(vdbID, VDBStatus.DELETED, callerToken.getUsername());

            // Attempt to delete it if no one is using it.
            RuntimeVDBDeleteUtility vdbDeleter = new RuntimeVDBDeleteUtility();
            vdbDeleter.deleteVDBMarkedForDelete(vdbID);
        } else {
            // If it's in another state, thow exception
            String msg = RuntimeMetadataPlugin.Util.getString("RuntimeMetadataAdminAPIImpl.Can_t_delete_VDB_in_state_{0}", VDBStatus.VDB_STATUS_NAMES[vdbStatus]);  //$NON-NLS-1$
            throw new VirtualDatabaseException(msg);
        }
    }

    /**
     * Migrate connector binding names from models in a virtual database, filling in connector
     * binding names possible for the models in the DTCInfo.
     * @param callerSessionID ID of the caller's current session.
     * @param sourceVDB The source VirtualDatabase from which to search for models and thus,
     * connector binding names.
     * @param dtc The DTC that contains the models for which to attach String Connector Binding names.  This param gets modified.
     * @return A Map of ModelNames to RoutingIDs.
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws VirtualDatabaseException if an error occurs while setting the state.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    public Map migrateConnectorBindingNames(VirtualDatabase sourceVDB, VDBDefn vdb)
        throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException {

        // Validate caller's session
        AdminAPIHelper.validateSession();


        Map modelNameToRoutingIDs = new HashMap();
        List modelNames = new ArrayList();

        // Insert all modelNames of interest
        Iterator modelItr = vdb.getModels().iterator();
        while (modelItr.hasNext()) {
            ModelInfo model = (ModelInfo) modelItr.next();
            String modelName = model.getName();
            modelNameToRoutingIDs.put(modelName, Collections.EMPTY_LIST); 
            modelNames.add(modelName);
        }

        // VDB state(s) we're interested in
        short stateFlag =
            RuntimeMetadataHelper.VDB_STATE_ACTIVE
                | RuntimeMetadataHelper.VDB_STATE_INACTIVE
                | RuntimeMetadataHelper.VDB_STATE_INCOMPLETE;

        // Starting from sourceVDB, walk back through all VDB ancesters, skipping deleted versions,
        // looking for connector bindings (routing IDs).  Stop when either we've exhausted
        // dtc's models or we've run out of VDB ancesters.
        VirtualDatabase currentVDB = sourceVDB;
        VirtualDatabaseID currentVDBID = sourceVDB.getVirtualDatabaseID();
        do {
            // Check all currentVDB's models for connector bindings
            Iterator currentModels = RuntimeMetadataCatalog.getInstance().getModels(currentVDB.getVirtualDatabaseID()).iterator();
            while (currentModels.hasNext()) {
                Model model = (Model) currentModels.next();
                String modelName = model.getName();
                if (modelNames.contains(modelName)) {
                    List cbNames = model.getConnectorBindingNames();
                    modelNameToRoutingIDs.put(modelName, cbNames);
                    
                    // If we get a routing ID, remove modelName from further consideration
                    if (cbNames.size() > 0) {
                        modelNames.remove(modelName);
                    }
                }
            }
            int currentVers = Integer.parseInt(currentVDBID.getVersion());
            currentVDB = RuntimeMetadataHelper.walkBack(currentVDB.getName(), currentVers, stateFlag);
            if (currentVDB != null) {
                currentVDBID = currentVDB.getVirtualDatabaseID();
            }
        }
       while (currentVDB != null && modelNames.size() > 0);

        return modelNameToRoutingIDs;
    }

    /**
     * Set connector binding names for models in a virtual database.
     *
     * @param callerSessionID
     * @param vdbID ID of the VirtualDatabase.
     * @param modelAndCBNames Map of <code>ModelID</code>s to String Connector Binding names.
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws VirtualDatabaseException if an error occurs while setting the state.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    @RolesAllowed(value=AdminRoles.RoleName.ADMIN_PRODUCT)
    public void setConnectorBindingNames(VirtualDatabaseID vdbID,
                                                      Map modelAndCBNames)
        throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException {

        // Validate caller's session
        SessionToken callerToken = AdminAPIHelper.validateSession();

        RuntimeMetadataCatalog.getInstance().setConnectorBindingNames(vdbID, modelAndCBNames, callerToken.getUsername());
    }

    /**
     * Given a routing ID, find all VDBs whose models use the connector binding.
     * @param callerSessionID The caller's session ID.
     * @param routingID ID of the connector binding.
     * @return Collection of VDBs where at least one Model is using given connector binding.
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws VirtualDatabaseException if an error occurs while setting the state.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    public Collection getVDBsForConnectorBinding(String routingID)
        throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException {
        // Validate caller's session
        AdminAPIHelper.validateSession();
        // Any administrator may call this read-only method - no need to validate role

        Collection VDBs = new HashSet();
        Iterator allVDBItr = RuntimeMetadataCatalog.getInstance().getVirtualDatabases().iterator();
        // Search all VDBs in system
        boolean found =false;
        while (allVDBItr.hasNext()) {
            found = false;
            VirtualDatabase aVDB = (VirtualDatabase) allVDBItr.next();
            Iterator modelItr = RuntimeMetadataCatalog.getInstance().getModels(aVDB.getVirtualDatabaseID()).iterator();
            // Search all models in VDB for connector binding name of interest
            while (!found && modelItr.hasNext()) {
                Model model = (Model) modelItr.next();
                List cbNames = model.getConnectorBindingNames();
                for (Iterator mit=cbNames.iterator(); mit.hasNext();) {
                    
                    String connectorBindingName = (String) mit.next();
                    if (connectorBindingName != null && connectorBindingName.equals(routingID)) {
                        VDBs.add(aVDB);
                        found=true;
                        break;
                    }
                    
                }
            }
        }
        return VDBs;
    }

    /**
     * Returns the <code>VirtualDatabase</code> specified by the <code>VirtualDatabaseID</code>.
     *
     * @param callerSessionID ID of the caller's current session.
     * @param vdbID ID of the VirtualDatabase.
     * @return VirtualDatabase
     * @throws VirtualDatabaseException if an error occurs while setting the state.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    public VirtualDatabase getVirtualDatabase(VirtualDatabaseID vdbID)
        throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException {

        // Validate caller's session
        AdminAPIHelper.validateSession();
        // Any administrator may call this read-only method - no need to validate role

        return RuntimeMetadataCatalog.getInstance().getVirtualDatabase(vdbID);
    }
    
    /**
     * Returns the latest<code>VirtualDatabase</code> for the specified vdbName.
     *
     * @param callerSessionID ID of the caller's current session.
     * @param vdbName is the name of the VirtualDatabase.
     * @return VirtualDatabase
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws VirtualDatabaseException if an error occurs during retrieval process.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    public VirtualDatabase getLatestVirtualDatabase(String vdbName)
    throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException {

	    // Validate caller's session
	    AdminAPIHelper.validateSession();
	    // Any administrator may call this read-only method - no need to validate role
	
	    VirtualDatabaseID vdbId = RuntimeMetadataCatalog.getInstance().getVirtualDatabaseID(vdbName, null);
	    if (vdbId != null) {
	        return RuntimeMetadataCatalog.getInstance().getVirtualDatabase(vdbId);
	    }
	    return null;
	}
    

    /**
     * Returns a <code>Collection</code> of the <code>VirtualDatabase</code>s in the system.
     * This would include all virtual databases flagged as incomplete, active, inactive or deleted.
     * @param callerSessionID ID of the caller's current session.
     * @return a Collection of all Virtualdatabases in the system.
     * @throws VirtualDatabaseException if an error occurs while setting the state.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    public Collection getVirtualDatabases()
        throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException {

        // Validate caller's session
        AdminAPIHelper.validateSession();
        // Any administrator may call this read-only method - no need to validate role

        return RuntimeMetadataCatalog.getInstance().getVirtualDatabases();
       
        
//        return filterVirtualDatabases(HIDDEN_VDBS);
    }

    /**
     * Returns a sorted <code>Collection</code> of type <code>Model</code> that represents
     * all the models that were deployed in the specified virtual database.
     *
     * @param callerSessionID ID of the caller's current session.
     * @param vdbID ID of the VirtualDatabase.
     * @return a Collection of all Models deployed in the VirtualDatabase.
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    public Collection getVDBModels(VirtualDatabaseID vdbID)
        throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException {

        // Validate caller's session
        AdminAPIHelper.validateSession();
        // Any administrator may call this read-only method - no need to validate role

        ArrayList models = new ArrayList(RuntimeMetadataCatalog.getInstance().getModels(vdbID));

        // Remove "SystemPhysical" model
        int sysModelIndex = -1;
        for (int i = 0; i < models.size(); i++) {

            Model model = (Model) models.get(i);
            if (model.getName().equals(SystemVdbUtility.PHYSICAL_MODEL_NAME)) {
                sysModelIndex = i;
                break;
            } 
        }
        if (sysModelIndex >= 0) {
            models.remove(sysModelIndex);
        }
   
        // Remove the SystemAdmin model
        int sysAdminModelIndex = -1;        
        for (int i = 0; i < models.size(); i++) {

            Model model = (Model) models.get(i);
            if (model.getName().equals(SystemVdbUtility.ADMIN_PHYSICAL_MODEL_NAME)) {
                sysAdminModelIndex = i;
                break;
            }
        }
        if (sysAdminModelIndex >= 0) {
            models.remove(sysAdminModelIndex);
        }        

        // LogManager.logCritical("========test=======", "Read to Sort Models");
        // currently BasicModel does not implement comparable interface
        //       Collections.sort(models);
        //LogManager.logCritical("========test=======", "Exiting method");
        return models;
    }

    /**
     * Updates the <code>VirtualDatabase</code> state.
     * The following four states are valid.
     * <p>Incomplete: The virtula database is not fully created yet. Set by runtime metadata during the creation of a new virtual database.</p>
     * <p>Inactive: Not ready for use.</p>
     * <p>Active: Ready for use.</p>
     * <p>Deleted: Ready for deletion.</p>
     *
     * @param callerSessionID ID of the caller's current session.
     * @param vdbID ID of the VirtualDatabase.
     * @param state the state the VirtualDatabase should be set to.
     * @throws VirtualDatabaseException if an error occurs while setting the state.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    @RolesAllowed(value=AdminRoles.RoleName.ADMIN_PRODUCT)
    public void setVDBState(VirtualDatabaseID vdbID, short state)
        throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException {

        // Validate caller's session
        SessionToken callerToken = AdminAPIHelper.validateSession();

        // Get VDB's current state
        VirtualDatabase theVDB = RuntimeMetadataCatalog.getInstance().getVirtualDatabase(vdbID);
        short vdbStatus = theVDB.getStatus();

        // If it's marked for delete or already in given state, do nothing
        if (vdbStatus == VDBStatus.DELETED || vdbStatus == state) {
            return;
        }

        // Determine if the requested state change is valid
        if(isValidStateChange(theVDB, state)) {
            // Set status
            RuntimeMetadataCatalog.getInstance().setVDBStatus(vdbID, state, callerToken.getUsername());
        } else {
            // If it's in another state, thow exception
            String msg = RuntimeMetadataPlugin.Util.getString("RuntimeMetadataAdminAPIImpl.Can__t_set_VDB_state_from_{0}_to_{1}",new Object[] {VDBStatus.VDB_STATUS_NAMES[vdbStatus],VDBStatus.VDB_STATUS_NAMES[state] }); //$NON-NLS-1$
            throw new VirtualDatabaseException(msg);
        }
        RuntimeMetadataCatalog.getInstance().setVDBStatus(vdbID, state, callerToken.getUsername());
    }

    /**
     * Helper method to determine whether the requested state change is valid
     *
     * @param theVdb the VDB on which to set the new state
     * @param newState the requested new state
     * @return 'true' if the requested state change is valid, 'false' if not.
     */
    private boolean isValidStateChange(VirtualDatabase theVDB, short newState) throws VirtualDatabaseException {
    	boolean isValid = false;

    	short currentState = theVDB.getStatus();
    	
    	// These state changes are always valid
    	if(    (currentState == VDBStatus.INACTIVE && newState == VDBStatus.ACTIVE) 
            || (currentState == VDBStatus.ACTIVE && newState == VDBStatus.INACTIVE)
        	|| (currentState == VDBStatus.ACTIVE_DEFAULT && newState == VDBStatus.INACTIVE)
        	|| (currentState == VDBStatus.ACTIVE_DEFAULT && newState == VDBStatus.ACTIVE) ) {
    		
    		return true;
    	}

    	// Changing state to active-default - there must not already be an active-default
    	if(    (currentState == VDBStatus.INACTIVE && newState == VDBStatus.ACTIVE_DEFAULT)
            	|| (currentState == VDBStatus.ACTIVE && newState == VDBStatus.ACTIVE_DEFAULT) ) {
        		
            Collection allVDBs = RuntimeMetadataCatalog.getInstance().getVirtualDatabases();

            // Change is valid if VDB does not already have Active-Default
    		isValid = !namedVdbHasActiveDefault(theVDB.getName(),allVDBs);
        }
    	
    	
    	return isValid;
    }
    
    
    /**
     * Helper method to determine if Vdb in the list with the supplied name has Active-Default
     *
     * @param vdbName the vdb name
     * @param vdbs collection of vdbs to search
     */
    private boolean namedVdbHasActiveDefault(String vdbName, Collection vdbs) {
    	boolean hasActiveDefaultStatus = false;

    	// Find highest version for each name - considered to be version with latest Date
    	Iterator vIter = vdbs.iterator();
    	while(vIter.hasNext()) {
    		VirtualDatabase vdb = (VirtualDatabase)vIter.next();
    		if(vdb.getName().equals(vdbName) && vdb.getStatus()==VDBStatus.ACTIVE_DEFAULT) {
    			hasActiveDefaultStatus = true;
    		}
    	}

    	return hasActiveDefaultStatus;
    }

    /**
     * Migrate as many existing entitlements as possible to a newly deployed VDB.
     * @param sourceVDB The existing VDB from which to copy entitlements.
     * @param targetVDB The new VDB to which to copy entitlements.
     * @return The report that contains a result for each attempted copy.
     * @throws InvalidSessionException if the administrative session is invalid
     * @throws AuthorizationException if admninistrator does not have the authority to perform the requested operation.
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    @RolesAllowed(value=AdminRoles.RoleName.ADMIN_PRODUCT)
    public EntitlementMigrationReport migrateEntitlements(VirtualDatabase sourceVDB,
                                                                          VirtualDatabase targetVDB)
        throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession();

        return RuntimeMetadataHelper.migrateEntitlements(sourceVDB, targetVDB, token);
    }
    
    /**
     * Migrate as many existing entitlements as possible to a newly deployed VDB from a supplied roles xml file.
     * @param sourceVDB The existing VDB from which to copy entitlements.
     * @param dataRoleContents The contents of the Data Roles xml File.
     * @param overwriteExisting flag to determine if existing entitlements should be overwritten.
     * @return The report that contains a result for each attempted copy.
     * @throws InvalidSessionException if the administrative session is invalid
     * @throws AuthorizationException if admninistrator does not have the authority to perform the requested operation.
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    @RolesAllowed(value=AdminRoles.RoleName.ADMIN_PRODUCT)
    public EntitlementMigrationReport migrateEntitlements(VirtualDatabase targetVDB,
                                                                          char[] dataRoleContents, boolean overwriteExisting)
		throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {
		// Validate caller's session
		SessionToken token = AdminAPIHelper.validateSession();
		
		return RuntimeMetadataHelper.migrateEntitlements(targetVDB.getVirtualDatabaseID(), dataRoleContents, overwriteExisting, token);
    }

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
    public PermissionDataNode getEntitlementTree(String vDBName,
                                                              String vDBVersion,
                                                              AuthorizationPolicyID policyID)
        throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        AdminAPIHelper.validateSession();
        // Any administrator may call this read-only method - no need to validate role

        LogManager.logDetail(
            ServerAdminLogConstants.CTX_RUNTIME_METADATA_ADMIN_API,
            "getEntitlementTree: Getting entitlements for policy: [" + policyID + "]"); //$NON-NLS-1$ //$NON-NLS-2$
        AuthorizationServiceInterface authProxy = PlatformProxyHelper.getAuthorizationServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);
        
        PermissionDataNode root = RuntimeMetadataHelper.getPermissionDataNodes(new AuthorizationRealm(vDBName, vDBVersion, null),
                                                                               policyID,
                                                                               authProxy);
        return root;
    }

    /**
     * Get the tree of data nodes that make op a VDB. This tree represents <i>just</i>
     * the data node hierarchy and does not contain authorization information.
     * @param vDBName The name of the VDB for which data nodes are sought.
     * @param vDBVersion The version of the VDB for which data nodes are sought.
     * @return The root of the data node tree for the VDB and version.
     * @throws InvalidSessionException if the administrative session is invalid
     * @throws AuthorizationException if admninistrator does not have the authority to perform the requested operation.
     * @throws MetaMatrixComponentException if this service has trouble communicating.
     */
    public PermissionDataNode getDataNodes(String vDBName, String vDBVersion)
        throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        AdminAPIHelper.validateSession();
        // Any administrator may call this read-only method - no need to validate role

        LogManager.logDetail(
            ServerAdminLogConstants.CTX_RUNTIME_METADATA_ADMIN_API,
            "getDataNodes: Getting data nodes for VDB: [" + vDBName + "] Vers: [" + vDBVersion + "]"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        PermissionDataNodeImpl root = RuntimeMetadataHelper.getDataNodes(vDBName, vDBVersion);

        return root;
    }

    /**
     * Get all data element paths for a VDB version.
     * @param callerSessionID The session ID of the administrator making the call.
     * @param vdbName The VDB name.
     * @param vdbVersion The version for the VDB.
     * @return All element paths in the given VDB version.
     */
    public Set getAllDataNodeNames(String vdbName, String vdbVersion)
        throws InvalidSessionException, MetaMatrixComponentException {
        // Validate caller's session
        AdminAPIHelper.validateSession();

        return new HashSet(RuntimeMetadataHelper.getAllDataNodeNames(vdbName, vdbVersion, new HashMap()));
    }

    /**
     * Call to export a VDB to the local file system
     *
     * @param vdbID ID of the VirtualDatabase.
     * @param dirLocation is the path where the files will be saved
     * @param vdbDefnFileName is the name of the VDB Definition file to be saved.
     * Visibility levels are defined in MetadataConstants.VISIBILITY_TYPES.
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws VirtualDatabaseException if an error occurs while setting the state.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    @RolesAllowed(value=AdminRoles.RoleName.ADMIN_PRODUCT)
    public byte[] getVDB(VirtualDatabaseID vdbID) 
    	throws AuthorizationException, InvalidSessionException,VirtualDatabaseException,MetaMatrixComponentException {

        VDBArchive vdbArchive = null;
        try {
            vdbArchive = VDBDefnFactory.createVDBArchive(vdbID.getName(), vdbID.getVersion());
            return VDBArchive.writeToByteArray(vdbArchive);
        } catch (VirtualDatabaseException e) {
            throw e;
        } catch (Exception e) {
            if (e instanceof AuthorizationException) {
                throw (AuthorizationException) e;
            }
            if (e instanceof InvalidSessionException) {
                throw (InvalidSessionException) e;
            }

            if (e instanceof MetaMatrixComponentException) {
                throw (MetaMatrixComponentException) e;
            }
            throw new MetaMatrixComponentException(e, RuntimeMetadataPlugin.Util.getString("RuntimeMetadataAdminAPIImpl.Error_getting_VDBDefn_for_{0}") + vdbID); //$NON-NLS-1$
        } finally {
        	if (vdbArchive != null) {
        		vdbArchive.close();
        	}
        }
    }

    @RolesAllowed(value=AdminRoles.RoleName.ADMIN_PRODUCT)
    public VirtualDatabase importVDB(byte[] vdbStream)
			throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException {

        // Validate caller's session
        SessionToken callerToken = AdminAPIHelper.validateSession();
        
        VDBArchive vdbArchive = null;
        try {
        	vdbArchive = new VDBArchive(new ByteArrayInputStream(vdbStream));
        	
        	// don't update bindings, the process from the client perspective is to prompt the user
        	// for which vms to deploy the bindings to
            return importVDBDefn(vdbArchive, callerToken.getUsername(), false, Collections.EMPTY_LIST);
             
        } catch (VirtualDatabaseException e) {
            throw e;
        } catch (Exception e) {
            if (e instanceof AuthorizationException) {
                throw (AuthorizationException) e;
            }
            if (e instanceof InvalidSessionException) {
                throw (InvalidSessionException) e;
            }

            if (e instanceof MetaMatrixComponentException) {
                throw (MetaMatrixComponentException) e;
            }
            
            Object[] params = new Object[] {vdbArchive.getName()};
            String msg = RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.admin_0008, params);
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.admin_0008, msg);
        } finally {
        	if (vdbArchive != null) {
        		vdbArchive.close();
        	}
        }
    }
    
    private VirtualDatabase importVDBDefn(VDBArchive vdb, String principal, boolean updateExistingBinding, List vms) throws Exception {
        VDBCreation vdbc = new VDBCreation();
        vdbc.setUpdateBindingProperties(updateExistingBinding);
        vdbc.setVMsToDeployBindings(vms);
        return vdbc.loadVDBDefn(vdb,principal);
    } 

    /**
     * Get the visibility levels for models in a given virtual database.
     *
     * @param vdbID ID of the VirtualDatabase.
     * @return Map of ModelIDs and String connector binding names.
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws VirtualDatabaseException if an error occurs while setting the state.
     * @throws MetaMatrixComponentException if an error occurs in communicating with a component.
     */
    public Map getModelVisibilityLevels(VirtualDatabaseID vdbID)
        throws AuthorizationException, InvalidSessionException, VirtualDatabaseException, MetaMatrixComponentException {

        // Validate caller's session
        AdminAPIHelper.validateSession();
        // Any administrator may call this read-only method - no need to validate role

        Collection models = RuntimeMetadataCatalog.getInstance().getModels(vdbID);
        Map modelIDsVLevels = new HashMap();
        Iterator iter = models.iterator();
        while (iter.hasNext()) {
            Model model = (Model) iter.next();
            modelIDsVLevels.put(model.getID(), new Short(model.getVisibility()));
        }

        return modelIDsVLevels;
    }

     /**
     * Returns a <code>Collection</code> of the <code>VirtualDatabase</code>s in the system
     * with certain VDBs that we don't want shown in the Console filtered.
     * This would include all virtual databases flagged as incomplete, active, inactive or deleted.
     * @param vdbsToFilter The set of VDB names we <i>don't</i> want to see.
     * @return a Collection of all <code>VirtualDatabase</code>s in the system.
     * @throws VirtualDatabaseException if an error occurs while setting the state.
     */
//    private static Collection filterVirtualDatabases(Set vdbsToFilter) throws VirtualDatabaseException {
//
//        Collection vdbs = RuntimeMetadataCatalog.getVirtualDatabases();
//
//        // Filter all the VDBs we don't want to see in the Console
//        Collection filteredVDBs = new ArrayList();
//        Iterator vdbItr = vdbs.iterator();
//        while (vdbItr.hasNext()) {
//            VirtualDatabase aVDB = (VirtualDatabase) vdbItr.next();
//            String aVDBName = aVDB.getName();
//            if (!vdbsToFilter.contains(aVDBName)) {
//                filteredVDBs.add(aVDB);
//            }
//        }
//        return filteredVDBs;
//    }

    /** 
     * @see com.metamatrix.server.admin.apiimpl.RuntimeMetadataAdminAPIInterface#validateConnectorBindingForMaterialization(ConnectorBinding)
     * @since 4.2
     */
    public boolean validateConnectorBindingForMaterialization(ConnectorBinding materializationConnector) throws AuthorizationException,
                                                                                          InvalidSessionException,
                                                                                          VirtualDatabaseException,
                                                                                          MetaMatrixComponentException {
// TODO: Implement validateConnectorBindingForMaterialization()
        // Criteria:
        // 1) Connector capabilities must support batched inserts
        // 2) Constrain types to be one of our supported JDBC types: Oracle, DB2, SQL Server
        // 3) Examine lossey semantics of datatype conversion between physical source and materialization platforms?
        return true;
    }

    /** 
     * @see com.metamatrix.server.admin.apiimpl.RuntimeMetadataAdminAPIInterface#getMaterializationScripts(com.metamatrix.common.config.api.ConnectorBinding, RMCVersionEntry, String, String, String, String, String, String)
     * @since 4.2
     */
    public MaterializationLoadScripts getMaterializationScripts(ConnectorBinding materializationConnector, VDBDefn vdb, 
                                                                String mmHost, String mmPort, 
                                                                String materializationUserName, String materializationUserPwd, String metamatrixUserName, String metamatrixPwd) 
                                                                                  throws AuthorizationException,
                                                                                  InvalidSessionException,
                                                                                  VirtualDatabaseException,
                                                                                  MetaMatrixComponentException {
        if ( vdb == null ) {
            throw new MetaMatrixRuntimeException(RuntimeMetadataPlugin.Util.getString("RuntimeMetadataAdminAPIImpl.VDB_null")); //$NON-NLS-1$
        }
        if ( materializationUserName == null || materializationUserName.trim().length() == 0 ) {
            throw new MetaMatrixRuntimeException(RuntimeMetadataPlugin.Util.getString("RuntimeMetadataAdminAPIImpl.materializationUserName_null")); //$NON-NLS-1$
        }
        if ( materializationUserPwd == null || materializationUserPwd.trim().length() == 0 ) {
            throw new MetaMatrixRuntimeException(RuntimeMetadataPlugin.Util.getString("RuntimeMetadataAdminAPIImpl.materializationUserPwd_null")); //$NON-NLS-1$
        }
        if ( metamatrixUserName == null || metamatrixUserName.trim().length() == 0 ) {
            throw new MetaMatrixRuntimeException(RuntimeMetadataPlugin.Util.getString("RuntimeMetadataAdminAPIImpl.metamatrixUserName_null")); //$NON-NLS-1$
        }
        if ( metamatrixPwd == null || metamatrixPwd.trim().length() == 0 ) {
            throw new MetaMatrixRuntimeException(RuntimeMetadataPlugin.Util.getString("RuntimeMetadataAdminAPIImpl.metamatrixPwd_null")); //$NON-NLS-1$
        }
        ModelInfo materializationModel = getMatertializationModel(vdb);
        if ( materializationModel == null ) {
            throw new MetaMatrixRuntimeException(RuntimeMetadataPlugin.Util.getString("RuntimeMetadataAdminAPIImpl.VDB_has_no_materialziation")); //$NON-NLS-1$
        }
        
        String vdbName = vdb.getName();
        int dotIndex = vdbName.indexOf('.');
        if ( dotIndex >= 0 ) {
            // remove extension if any
            vdbName = vdbName.substring(0, dotIndex);
        }
        String vdbVersion = vdb.getVersion();
        
        // Parse the required database type from selected connector binding props
        Properties connectorProps = materializationConnector.getProperties();
        String matURL = connectorProps.getProperty("URL"); //$NON-NLS-1$ // !!COPIED FROM JDBC Connector JDBCPropertyNames!!
        // Create properties file with connection information
        String matDriver = connectorProps.getProperty("Driver"); //$NON-NLS-1$ // !!COPIED FROM JDBC Connector JDBCPropertyNames!!
        
        if ( mmHost == null ) {
            throw new MetaMatrixRuntimeException(RuntimeMetadataPlugin.Util.getString("RuntimeMetadataAdminAPIImpl.MetaMatrix_host_null")); //$NON-NLS-1$
        }
        // Check mmPort argument
        try {
            Integer.parseInt(mmPort);
        } catch (NumberFormatException err) {
            Object[] params = new Object[] {mmPort};
            throw new MetaMatrixRuntimeException(RuntimeMetadataPlugin.Util.getString("RuntimeMetadataAdminAPIImpl.Expected_integer_for_port {0}", params)); //$NON-NLS-1$
        }
        
        String mmDriver = "com.metamatrix.jdbc.MMDriver"; //$NON-NLS-1$
        
        boolean useSSL = false;
        
//        try {
//            useSSL = SSLConfiguration.isSSLEnabled();
//            
//        } catch (Exception err) {
//             throw new MetaMatrixRuntimeException(RuntimeMetadataPlugin.Util.getString("RuntimeMetadataAdminAPIImpl.Unable_to_determine_ssl_mode")); //$NON-NLS-1$
//        }         
        
        // Encrypt connection props
        try {
            metamatrixPwd = PropertiesUtils.saveConvert(CryptoUtil.stringEncrypt(metamatrixPwd), false);
            materializationUserPwd = PropertiesUtils.saveConvert(CryptoUtil.stringEncrypt(materializationUserPwd), false);
        } catch (CryptoException err1) {
          final Object[] params = new Object[] {vdbName};
          throw new MetaMatrixRuntimeException(err1, RuntimeMetadataPlugin.Util.getString("RuntimeMetadataAdminAPIImpl.Unable_to_encrypt_pwd", params)); //$NON-NLS-1$
        }
        
        // Generate connection props and insert into scripts.
        MaterializationLoadScripts scripts = RuntimeMetadataHelper.createMaterializedViewLoadProperties(materializationModel, 
                                                                                                        matURL, matDriver, materializationUserName, 
                                                                                                       materializationUserPwd, mmHost, mmPort, mmDriver, useSSL, 
                                                                                                       metamatrixUserName, metamatrixPwd, vdbName, vdbVersion);        
        return scripts;
    }

    
    private ModelInfo getMatertializationModel(VDBDefn vdb) {
        ModelInfo matModel = null;
        Iterator modelItr = vdb.getModels().iterator();
        while ( modelItr.hasNext() ) {
            ModelInfo aModel = (ModelInfo)modelItr.next();
            if ( aModel != null && aModel.isMaterialization() ) {
                matModel = aModel;
                break;
            }
        }
        return matModel;
    }    
}