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

package com.metamatrix.console.models;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.admin.api.server.ServerAdmin;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.tree.TreeView;
import com.metamatrix.common.vdb.api.DEFReaderWriter;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.common.vdb.api.VDBDefn;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.security.UserCapabilities;
import com.metamatrix.console.ui.views.deploy.util.DeployPkgUtils;
import com.metamatrix.console.ui.views.vdb.WSDLOperationsDescription;
import com.metamatrix.console.util.StaticProperties;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.vdb.VDBStatus;
import com.metamatrix.metadata.runtime.api.Model;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.platform.admin.api.EntitlementMigrationReport;
import com.metamatrix.platform.admin.api.PermissionDataNode;
import com.metamatrix.platform.admin.api.PermissionDataNodeTreeView;
import com.metamatrix.platform.admin.apiimpl.PermissionDataNodeTreeViewImpl;
import com.metamatrix.platform.security.api.AuthorizationRealm;
import com.metamatrix.server.admin.api.MaterializationLoadScripts;

public class VdbManager extends Manager {

    public VdbManager(ConnectionInfo connection) {
        super(connection);
    }

    public boolean hasAnyModels(TreeView treeView) {
        return true;
    }

    public Collection /* <VirtualDatabase> */getVDBs() throws Exception {

        Collection colVdbs = ModelManager.getRuntimeMetadataAPI(getConnection()).getVirtualDatabases();
        
        return colVdbs;
    }

    public VirtualDatabase getVirtualDatabase(VirtualDatabaseID vdbID) throws Exception {

        VirtualDatabase vdb = ModelManager.getRuntimeMetadataAPI(getConnection()).getVirtualDatabase(vdbID);

        return vdb;
    }

    public Collection /* <Model> */getVdbModels(VirtualDatabaseID vdbID) throws Exception {
        Collection colModels = ModelManager.getRuntimeMetadataAPI(getConnection()).getVDBModels(vdbID);
        return colModels;
    }

    /**
     * Returns null if VDB not found, else Boolean. Else Boolean.TRUE or Boolean.FALSE.
     */
    public Boolean isVDBActive(String vdbName,
                               int vdbVersion) throws Exception {
        Boolean response = null;
        Collection /* <VirtualDatabase> */vdbs = null;
        try {
            vdbs = getVDBs();
        } catch (Exception ex) {
            throw ex;
        }
        Iterator it = vdbs.iterator();
        while (it.hasNext() && (response == null)) {
            VirtualDatabase vdb = (VirtualDatabase)it.next();
            String name = vdb.getName();
            if (name.equals(vdbName)) {
                VirtualDatabaseID id = (VirtualDatabaseID)vdb.getID();
                String versStr = id.getVersion();
                int vers = (new Integer(versStr)).intValue();
                if (vers == vdbVersion) {
                    short status = vdb.getStatus();
                    response = new Boolean( (status==VDBStatus.ACTIVE) || (status==VDBStatus.ACTIVE_DEFAULT) );
                }
            }
        }
        return response;
    }

    public void updateVirtualDatabase(VirtualDatabase vdb) throws Exception {
        ModelManager.getRuntimeMetadataAPI(getConnection()).updateVirtualDatabase(vdb);
    }
    
    public boolean vdbHasDataRoles(VirtualDatabaseID vid) throws Exception {
    	boolean hasDataRoles = false;

    	byte[] vdbStream = ModelManager.getRuntimeMetadataAPI(getConnection()).getVDB(vid);
        VDBArchive vdbArchive = null;
        try {
        	vdbArchive = new VDBArchive(new ByteArrayInputStream(vdbStream));
        	hasDataRoles = ( vdbArchive.getDataRoles()!=null );
        } finally {
        	if (vdbArchive != null) {
        		vdbArchive.close();
        	}
        }
    	return hasDataRoles;
    }

    public void setConnectorBindingNames(VirtualDatabaseID vdbId,
                                         Map /* <String model name to Collection of String UUID> */mapModelsToConnBinds) throws Exception {
        ModelManager.getRuntimeMetadataAPI(getConnection()).setConnectorBindingNames(vdbId, mapModelsToConnBinds);
    }

    public Map migrateConnectorBindingNames(VirtualDatabase vdbSourceVdb, VDBDefn vdb) throws Exception {
        Map mapModelsToBindings = ModelManager.getRuntimeMetadataAPI(getConnection()).migrateConnectorBindingNames(vdbSourceVdb, vdb);
        return mapModelsToBindings;
    }

    public Collection /* <VirtualDatabase> */getVDBsForConnectorBinding(String routingID) throws Exception {
        // for now, use this test data:
        if (routingID == null) {
            throw new Exception("Routing ID is NULL!"); //$NON-NLS-1$
        }
        Collection colVdbs = ModelManager.getRuntimeMetadataAPI(getConnection()).getVDBsForConnectorBinding(routingID);

        return colVdbs;
    }

    public void setVDBState(VirtualDatabaseID vdbID,
                            short siState) throws Exception {
        ModelManager.getRuntimeMetadataAPI(getConnection()).setVDBState(vdbID, siState);
    }

    public void markVDBForDelete(VirtualDatabaseID vdbID) throws Exception {
        ModelManager.getRuntimeMetadataAPI(getConnection()).markVDBForDelete(vdbID);
    }

    public EntitlementMigrationReport migrateEntitlements(VirtualDatabase vdbSource,
                                                          VirtualDatabase vdbTarget) throws Exception {
        EntitlementMigrationReport emrMigrateReport;
        emrMigrateReport = ModelManager.getRuntimeMetadataAPI(getConnection()).migrateEntitlements(vdbSource, vdbTarget);
        return emrMigrateReport;
    }
    
    public EntitlementMigrationReport importEntitlements(VirtualDatabase vdbSource, char[] dataRoleContents, boolean overwriteExisting) throws Exception {
		EntitlementMigrationReport emrMigrateReport;
		emrMigrateReport = ModelManager.getRuntimeMetadataAPI(getConnection()).migrateEntitlements(vdbSource, dataRoleContents,overwriteExisting);
		return emrMigrateReport;
	}

    public boolean modelSuppressed(String sModelName) {
        // boolean bSuppressThisModel = false;
        return false;
    }

    public boolean vdbSuppressed(String sVdbName) {
        // boolean bSuppressThisVdb = false;
        return false;
    }

    // public Collection getVdbsToSuppress() {
    // ArrayList arylVdbsToSuppress = new ArrayList();
    // arylVdbsToSuppress.add(MetadataConstants.RUNTIME_VDB.VDB_NAME);
    // return arylVdbsToSuppress;
    // }

    public String getVdbStatusAsString(short siStatus) {
        String sStatus = "Unknown"; //$NON-NLS-1$

        if (siStatus == VDBStatus.ACTIVE) {
            sStatus = "Active"; //$NON-NLS-1$
        } else if (siStatus == VDBStatus.INACTIVE) {
            sStatus = "Inactive"; //$NON-NLS-1$
        } else if (siStatus == VDBStatus.DELETED) {
            sStatus = "Deleted"; //$NON-NLS-1$
        } else if (siStatus == VDBStatus.INCOMPLETE) {
            sStatus = "Incomplete"; //$NON-NLS-1$
	    } else if (siStatus == VDBStatus.ACTIVE_DEFAULT) {
	        sStatus = "Active (Default)"; //$NON-NLS-1$
	    }
        return sStatus;
    }

    public Collection getVdbEntitlements(VirtualDatabaseID vdbID) throws Exception {
        AuthorizationRealm realm = new AuthorizationRealm(vdbID.getName(), vdbID.getVersion());
        Collection colEntitlementIDs = ModelManager.getAuthorizationAPI(getConnection()).getPolicyIDsInRealm(realm);
        return colEntitlementIDs;
    }

    public PermissionDataNodeTreeView getTreeViewForVDB(String vdbName,
                                                        String vdbVersion) throws Exception {
        PermissionDataNode root = ModelManager.getRuntimeMetadataAPI(getConnection()).getDataNodes(vdbName, vdbVersion);
        PermissionDataNodeTreeView treeView = new PermissionDataNodeTreeViewImpl(root);
        return treeView;
    }

    public int getHighestNumberedVDBVersion(String vdbName) throws Exception {
        int highest = -1;
        VirtualDatabase vdb = getLatestVDBVersion(vdbName);
        if (vdb != null) {
            VirtualDatabaseID id = (VirtualDatabaseID)vdb.getID();
            String curVersionStr = id.getVersion();
            highest = (new Integer(curVersionStr)).intValue();
        }
        return highest;

    }

    public VirtualDatabase getLatestVDBVersion(String vdbName) throws Exception {
        return ModelManager.getRuntimeMetadataAPI(getConnection()).getLatestVirtualDatabase(vdbName);
    }

    public Object[] importVDB(VDBArchive vdbArchive, boolean importRoles) throws Exception {
    	Object[] result = new Object[2];

    	// Import the VDB
        VirtualDatabase vdb = ModelManager.getRuntimeMetadataAPI(getConnection()).importVDB(VDBArchive.writeToByteArray(vdbArchive));

        // check if there are data roles associated with the VDB, if there are then
        // migrate them.
        EntitlementMigrationReport entReport = null;
        if (importRoles && vdbArchive.getDataRoles() != null) {
        	entReport = importEntitlements(vdb,vdbArchive.getDataRoles(),true);
        }             
        result[0] = vdb;
        result[1] = entReport;
        return result;
    }
    
    public void exportVDB(VirtualDatabase vdb, String vdbFileName, String parentDir) throws Exception {
    	    	
        VirtualDatabaseID vid = (VirtualDatabaseID)vdb.getID();
        byte[] vdbStream = ModelManager.getRuntimeMetadataAPI(getConnection()).getVDB(vid);

        Properties headerProps = new Properties();

        String userName = UserCapabilities.getLoggedInUser(getConnection()).getName();
        String version = StaticProperties.getVersions() + ":" + StaticProperties.getBuild(); //$NON-NLS-1$

        headerProps.put(DEFReaderWriter.Header.APPLICATION_CREATED_BY, DeployPkgUtils.getString("dmp.console.name")); //$NON-NLS-1$
        headerProps.put(DEFReaderWriter.Header.APPLICATION_VERSION, version);
        headerProps.put(DEFReaderWriter.Header.USER_CREATED_BY, userName);
        
        File archiveFile = null;
        if (parentDir != null ) {
            archiveFile = new File(parentDir, vdbFileName);
        } else {
            archiveFile = new File(vdbFileName);
        }
        
        ServerAdmin admin = getConnection().getServerAdmin();
        char[] rolesContent = admin.exportDataRoles(vid.getName(), vid.getVersion());
        
        VDBArchive vdbArchive = null;
        try {
        	vdbArchive = new VDBArchive(new ByteArrayInputStream(vdbStream));
        	vdbArchive.updateRoles(rolesContent);
        	vdbArchive.getConfigurationDef().setHeaderProperties(headerProps);
        	FileUtils.write(vdbArchive.getInputStream(), archiveFile);
        } finally {
        	if (vdbArchive != null) {
        		vdbArchive.close();
        	}
        }
    }

    public WSDLOperationsDescription getWSDLOperationsDescription(VirtualDatabase vdb) {
        WSDLOperationsDescription opsDesc = null;
        if (vdb.hasWSDLDefined()) {
            opsDesc = new WSDLOperationsDescription(vdb);
        }
        return opsDesc;
    }

    public ModelInfo getMaterializationTableModel(VDBDefn vdb) {
        ModelInfo model = null;
        Collection /* <ModelInfo> */models = vdb.getModels();
        Iterator it = models.iterator();
        while ((model == null) && it.hasNext()) {
            ModelInfo curModel = (ModelInfo)it.next();
            if (curModel.isMaterialization()) {
                model = curModel;
            }
        }
        return model;
    }
    
    public MaterializationLoadScripts getMaterializationScripts(ConnectorBinding materializationConnector,
                                                                VDBDefn vdb,
                                                                String mmHost,
                                                                String mmPort,
                                                                String materializationUserName,
                                                                String materializationUserPwd,
                                                                String metamatrixUserName,
                                                                String metamatrixUserPwd) throws Exception {
        return ModelManager.getRuntimeMetadataAPI(getConnection()).getMaterializationScripts(materializationConnector,
                                                                                             vdb,
                                                                                             mmHost,
                                                                                             mmPort,
                                                                                             materializationUserName,
                                                                                             materializationUserPwd,
                                                                                             metamatrixUserName,
                                                                                             metamatrixUserPwd);
    }
    
    /**
     *  return true if the specified VirtualDatabase has any materialization models.
     * @since 4.3
     */
    public boolean hasMaterializationModels(VirtualDatabase virtualDatabase) throws Exception {
        Collection models = getVdbModels(virtualDatabase.getVirtualDatabaseID());
        for (Iterator iter = models.iterator(); iter.hasNext(); ) {
            Model model = (Model) iter.next();
            if (model.isMaterialization()) {
                return true;
            }
        }
        
        return false;
    }
    
    
}