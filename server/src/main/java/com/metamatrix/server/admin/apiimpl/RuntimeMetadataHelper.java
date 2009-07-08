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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.adminapi.AdminOptions;
import org.teiid.metadata.RuntimeMetadataPlugin;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationMgmtException;
import com.metamatrix.common.jdbc.JDBCPlatform;
import com.metamatrix.common.jdbc.JDBCPlatform.Supported;
import com.metamatrix.common.tree.basic.BasicTreeNode;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.id.IDGenerator;
import com.metamatrix.core.id.LongID;
import com.metamatrix.core.id.LongIDFactory;
import com.metamatrix.core.id.ObjectIDFactory;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.core.vdb.VDBStatus;
import com.metamatrix.metadata.runtime.RuntimeMetadataCatalog;
import com.metamatrix.metadata.runtime.VDBTreeUtility;
import com.metamatrix.metadata.runtime.api.Model;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseDoesNotExistException;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseException;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseMetadata;
import com.metamatrix.metadata.util.ErrorMessageKeys;
import com.metamatrix.platform.admin.api.EntitlementMigrationReport;
import com.metamatrix.platform.admin.api.PermissionDataNode;
import com.metamatrix.platform.admin.api.PermissionDataNodeDefinition;
import com.metamatrix.platform.admin.apiimpl.PermissionDataNodeDefinitionImpl;
import com.metamatrix.platform.admin.apiimpl.PermissionDataNodeImpl;
import com.metamatrix.platform.security.api.AuthorizationPolicyFactory;
import com.metamatrix.platform.security.api.AuthorizationPolicyID;
import com.metamatrix.platform.security.api.AuthorizationRealm;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.security.api.StandardAuthorizationActions;
import com.metamatrix.platform.security.api.service.AuthorizationServiceInterface;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.service.api.exception.ServiceNotFoundException;
import com.metamatrix.platform.util.PlatformProxyHelper;
import com.metamatrix.server.admin.api.MaterializationLoadScripts;
import com.metamatrix.vdb.materialization.DatabaseDialect;
import com.metamatrix.vdb.materialization.MaterializedViewScriptGenerator;
import com.metamatrix.vdb.materialization.MaterializedViewScriptGeneratorImpl;
import com.metamatrix.vdb.materialization.ScriptType;
import com.metamatrix.vdb.materialization.template.MaterializedViewConnectionData;

/**
 * A collection of static methods that facilitate the server-side remote class
 * RuntimeMetadataAdminAPIImpl.
 */
public class RuntimeMetadataHelper {

    public static final char PATH_SEPERATOR_CHAR = '.';

    public static final short VDB_STATE_INCOMPLETE = 0;
    public static final short VDB_STATE_INACTIVE = 1;
    public static final short VDB_STATE_ACTIVE = 2;
    public static final short VDB_STATE_DELETED = 4;
    public static final short VDB_STATE_ACTIVE_DEFAULT = 5;

    public static short[] mapVDBStates(short stateFlag) {
        short[] states = new short[5];
        if ((stateFlag & VDB_STATE_INCOMPLETE) == 0) {
            states[0] = VDBStatus.INCOMPLETE;
        } else {
            states[0] = -1;
        }
        if ((stateFlag & VDB_STATE_INACTIVE) == 0) {
            states[1] = VDBStatus.INACTIVE;
        } else {
            states[1] = -1;
        }
        if ((stateFlag & VDB_STATE_ACTIVE) == 0) {
            states[2] = VDBStatus.ACTIVE;
        } else {
            states[2] = -1;
        }
        if ((stateFlag & VDB_STATE_DELETED) == 0) {
            states[3] = VDBStatus.DELETED;
        } else {
            states[3] = -1;
        }
        if ((stateFlag & VDB_STATE_ACTIVE_DEFAULT) == 0) {
            states[4] = VDBStatus.ACTIVE_DEFAULT;
        } else {
            states[4] = -1;
        }
        return states;
    }

    /**
     * Given a VDB, get its previous version of a given state.<br></br>
     * <strong>Note: The <code>stateFlag</code> paremeter is currently meaningless
     * and has no effect on the algorithm.  All VDB ancesters of state INCOMPLETE,
     * INACTIVE or ACTIVE will be returned</strong>.
     * @param vdbName The VDB to get the previous version.
     * @param vers The current version of the given VDB. We'll be looking for the one before this.
     * @param stateFlag Only interested in VDBs of state(s).
     * @return The given VDBs previos version or null if previous DNE.
     */
    public static VirtualDatabase walkBack(String vdbName, int vers, short stateFlag) throws VirtualDatabaseException {
        // If we're at version 0, we've run out of VDB ancesters
// FIXME: Need to know from Runtime Metadata the initial version of all VDBs
// when created so that I can stop walking back - (public constant value?)
        if (vers > 1) {
            --vers;
        } else {
            return null;
        }

// DEBUG:
//System.out.println(" *** RuntimeMetadataHelper.walkback(): Looking for VDB version <" + vers + ">");
        // Get previous VDB version, skipping any deleted version(s)
        VirtualDatabaseID previousID = null;
        VirtualDatabase previousVDB = null;
        try {
            previousID = RuntimeMetadataCatalog.getInstance().getVirtualDatabaseID(vdbName, String.valueOf(vers));
            previousVDB = RuntimeMetadataCatalog.getInstance().getVirtualDatabase(previousID);
        } catch (VirtualDatabaseDoesNotExistException e) {
            // This version doesn't exist, skip it.
// DEBUG:
//System.out.println(" *** RuntimeMetadataHelper.walkback(): VDB exception <" + e.getMessage() + ">");
        }

        // previousVDB will be null if non existant
        if (previousVDB != null) {
// DEBUG:
//System.out.println(" *** RuntimeMetadataHelper.walkback(): VDB is NOT null");
            short state = previousVDB.getStatus();
            // Be nice if we could use a bit mask here...
            if (state == VDBStatus.INCOMPLETE ||
                    state == VDBStatus.INACTIVE ||
                    state == VDBStatus.ACTIVE ||
                    state == VDBStatus.ACTIVE_DEFAULT) {
                return previousVDB;
            }
        }

// DEBUG:
//System.out.println(" *** RuntimeMetadataHelper.walkback(): Recursing. Current version <" + vers + ">");
        // Recurse until we find one in given state or we run out
        return walkBack(vdbName, vers, stateFlag);
    }

    /**
     * Aquire all data node full names from the runtime catalog.
     * @param vdbID The VDB from which to get the node names.
     * @return The sorted <strong>flat</strong> list of fully-qualified path names
     * for all data nodes in the given VDB.
     * @throws MetaMatrixComponentException if an error occurs in the runtime
     * catalog.
     */
    public static List getAllDataNodeNames(VirtualDatabaseID vdbID, Map modelNameToModelMap)
            throws MetaMatrixComponentException {
        VirtualDatabaseMetadata vdbMeta = null;

        try {
            vdbMeta = RuntimeMetadataCatalog.getInstance().getVirtualDatabaseMetadata(vdbID);
        } catch (VirtualDatabaseException e) {
            String msg = RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.admin_0051, new Object[]{vdbID});
            throw  new MetaMatrixComponentException(e, ErrorMessageKeys.admin_0051, msg);
        }
        
        // Get all the Models from VDB metadata
        Collection models = Collections.EMPTY_LIST;
        try {
            models = vdbMeta.getDisplayableModels();
        } catch (VirtualDatabaseException e) {
            String msg = RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.admin_0053, new Object[]{vdbID});
            throw new MetaMatrixComponentException(e,ErrorMessageKeys.admin_0053,msg);
        }    
        
        List permissionNodeNames = Collections.EMPTY_LIST;
        

        if (models == null || models.isEmpty()) { 
            return permissionNodeNames;
        }
        
        // Get paths (full names) for all data nodes in the system
        try {
            permissionNodeNames = vdbMeta.getALLPaths(models);
        } catch (VirtualDatabaseException e) {
            String msg = RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.admin_0052, new Object[]{vdbID});
            throw new MetaMatrixComponentException(e,msg);
        }


        // Create a Map of ModelName->Model for quick lookup
        Iterator modelItr = models.iterator();
        while (modelItr.hasNext()) {
            Model aModel = (Model) modelItr.next();
            modelNameToModelMap.put(aModel.getName(), aModel);
        }
        if (modelNameToModelMap.size() == 0) {
            String msg = RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.admin_0053, new Object[]{vdbID});
            throw new MetaMatrixComponentException(ErrorMessageKeys.admin_0053,msg);
        }

        return permissionNodeNames;
    }

    /**
     * A recursive method that gets the list of data node names from the given data node tree.
     * @param treeRoot The root of the data node (sub)tree.
     * @param nodeNames The list of all data node names found in the tree collected during recursion.
     */
    public static void getDataNodeNames(PermissionDataNodeImpl treeRoot, List nodeNames) {
        List childNodes = treeRoot.getChildren();
        Iterator childItr = childNodes.iterator();
        while (childItr.hasNext()) {
            PermissionDataNodeImpl aNode = (PermissionDataNodeImpl) childItr.next();
            nodeNames.add(aNode.getResourceName());
            getDataNodeNames(aNode, nodeNames);
        }
    }

    /**
     * Aquire all data node full names from the runtime catalog.
     * @param vdbName The VDB name from which to get the node names.
     * @param vdbVersion The VDB version from which to get the node names.
     * @return The sorted <strong>flat</strong> list of fully-qualified path names
     * for all data nodes in the given VDB.
     * @throws MetaMatrixComponentException if an error occurs in the runtime
     * catalog.
     */
    public static List getAllDataNodeNames(String vdbName, String vdbVersion, Map modelNameToModelMap)
            throws MetaMatrixComponentException {
        VirtualDatabaseID vdbID = getVDBID(vdbName, vdbVersion);

        return RuntimeMetadataHelper.getAllDataNodeNames(vdbID, modelNameToModelMap);
    }

    /**
     * Get the tree of data nodes that make op a VDB. This tree represents <i>just</i>
     * the data node hierarchy and does not contain authorization information.
     * @param vdbName The name of the VDB for which data nodes are sought.
     * @param vdbVersion The version of the VDB for which data nodes are sought.
     * @return The root of the data node tree for the VDB and version.
     */
    public static PermissionDataNodeImpl getDataNodes(String vdbName,
                                                      String vdbVersion)
            throws MetaMatrixComponentException {
        // Get VDBID for vdbName and vdbVersion
//        VirtualDatabaseID vdbID = null;
//        try {
//            vdbID = RuntimeMetadataCatalog.getVirtualDatabaseID(vdbName, vdbVersion);
//        } catch (VirtualDatabaseException e) {
//            String msg = RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.admin_0054,
//                                                     new Object[]{vdbName, vdbVersion});
//            MetaMatrixComponentException e2 = new MetaMatrixComponentException(e, ErrorMessageKeys.admin_0054,
//                                                                               msg);
//            RuntimeMetadataPlugin.Util.log(IStatus.ERROR, e2, msg);
//            throw e2;
//        }
//
//        // Get VDBMetadata for vdbID
//        VirtualDatabaseMetadata vDBMetadata = null;
//        try {
//            vDBMetadata = RuntimeMetadataCatalog.getVirtualDatabaseMetadata(vdbID);
//        } catch (VirtualDatabaseException e) {
//			String msg = RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.admin_0051, new Object[]{vdbID});
//            MetaMatrixComponentException e2 = new MetaMatrixComponentException(e, ErrorMessageKeys.admin_0051,
//                                                                               msg);
//            RuntimeMetadataPlugin.Util.log(IStatus.ERROR, e2, msg);
//            throw e2;
//        }

        ObjectIDFactory idFactory = IDGenerator.getInstance().getFactory(LongID.PROTOCOL);
        if ( idFactory == null ) {
            IDGenerator.getInstance().addFactory(new LongIDFactory());
            idFactory = IDGenerator.getInstance().getFactory(LongID.PROTOCOL);
        }

        // Build initial tree of PermissionDataNodes with no permissions
        // This node serves as the "root" of all paths in the tree
        PermissionDataNodeImpl root = new PermissionDataNodeImpl(null,
                StandardAuthorizationActions.NONE,
                new PermissionDataNodeDefinitionImpl("root", "root", PermissionDataNodeDefinition.TYPE.UNKOWN), //$NON-NLS-1$ //$NON-NLS-2$
                false,
                idFactory.create());
        try {
            buildVDBTree(vdbName, vdbVersion, root, idFactory);
            
//            buildDataNodeTree(root, idFactory, vDBMetadata);
        } catch (VirtualDatabaseException e) {
            String msg = RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.admin_0053, new Object[] {vdbName});
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.admin_0053,msg);
        }


        // The whole runtime cache was loaded for this VDB... purge
        return root;
    }

    
    /**
     * Call to add the VDB metadata tree to the root node.
     * @param vdbName name of the vdb
     * @param vdbVersion version of the vdb
     * @param rootNode is the tree root node to start adding the metadata nodes
     * @param idFactory is the factory to use to generate the unique ids for each node
     *  
     */
    private static synchronized void buildVDBTree(String vdbName, String vdbVersion, BasicTreeNode rootNode,ObjectIDFactory idFactory) throws VirtualDatabaseException {

        VirtualDatabaseID vdbID = RuntimeMetadataCatalog.getInstance().getVirtualDatabaseID(vdbName, vdbVersion);

        VirtualDatabaseMetadata vDBMetadata = RuntimeMetadataCatalog.getInstance().getVirtualDatabaseMetadata(vdbID);
        
        VDBTreeUtility.buildDataNodeTree(rootNode, idFactory, vDBMetadata);
    } 
    
    
    /**
     * Method to provide the task of finding all data nodes in realm, finding
     * any and all permissions assigned to them, uniting these and returning the
     * united objects.
     * @param realm The realm in which to look for data nodes and their permissions.
     * @param policyID The <code>AuthorizationPolicyID</code> for which to check for
     * permissions on data nodes.
     * @param authSvcProxy A proxy to the AuthorizationService.
     * @return The root of the permission tree for the given policy.
     */
    public static PermissionDataNode getPermissionDataNodes(AuthorizationRealm realm,
                                                       AuthorizationPolicyID policyID,
                                                       AuthorizationServiceInterface authSvcProxy) throws MetaMatrixComponentException {
        // VDB Name
        String vdbName = realm.getSuperRealmName();
        // VDB Version
        String vdbVersion = realm.getSubRealmName();

//        // Get VDBID for vdbName and vdbVersion
//        VirtualDatabaseID vdbID = null;
//        try {
//            vdbID = RuntimeMetadataCatalog.getVirtualDatabaseID(vdbName, vdbVersion);
//        } catch (VirtualDatabaseException e) {
//			String msg = RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.admin_0054, new Object[]{vdbName, vdbVersion});
//            MetaMatrixComponentException e2 = new MetaMatrixComponentException(e, ErrorMessageKeys.admin_0054,
//                                                                               msg);
//            RuntimeMetadataPlugin.Util.log(IStatus.ERROR, e2, msg);
//            throw e2;
//        }
//
//        // Get VDBMetadata for vdbID
//        VirtualDatabaseMetadata vDBMetadata = null;
//        try {
//            vDBMetadata = RuntimeMetadataCatalog.getVirtualDatabaseMetadata(vdbID);
//        } catch (VirtualDatabaseException e) {
//            String msg = RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.admin_0051, new Object[]{vdbID});
//            MetaMatrixComponentException e2 = new MetaMatrixComponentException(e, ErrorMessageKeys.admin_0051,
//                                                                               msg);
//            RuntimeMetadataPlugin.Util.log(IStatus.ERROR, e2, msg);
//            throw e2;
//        }

        ObjectIDFactory idFactory = IDGenerator.getInstance().getFactory(LongID.PROTOCOL);
        if ( idFactory == null ) {
            IDGenerator.getInstance().addFactory(new LongIDFactory());
            idFactory = IDGenerator.getInstance().getFactory(LongID.PROTOCOL);
        }

        // Build initial tree of PermissionDataNodes with no permissions
        // This node serves as the "root" of all paths in the tree
        PermissionDataNodeImpl root = new PermissionDataNodeImpl(null,
                StandardAuthorizationActions.NONE,
                new PermissionDataNodeDefinitionImpl("root", "root", PermissionDataNodeDefinition.TYPE.UNKOWN), //$NON-NLS-1$ //$NON-NLS-2$
                false,
                idFactory.create());
                
        try {
            buildVDBTree(vdbName, vdbVersion, root, idFactory);
//            buildDataNodeTree(root, idFactory, vDBMetadata);
        } catch (VirtualDatabaseException e) {
            String msg = RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.admin_0053, new Object[] {vdbName});
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.admin_0053,msg);
        }
        
        PermissionDataNode permissionNode = root;
        try {
            permissionNode = authSvcProxy.fillPermissionNodeTree(root, policyID);
        } catch (AuthorizationMgmtException e) {
            String msg = RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.admin_0055);
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.admin_0055,msg);
        }
        return permissionNode;
    }
    
    public static VirtualDatabaseID getVDBID(String vdbName, String vdbVersion) 
    throws MetaMatrixComponentException {

        VirtualDatabaseID vdbID = null;
        try {
            vdbID = RuntimeMetadataCatalog.getInstance().getVirtualDatabaseID(vdbName, vdbVersion);
        } catch (VirtualDatabaseException e) {
            String msg = RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.admin_0054, new Object[]{vdbName, vdbVersion});
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.admin_0054,msg);
        }
        return vdbID;
    }

    
    public static EntitlementMigrationReport migrateEntitlements(VirtualDatabaseID tvdbID)
    throws MetaMatrixComponentException {

        int currentVersion = new Integer(tvdbID.getVersion()).intValue();
        // if theres no previous version, then theres nothing
        // to migrate from
        if (currentVersion <= 1) {
            return null;
        }
        int previousVersion = currentVersion - 1;
        
        String prevV = String.valueOf(previousVersion); 
        VirtualDatabaseID svdbID = null;           

        try {
            svdbID = RuntimeMetadataCatalog.getInstance().getVirtualDatabaseID(tvdbID.getFullName(), prevV);
        } catch (VirtualDatabaseException e) {
        }    
        // if the vdb isn't found then no migration can be done        
        if (svdbID == null) {
            return null;
        }    
        
        VirtualDatabase tvdb = null;
        VirtualDatabase svdb = null;

        try {
             svdb = RuntimeMetadataCatalog.getInstance().getVirtualDatabase(svdbID);
        } catch (VirtualDatabaseException e) {
            String msg = RuntimeMetadataPlugin.Util.getString("RuntimeMetadataHelper.VDB_is_not_found", new Object[]{svdbID.toString()});//$NON-NLS-1$
            throw new MetaMatrixComponentException(e, msg);
        }
        
        try {
             tvdb = RuntimeMetadataCatalog.getInstance().getVirtualDatabase(tvdbID);
        } catch (VirtualDatabaseException e) {
            String msg = RuntimeMetadataPlugin.Util.getString("RuntimeMetadataHelper.VDB_is_not_found", new Object[]{svdbID.toString()});//$NON-NLS-1$
            throw new MetaMatrixComponentException(e, msg);
        }
        SessionToken token =  new SessionToken();

        return RuntimeMetadataHelper.migrateEntitlements(svdb, tvdb, token);

    }
    
    public static EntitlementMigrationReport migrateEntitlements(VirtualDatabaseID tvdbID, char[] dataRoleContents, boolean overwriteExisting, SessionToken session) throws MetaMatrixComponentException {

    	// Set the options based on the passed in boolean flag
    	AdminOptions options = null;
    	if(overwriteExisting) {
    		options = new AdminOptions(AdminOptions.OnConflict.OVERWRITE);
    	} else {
    		options = new AdminOptions(AdminOptions.OnConflict.IGNORE);
    	}

    	String vdbName = tvdbID.getName();
    	String vdbVersion = tvdbID.getVersion();
    	
        try {
    	
	        Collection roles = AuthorizationPolicyFactory.buildPolicies(vdbName, vdbVersion, dataRoleContents);
	        
	        EntitlementMigrationReport rpt = new EntitlementMigrationReport("from file", vdbName + " " + vdbVersion); //$NON-NLS-1$ //$NON-NLS-2$
	        
	        Set allPaths = new HashSet(RuntimeMetadataHelper.getAllDataNodeNames(vdbName, vdbVersion, new HashMap()));
	        
	        getAuthorizationService().migratePolicies(session, rpt, vdbName, vdbVersion, allPaths, roles, options);
	        return rpt;
        
        } catch (Exception e) {
            String msg = RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.admin_0006);
            throw  new MetaMatrixComponentException(e, ErrorMessageKeys.admin_0006, msg);
        }        
    }
    
    public static EntitlementMigrationReport migrateEntitlements(
        VirtualDatabase sourceVDB,
        VirtualDatabase targetVDB,
        SessionToken token)
        throws MetaMatrixComponentException {

        // Entitlement Migration Rpt
        EntitlementMigrationReport rpt = null;
        
        try {

            // Initialize source and target
            VirtualDatabaseID sourceVDBID = sourceVDB.getVirtualDatabaseID();
            String sourceNameVersion = sourceVDBID.getName() + " " + sourceVDBID.getVersion(); //$NON-NLS-1$
            AuthorizationRealm sourceRealm =
                new AuthorizationRealm(sourceVDBID.getName(), sourceVDBID.getVersion(), null);

            VirtualDatabaseID targetVDBID = targetVDB.getVirtualDatabaseID();
            String targetNameVersion = targetVDBID.getName() + " " + targetVDBID.getVersion(); //$NON-NLS-1$
            
            // Create and initialize the report
            rpt = new EntitlementMigrationReport(sourceNameVersion, targetNameVersion);

            AuthorizationServiceInterface authProxy = getAuthorizationService();
            
            // Get all Policies from source
            Collection sourcePolicies = Collections.EMPTY_LIST;
            try {
	            try {
	                sourcePolicies = authProxy.getPoliciesInRealm(token, sourceRealm);
	            } catch(ServiceNotFoundException se){
	            	//the registry may not have been updated yet
	            	Thread.sleep(5000);
	            	sourcePolicies = authProxy.getPoliciesInRealm(token, sourceRealm);
	            } 
            } catch (Exception e) {
                String msg = RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.admin_0004,new Object[]{sourceRealm, e.getMessage()});
                throw  new MetaMatrixComponentException(e,ErrorMessageKeys.admin_0004,msg);
            }

            Set targetNodes = new HashSet(RuntimeMetadataHelper.getAllDataNodeNames(targetVDBID.getName(), targetVDBID.getVersion(), new HashMap()));
            
            authProxy.migratePolicies(token, rpt, targetVDBID.getName(), targetVDBID.getVersion(), targetNodes, sourcePolicies, new AdminOptions(AdminOptions.OnConflict.OVERWRITE));
            
            Set sourceNodes = new HashSet(RuntimeMetadataHelper.getAllDataNodeNames(sourceVDBID.getName(), sourceVDBID.getVersion(), new HashMap()));

            // Check for newly added resources
            Set newResources = new HashSet(targetNodes);
            newResources.removeAll(sourceNodes);
            if (newResources.size() > 0) {
                Iterator newResourceItr = newResources.iterator();
                while (newResourceItr.hasNext()) {
                    if (rpt != null) {
                        rpt.addResourceEntry(RuntimeMetadataPlugin.Util.getString("RuntimeMetadataAdminAPIImpl.Failed_migration"), //$NON-NLS-1$
                        		newResourceItr.next(),
                        		"", //$NON-NLS-1$
                        		"", //$NON-NLS-1$
                                StandardAuthorizationActions.NONE_LABEL,
                                RuntimeMetadataPlugin.Util.getString("RuntimeMetadataAdminAPIImpl.This_resource_exists_in_the_target_VDB_but_not_in_the_source_VDB.")); //$NON-NLS-1$
                    }
                }
            }

        } catch (Exception e) {
            String msg = RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.admin_0006);
            throw  new MetaMatrixComponentException(e, ErrorMessageKeys.admin_0006, msg);
        }

        return rpt;
    }

	private static AuthorizationServiceInterface getAuthorizationService() throws ServiceException {
		// Get proxy for Authorizartion service
		AuthorizationServiceInterface authProxy = PlatformProxyHelper.getAuthorizationServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);
		return authProxy;
	}

    /** 
     * Generate connection properties for materialization model in given VDB. Generated props will
     * be inserted into <code>scripts</code> to be given back to Console admin to save. 
     * @param vdbInfo TODO
     * @param matURL
     * @param matDriver
     * @param materializationUserName
     * @param materializationUserPwd
     * @param mmHost
     * @param mmPort
     * @param mmDriver
     * @param metamatrixUserName
     * @param metamatrixPwd
     * @param vdbName TODO
     * @param vdbVersion TODO
     * @return TODO
     * @since 4.2
     */
    public static MaterializationLoadScripts createMaterializedViewLoadProperties(ModelInfo materializationModel,
                                                                                  String matURL, String matDriver,
                                                                                  String materializationUserName,
                                                                                  String materializationUserPwd,
                                                                                  String mmHost, String mmPort,
                                                                                  String mmDriver,
                                                                                  boolean useSSL,
                                                                                  String metamatrixUserName,
                                                                                  String metamatrixPwd,
                                                                                  String vdbName, String vdbVersion) {
      
      // Determine next vdb version for this VDB
      String nextVersion = getNextVDBVersion(vdbName, vdbVersion);
      
      return createMaterializedViewLoadPropertiesVersion( materializationModel,
                                                          matURL,  matDriver,
                                                          materializationUserName,
                                                          materializationUserPwd,
                                                          mmHost,  mmPort,
                                                          mmDriver,
                                                          useSSL,
                                                          metamatrixUserName,
                                                          metamatrixPwd,
                                                          vdbName, nextVersion );
  }
    
    public static MaterializationLoadScripts createMaterializedViewLoadPropertiesVersion(ModelInfo materializationModel,
                                                                                  String matURL, String matDriver,
                                                                                  String materializationUserName,
                                                                                  String materializationUserPwd,
                                                                                  String mmHost, String mmPort,
                                                                                  String mmDriver,
                                                                                  boolean useSSL,
                                                                                  String metamatrixUserName,
                                                                                  String metamatrixPwd,
                                                                                  String vdbName, String vdbVersion) {
 
        // Connection properties file name pattern: "Connection_vdbName_vdbVersion.properties"
        String propFileName = ScriptType.connectionPropertyFileName(vdbName, vdbVersion);
        // Script log file name pattern: "Connection_vdbName_vdbVersion.log"
        String logFileName = ScriptType.logFileName(vdbName, vdbVersion);
        
      // Parse the required database type from URL
        ByteArrayOutputStream oStream = null;
        MaterializationLoadScriptsImpl scripts = null;
        try {        
          String databaseType = parseDatabaseType(matURL, matDriver, materializationUserName, materializationUserPwd); 
          if ( databaseType == null ) {
              throw new MetaMatrixRuntimeException(RuntimeMetadataPlugin.Util.getString("RuntimeMetadataAdminAPIImpl.Connector_has_no_URL")); //$NON-NLS-1$
          }
          
          // baes on the db type from URL get the registered dialect, if found use
          // the driver designated to be used with that data source.
          if (matDriver == null || matDriver.length() == 0) {
              DatabaseDialect dbDialect = DatabaseDialect.getDatabaseDialect(databaseType);
              if (dbDialect != null) {
                  matDriver = dbDialect.getDriverClassname();
              }
          }
          
       
         
          scripts = new MaterializationLoadScriptsImpl();
          // DDL file names will be of pattern: "<action_>vdbName_version.DDL", where "action" is one of "Create_", 
          // "Truncate_", "Load_" and "Swap_" and "version" is integer VDB version.
          // This is the common end of file name. Actions are filled in as files are added to MaterializationLoadScripts
          // Get DDL scripts out of VDB for particular DBMS platform
          String[] scriptFileNames = materializationModel.getDDLFileNames();
          // FIXME: Defect 14911 - SQLServer and SQL_Server don't match
          final String SQL_SERVER_STR = "sql_server"; //$NON-NLS-1$
          for ( int i=0; i<scriptFileNames.length; ++i ) {
              String aScriptFileName = scriptFileNames[i];
              // FIXME: Defect 14911 - SQLServer and SQL_Server don't match
              String dbAlias = databaseType;
              if ( StringUtil.indexOfIgnoreCase(aScriptFileName, SQL_SERVER_STR) >= 0 &&
                   databaseType.equalsIgnoreCase(DatabaseDialect.SQL_SERVER.getType())) {
                  dbAlias = SQL_SERVER_STR;
              }
              
              
              //
              // First find scripts that have the name of the RDBMS of the connector we're using.
              //
              if ( StringUtil.indexOfIgnoreCase(aScriptFileName, dbAlias) >= 0 ) {
                  if ( ScriptType.isMaterializationScript(aScriptFileName)) {
                      // DB materialization table create script
                      String createScriptName = ScriptType.createScriptFileName(vdbName, vdbVersion);
                      scripts.setCreateScript(createScriptName, materializationModel.getDDLFileContentsGetBytes(aScriptFileName));
                      
                  } else if ( ScriptType.isTruncateScript(aScriptFileName)) {
                      // DB truncate script
                      String truncateScriptName = ScriptType.truncateScriptFileName(vdbName, vdbVersion);
                      scripts.setTruncateScript(truncateScriptName, materializationModel.getDDLFileContentsGetBytes(aScriptFileName));
                      
                  } else if ( ScriptType.isSwapScript(aScriptFileName)) {
                      // DB swap table script
                      String swapScriptName = ScriptType.swapScriptFileName(vdbName, vdbVersion);
                      scripts.setSwapScript(swapScriptName, materializationModel.getDDLFileContentsGetBytes(aScriptFileName));
                  }
              } else if ( ScriptType.isLoadScript(aScriptFileName)) {
                  // MetaMatrix load script
                  String loadScriptName = ScriptType.loadScriptFileName(vdbName, vdbVersion);
                  scripts.setLoadScript(loadScriptName, materializationModel.getDDLFileContentsGetBytes(aScriptFileName));
              }
          }
          
          
          String mmProtocol = (useSSL==true?"mms":"mm");  //$NON-NLS-1$  //$NON-NLS-2$
          // Create the prop file gen data
          MaterializedViewConnectionData propGenData = new MaterializedViewConnectionData(vdbName,
                                                                                          vdbVersion,
                                                                                          mmHost,
                                                                                          mmPort,
                                                                                          mmDriver,                                                                                      
                                                                                          metamatrixPwd,
                                                                                          metamatrixUserName,
                                                                                          mmProtocol,
                                                                                          matURL,
                                                                                          matDriver,
                                                                                          materializationUserPwd,
                                                                                          materializationUserName,
                                                                                          scripts.getTruncateScriptFileName(),
                                                                                          scripts.getLoadScriptFileName(),
                                                                                          scripts.getSwapScriptFileName(), 
                                                                                          logFileName);
          
          // Generate the connection prop file
          MaterializedViewScriptGenerator propGen = new MaterializedViewScriptGeneratorImpl(propGenData);
          
          oStream = new ByteArrayOutputStream();
    
           propGen.generateMaterializationConnectionPropFile(oStream);
      } catch (Throwable t) {
          final Object[] params = new Object[] {propFileName};
          throw new MetaMatrixRuntimeException(t, RuntimeMetadataPlugin.Util.getString("RuntimeMetadataHelper.Error_creating_prop_file", params)); //$NON-NLS-1$
      } finally {
          if (oStream != null) {
              try {
                  oStream.close();
              } catch (IOException err) {
              }
          }
      }
      
      // Add the prop file to the MaterializationLoadScripts
      if  ( oStream != null ) {
          scripts.setConnectionPropsFile(propFileName, oStream.toByteArray());
      }
      return scripts;
  }
    
    /**
     *  
     * @param vdbName
     * @param vdbVersion
     * @return
     * @since 4.2
     */
    private static String getNextVDBVersion(String vdbName, String vdbVersion) {
        // default to 1
        String nextVersion = "1"; //$NON-NLS-1$
        if ( vdbVersion == null) {
            // null passed in - version was unknown.  Query runtime catalog
            VirtualDatabaseID vdbID = null;
            try {
                vdbID = RuntimeMetadataCatalog.getInstance().getActiveVirtualDatabaseID(vdbName, vdbVersion);
            } catch (VirtualDatabaseDoesNotExistException err) {
                // VDB is new
            } catch (VirtualDatabaseException err) {
                // Config error
//            final Object[] params = new Object[] {vdbName};
//            throw new MetaMatrixRuntimeException(err, "Unable to determine VDB version for VDB {0}");
            }
            
            if (vdbID != null) {
                // If vdbID not null, version will be active version
                int existingVers = Integer.parseInt(vdbID.getVersion());
                nextVersion = Integer.toString(existingVers);
            }
            // if vdbID is null, no current VDB exists - default will be 1
        } else {
            // Version was known
            nextVersion = vdbVersion;
        }
        
        return nextVersion;
    }
    
    /** 
     * @param url
     * @return
     * @since 4.2
     */

    protected static String parseDatabaseType(final String url, final String driver, final String userName, final String pwd)
    throws Throwable {
        
        return parseDatabaseType(url, driver);

    }
        
    
    // This is the original method and is kept for old unit tests where
    // the database schema may no longer exist and there
    // the database connection cannot be made
    protected static String parseDatabaseType(final String url, final String driver) {
        // Example: jdbc:mmx:oracle://host:port;SID=sid
        if ( url != null ) {
            
            Supported supported = JDBCPlatform.getSupportedByProtocol(url);
            if (supported != null) {
            	switch (supported) {
            	case DB2:
                    return DatabaseDialect.DB2.getType().toLowerCase();
            	case MM_ORACLE:
            	case ORACLE:
                    return DatabaseDialect.ORACLE.getType().toLowerCase();
            	case SYBASE:
                    return DatabaseDialect.SYBASE.getType().toLowerCase();
            	case MSSQL:
                    return DatabaseDialect.SQL_SERVER.getType().toLowerCase();
            	case MYSQL:
                    return DatabaseDialect.MYSQL.getType().toLowerCase();
            	}
            }
            
            Object[] urlParts = StringUtil.split(url, ":").toArray(); //$NON-NLS-1$
            
            if (urlParts.length >= 3) {
                StringBuffer dbType = new StringBuffer((String)urlParts[2]);
                if ( dbType.charAt(dbType.length()-1) == '\\' ) {
                    // url may have escaped the ':'
                    dbType.setLength(dbType.length()-1);
                }
                
                return dbType.toString();
            }
        }
        return null;
    }    
    

}
