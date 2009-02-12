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

package com.metamatrix.console.models;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.common.actions.ModificationActionQueue;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.views.entitlements.DataNodePermissions;
import com.metamatrix.console.ui.views.entitlements.DataNodePermissionsWithNodeName;
import com.metamatrix.console.ui.views.entitlements.EntitlementInfo;
import com.metamatrix.console.ui.views.entitlements.EntitlementsDataInterface;
import com.metamatrix.console.ui.views.entitlements.EntitlementsTableRowData;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticQuickSorter;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.platform.admin.api.AuthorizationAdminAPI;
import com.metamatrix.platform.admin.api.AuthorizationEditor;
import com.metamatrix.platform.admin.api.PermissionDataNode;
import com.metamatrix.platform.admin.api.PermissionDataNodeTreeView;
import com.metamatrix.platform.admin.api.PermissionNode;
import com.metamatrix.platform.admin.apiimpl.PermissionDataNodeTreeViewImpl;
import com.metamatrix.platform.security.api.AuthorizationActions;
import com.metamatrix.platform.security.api.AuthorizationPolicy;
import com.metamatrix.platform.security.api.AuthorizationPolicyID;
import com.metamatrix.platform.security.api.AuthorizationRealm;
import com.metamatrix.platform.security.api.StandardAuthorizationActions;
import com.metamatrix.server.admin.api.RuntimeMetadataAdminAPI;

/**
 * The EntitlementManager handles all calls to the MetaMatrix server for viewing and modifying
 * entitlements.
 */
public class EntitlementManager extends Manager implements EntitlementsDataInterface {

    public EntitlementManager(ConnectionInfo connection) {
        super(connection);
    }
    
    public void init() {
        super.init();
    }

    public void deleteEntitlement(AuthorizationPolicyID id) throws ExternalException,
            ComponentNotFoundException, AuthorizationException {
        try {
            AuthorizationEditor aoe = ModelManager.getAuthorizationAPI(getConnection()).createEditor();
            aoe.remove(id);
            ModificationActionQueue maq = aoe.getDestination();
            List actions = maq.popActions();
            ModelManager.getAuthorizationAPI(getConnection())
            		.executeTransaction(actions);
        } catch (AuthorizationException e) {
            throw e;
        } catch (ComponentNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new ExternalException(e);
        }
        fireModelChangedEvent(MODEL_CHANGED);
    }

    /**
     * @return the Principals that are inside the AuthorizationPolicy for the specified Entitlement.
     */
    public Collection /*<MetaMatrixPrincipalName>*/ getPrincipalsForEntitlement(
            AuthorizationPolicyID policyID) throws ExternalException,
            AuthorizationException, ComponentNotFoundException {
        Collection /*<MetaMatrixPrincipalName>*/ princ = null;
        AuthorizationPolicy policy = getPolicy(policyID);
        if (policy != null) {
            princ = policy.getPrincipals();
        }
        return princ;
    }

    /**
     * Obtains an AuthorizationPolicy.
     *
     * @param policyID the AuthorizationPolicyID - exposed only as Object outside this manager
     * @return the AuthorizationPolicy for the specified ID
     */
    public AuthorizationPolicy getPolicy(AuthorizationPolicyID policyID) throws
            AuthorizationException, ComponentNotFoundException,
            ExternalException {
        AuthorizationPolicy result = null;
        if (policyID != null) {
            try {
                result = ModelManager.getAuthorizationAPI(getConnection())
                		.getPolicy(policyID);
            } catch (AuthorizationException e) {
                throw e;
            } catch (ComponentNotFoundException e) {
                throw e;
            } catch (Exception e) {
                throw new ExternalException(e);
            }
        } else {
        }
        return result;
    }

    /*
     * Obtain all the Entitlements stored in the server.
     * @return a Collection of id Objects which can be placed directly in JLists, JTables,
     * etc.
     */
    public Collection /*<AuthorizationPolicyID>*/ getAllEntitlements()
            throws ExternalException, AuthorizationException {
        Collection /*<AuthorizationPolicyID>*/ result;
        try {
            result = new TreeSet( ModelManager.getAuthorizationAPI(
            		getConnection()).findAllPolicyIDs() );
        } catch (AuthorizationException e) {
            throw e;
        } catch (Exception e) {
            throw new ExternalException(e);
        }
        return result;
    }

    public EntitlementsTableRowData[] getEntitlements()
            throws AuthorizationException, ExternalException {
        Collection /*<AuthorizationPolicyID>*/ ids = getAllEntitlements();
        Iterator it = ids.iterator();
        Collection /*<EntitlementsTableRowData>*/ ent = new ArrayList(ids.size());
        while (it.hasNext()) {
            AuthorizationPolicyID id = (AuthorizationPolicyID)it.next();
            String name = id.getName();
            String displayName = id.getDisplayName();
            String vdbName = id.getVDBName();
            int vdbVersion = id.getVDBVersion();
            if ((displayName == null) && (name.indexOf(
                    AuthorizationPolicyID.DELIMITER) >= 0)) {
                int firstDelimiterLoc = name.indexOf(
                        AuthorizationPolicyID.DELIMITER);
                int secondDelimiterLoc = name.lastIndexOf(
                        AuthorizationPolicyID.DELIMITER);
                displayName = name.substring(0, firstDelimiterLoc);
                vdbName = name.substring(firstDelimiterLoc + 1, secondDelimiterLoc);
                String vdbVersionString = name.substring(secondDelimiterLoc + 1);
                int underscoreLoc = vdbVersionString.indexOf('_');
                if (underscoreLoc >= 0) {
                    vdbVersionString = vdbVersionString.substring(0, underscoreLoc);
                }
                vdbVersion = (new Integer(vdbVersionString)).intValue();
            }
            EntitlementsTableRowData row = new EntitlementsTableRowData(
                    displayName, vdbName, vdbVersion);
            //Ignore entitlement names not in the correct format.  These must have been
            //pre-existing in the entitlements data base.
            if ((row != null) && (row.getEntitlementName() != null) &&
                    (row.getVDBName() != null)) {
                ent.add(row);
            }
        }
        EntitlementsTableRowData[] rows = new EntitlementsTableRowData[ent.size()];
        it = ent.iterator();
        for (int i = 0; it.hasNext(); i++) {
            rows[i] = (EntitlementsTableRowData)it.next();
        }
        return rows;
    }

    public java.util.List /*<AuthorizationPolicyID>*/ getPolicyIDs()
            throws AuthorizationException, ExternalException {
        Collection /*<AuthorizationPolicyID>*/ ids = getAllEntitlements();
        Vector v = new Vector(ids.size());
        Iterator it = ids.iterator();
        while (it.hasNext()) {
            AuthorizationPolicyID id = (AuthorizationPolicyID)it.next();
            v.add(id);
        }
        return v;
    }

    public Collection /*<VirtualDatabase>*/ getAllVDBs()
            throws AuthorizationException, ExternalException,
            ComponentNotFoundException {
        try {
            Collection coll = ModelManager.getVdbManager(getConnection())
            		.getVDBs();
            return coll;
        } catch (AuthorizationException ex) {
            throw ex;
        } catch (ComponentNotFoundException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ExternalException(ex);
        }
    }

    public int[] getVersionsForVDB(String vdbName)
            throws AuthorizationException, ExternalException,
            ComponentNotFoundException {
        Collection coll = getAllVDBs();
        java.util.List versionsList = new ArrayList(coll.size());
        Iterator it = coll.iterator();
        for (int i = 0; it.hasNext(); i++) {
            VirtualDatabase vdb = (VirtualDatabase)it.next();
            VirtualDatabaseID id = (VirtualDatabaseID)vdb.getID();
            String name = id.getName();
            if (name.equals(vdbName)) {
                String versionStr = id.getVersion();
                //BWP 03/01/02  Do we need this code anymore?  It was put in
                //when we were seeing versions such as "1.0".
                float version = new Float(versionStr).floatValue();
                int versInt = Math.round(version);
                versionsList.add(new Integer(versInt));
            }
        }
        int[] versions = new int[versionsList.size()];
        it = versionsList.iterator();
        for (int i = 0; it.hasNext(); i++) {
            Integer temp = (Integer)it.next();
            versions[i] = temp.intValue();
        }
        //Do bubble sort to sort the versions
        boolean done = false;
        while (!done) {
            done = true;
            for (int i = 0; i < versions.length - 1; i++) {
                if (versions[i] > versions[i + 1]) {
                    done = false;
                    int temp = versions[i];
                    versions[i] = versions[i + 1];
                    versions[i + 1] = temp;
                }
            }
        }
        return versions;
    }

    public void createNewEntitlement(String entitlementName,
            String entitlementDescription, String vdbName,
            int vdbVersion, EntitlementsTableRowData dataNodesSameAsEntitlement,
            EntitlementsTableRowData principalsSameAsEntitlement)
            throws AuthorizationException, ExternalException {
        try {
            AuthorizationEditor aoe = ModelManager.getAuthorizationAPI(getConnection()).createEditor();
            AuthorizationPolicyID policyID = new AuthorizationPolicyID(entitlementName, vdbName, vdbVersion);
            AuthorizationPolicy policy = aoe.createAuthorizationPolicy(policyID);
            String desc = entitlementDescription;
            if (desc == null) {
                desc = ""; //$NON-NLS-1$
            }
            aoe.setDescription(policy, desc);
            if (dataNodesSameAsEntitlement != null) {
                AuthorizationPolicyID dPolicyID = new AuthorizationPolicyID(
                        dataNodesSameAsEntitlement.getEntitlementName(),
                        dataNodesSameAsEntitlement.getVDBName(),
                        dataNodesSameAsEntitlement.getVDBVersion());
                AuthorizationPolicy dPolicy = this.getPolicy(dPolicyID);
                RuntimeMetadataAdminAPI api = ModelManager.getRuntimeMetadataAPI(
                		getConnection());
                // Get list of all paths in this VDB version so that we know to skip cloning permission
                // whose resource is now deleted from this VDB.
                Set allPaths = api.getAllDataNodeNames(vdbName, (new Integer(vdbVersion)).toString());
                policy = aoe.clonePolicyPermissions(dPolicy, policy,
                        new AuthorizationRealm(vdbName,
                        (new Integer(vdbVersion)).toString()),
                        allPaths, null);
            }
            if (principalsSameAsEntitlement != null) {
                AuthorizationPolicyID pPolicyID = new AuthorizationPolicyID(
                        principalsSameAsEntitlement.getEntitlementName(),
                        principalsSameAsEntitlement.getVDBName(),
                        principalsSameAsEntitlement.getVDBVersion());
                AuthorizationPolicy pPolicy = this.getPolicy(pPolicyID);
                policy = aoe.clonePolicyPrincipals(pPolicy, policy);
            }
            ModificationActionQueue maq = aoe.getDestination();
            List actions = maq.popActions();
            ModelManager.getAuthorizationAPI(getConnection())
            		.executeTransaction(actions);
        } catch (AuthorizationException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ExternalException(ex);
        }
    }

    public boolean doesEntitlementExist(String entName,String vdbName,int vdbVersion) 
        throws AuthorizationException,ExternalException,ComponentNotFoundException {
        
        boolean result = false;
        AuthorizationAdminAPI api = ModelManager.getAuthorizationAPI(getConnection());
        try {
            AuthorizationPolicyID id = new AuthorizationPolicyID(entName, vdbName, vdbVersion);
            Boolean exists = api.containsPolicy(id);
            result = exists.booleanValue();
        } catch (ComponentNotFoundException ex) {
            throw ex;
        } catch (AuthorizationException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ExternalException(ex);
        }
        return result;
    }

    public EntitlementInfo getEntitlementInfo(String entName, String vdbName,
            int vdbVersion) throws AuthorizationException, ExternalException,
            ComponentNotFoundException {
        try {
            AuthorizationPolicyID policyID = new AuthorizationPolicyID(
                    entName, vdbName, vdbVersion);
//System.err.println("in getEntitlementInfo(), policyID is " + policyID);
            AuthorizationPolicy policy = this.getPolicy(policyID);
//System.err.println("description for policy is " + policy.getDescription());
            Collection coll = this.getPrincipalsForEntitlement(policyID);
            PermissionDataNodeTreeView tv = this.getTreeViewForData(vdbName, vdbVersion, policyID);
			EntitlementInfo entInfo = new EntitlementInfo(tv, policyID, policy,
                    entName, policy.getDescription(), vdbName, vdbVersion, new ArrayList(coll));
            return entInfo;
        } catch (ComponentNotFoundException ex) {
            throw ex;
        } catch (AuthorizationException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ExternalException(ex);
        }
    }

    public void changeAPolicy(PermissionDataNodeTreeView treeView,
                              AuthorizationPolicy policy,
                              String newDescription,
                              Collection /* <String> */addedGroupPrincipals,
                              Collection /* <String> */removedGroupPrincipals,
                              java.util.List /* <DataNodePermissionsWithNodeName> */changedPermissions)

                                                                                                      throws AuthorizationException,
                                                                                                      ExternalException {
            	
        try {
            AuthorizationEditor aoe = ModelManager.getAuthorizationAPI(getConnection()).createEditor();
            String oldDescription = policy.getDescription();
            if (!oldDescription.equals(newDescription)) {
                policy = aoe.setDescription(policy, newDescription);
            }

            Set /*<MetaMatrixPrincipalName>*/ removees = new HashSet();
            Iterator it = removedGroupPrincipals.iterator();
            while (it.hasNext()) {
//                MetaMatrixPrincipalName prin = new MetaMatrixPrincipalName(
//                        (String)it.next(), MetaMatrixPrincipal.TYPE_GROUP);
                removees.add(it.next());
            }
            if (removees.size() > 0) {
                policy = aoe.removePrincipals(policy, removees);
            }
            Set /*<MetaMatrixPrincipalName>*/ addees = new HashSet();
            it = addedGroupPrincipals.iterator();
            while (it.hasNext()) {
//                MetaMatrixPrincipalName prin = new MetaMatrixPrincipalName(
//                        (String)it.next(), MetaMatrixPrincipal.TYPE_GROUP);
                addees.add(it.next());
            }
            if (addees.size() > 0) {
                policy = aoe.addAllPrincipals(policy, addees);
            }
            if (changedPermissions.size() > 0) {
                setPermissionNodeAuths(changedPermissions);
                aoe.modifyPermissions(treeView, policy);
            }
            ModificationActionQueue maq = aoe.getDestination();
//System.err.println("Connected to " + getConnection().getURL());          
//System.err.println("ModificationActionQueue for " + policy.getAuthorizationPolicyID().getName() + " is:");
//System.err.println(maq);
            List actions = maq.popActions();
            AuthorizationAdminAPI api = ModelManager.getAuthorizationAPI(
            		getConnection());
//Date before = new Date();            
            api.executeTransaction(actions);
//Date after = new Date();
//DaysHoursMinutesSeconds.printMethodCallDurationMessage("executeTransaction",
// before, after);
        } catch (AuthorizationException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ExternalException(ex);
        }
    }

    public PermissionDataNodeTreeView getTreeViewForData(String vdbName,
            int vdbVersion, AuthorizationPolicyID policyID)
            throws AuthorizationException, ExternalException {
        try {
            RuntimeMetadataAdminAPI api = ModelManager.getRuntimeMetadataAPI(getConnection());
            String versString = (new Integer(vdbVersion)).toString();
            PermissionDataNode root = api.getEntitlementTree(vdbName, versString, policyID);
            PermissionDataNodeTreeView view = new PermissionDataNodeTreeViewImpl(root);
            return view;
        } catch (AuthorizationException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ExternalException(ex);
        }
    }

    public java.util.List /*<String>*/ getEntitlementsForVDB(String vdbName,
            int vdbVersion) throws AuthorizationException, ExternalException {
        java.util.List /*<String>*/ matches = new ArrayList();
        java.util.List /*<AuthorizationPolicyID>*/ ids = getPolicyIDs();
        Iterator it = ids.iterator();
        while (it.hasNext()) {
            AuthorizationPolicyID id = (AuthorizationPolicyID)it.next();
            if (policyIsForVDB(id, vdbName, vdbVersion)) {
                String entName = getEntitlementName(id);
                matches.add(entName);
            }
        }
        Collection sorted = StaticQuickSorter.quickStringCollectionSort(matches);
        matches = new ArrayList(sorted);
        return matches;
    }

/**************************
* Internal private methods
**************************/

    public void setPermissionNodeAuths(
            Collection /*<DataNodePermissionsWithNodeName>*/ changedPermissions) {
        Iterator it = changedPermissions.iterator();
        while (it.hasNext()) {
            DataNodePermissionsWithNodeName perms =
                    (DataNodePermissionsWithNodeName)it.next();
            DataNodePermissions newPerms = perms.getNewPermissions();
            int flag = 0;
            if (newPerms.hasCreate()) {
                flag |= StandardAuthorizationActions.DATA_CREATE_VALUE;
            }
            if (newPerms.hasRead()) {
                flag |= StandardAuthorizationActions.DATA_READ_VALUE;
            }
            if (newPerms.hasUpdate()) {
                flag |= StandardAuthorizationActions.DATA_UPDATE_VALUE;
            }
            if (newPerms.hasDelete()) {
                flag |= StandardAuthorizationActions.DATA_DELETE_VALUE;
            }
            AuthorizationActions actions =
                    StandardAuthorizationActions.getAuthorizationActions(flag);
            try {
            	PermissionNode pNode = perms.getCorrespondingNode();
				pNode.setActions(actions);
//System.err.println("setting actions for node " + pNode + " to " + actions);
            } catch (com.metamatrix.platform.admin.api.exception.PermissionNodeNotActionableException ex) {
                LogManager.logError(LogContexts.ENTITLEMENTS, ex,
                        "On modifying entitlement recvd " + //$NON-NLS-1$
                        "PermissionNodeNotActionableException.  Should never happen."); //$NON-NLS-1$
            }
        }
    }

    private boolean policyIsForVDB(AuthorizationPolicyID id, String vdbName,
            int vdbVersion) {
        String policyVDB = id.getVDBName();
        int policyVDBVersion = id.getVDBVersion();
        boolean matches = ((policyVDBVersion == vdbVersion) &&
                policyVDB.equals(vdbName));
        return matches;
    }

    private String getEntitlementName(AuthorizationPolicyID id) {
        String name = id.getName();
        int delimiterLoc = name.indexOf(AuthorizationPolicyID.DELIMITER);
        if (delimiterLoc < 0) {
            delimiterLoc = name.length();
        }
        String entName = name.substring(0, delimiterLoc);
        return entName;
    }
}
