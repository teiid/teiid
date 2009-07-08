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

package com.metamatrix.metadata.runtime;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.teiid.metadata.RuntimeMetadataPlugin;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.security.SessionServiceException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseException;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.metadata.runtime.util.LogRuntimeMetadataConstants;
import com.metamatrix.metadata.util.ErrorMessageKeys;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.util.PlatformProxyHelper;

/**
 * Utility class that deletes VDBs that were marked for deletion.
 */
public abstract class AbstractVDBDeleteUtility {


    /**
     * Constructor
     */
    public AbstractVDBDeleteUtility() {
    }

    /**
     * Deletes the given VDB version provided it has been marked for delete and
     * has no user sessions logged in to it.
     * @throws VirtualDatabaseException If an error occurs while deleting the VDB.
     * @throws MetaMatrixComponentException If an erorr occurs while accessing components required to delete the VDB.
     */
    public void deleteVDBMarkedForDelete(VirtualDatabaseID vdbID)
            throws VirtualDatabaseException, MetaMatrixComponentException {
        LogManager.logTrace(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, "deleteVDBMarkedForDelete(): checking " + vdbID + //$NON-NLS-1$
                                                                              " to see if marked for deletion."); //$NON-NLS-1$
        // Get all VDBIDs marked for delete
        Collection vdbs = RuntimeMetadataCatalog.getInstance().getDeletedVirtualDatabaseIDs();
        // If none marked for delete or this one is not in list, nothing to do.
        if (vdbs.size() == 0 || !vdbs.contains(vdbID)) {
            return;
        }

        // If no sessions are logged in to the VDB version, delete it.
        if (checkSessions(vdbID)) {            
            LogManager.logTrace(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, "deleteVDBMarkedForDelete(): deleting " + vdbID + //$NON-NLS-1$
                                                                                  " - no sessions logged in."); //$NON-NLS-1$

            // Delete any entitlements that were in deleted VDB version first
            deleteAuthorizationPoliciesForVDB(vdbID.getName(), vdbID.getVersion());

            // Finally, delete VDB
            RuntimeMetadataCatalog.getInstance().deleteVirtualDatabase(vdbID);
            
            LogManager.logInfo(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA,  RuntimeMetadataPlugin.Util.getString("VDBDeleteUtility.1", new Object[] {vdbID})); //$NON-NLS-1$
            
        } else {
            LogManager.logTrace(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, "deleteVDBMarkedForDelete(): NOT deleting " + vdbID + " - has sessions logged in."); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

    /** 
     * @see com.metamatrix.platform.util.VDBDeleteUtility#deleteVDBsMarkedForDelete(com.metamatrix.platform.security.api.MetaMatrixSessionID)
     * @since 4.3
     */
    public void deleteVDBsMarkedForDelete(MetaMatrixSessionID id) throws MetaMatrixProcessingException,
                                                                 MetaMatrixComponentException {
        // Get all VDBIDs marked for delete
        Collection vdbs = RuntimeMetadataCatalog.getInstance().getDeletedVirtualDatabaseIDs();
        // If none marked for delete, nothing to do.
        if (vdbs.size() == 0) {
            return;
        }
        LogManager.logTrace(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, "deleteVDBsMarkedForDelete(" + id + "): checking " + //$NON-NLS-1$ //$NON-NLS-2$
                                                                              vdbs.size() + " VDBs marked for deletion."); //$NON-NLS-1$

        Iterator vdbItr = vdbs.iterator();
        while ( vdbItr.hasNext() ) {
            VirtualDatabaseID vdbID = (VirtualDatabaseID) vdbItr.next();
            
            // If no sessions are logged in to the VDB version, delete it.
            if (checkSessions(vdbID, id)) {
                LogManager.logTrace(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, "deleteVDBsMarkedForDelete(" + id + "): deleting " + //$NON-NLS-1$ //$NON-NLS-2$
                                                                                      vdbID + " - no sessions logged in."); //$NON-NLS-1$

                // Delete any entitlements to VDB version first
                deleteAuthorizationPoliciesForVDB(vdbID.getName(), vdbID.getVersion());

                // Finally, delete VDB
                RuntimeMetadataCatalog.getInstance().deleteVirtualDatabase(vdbID);
                LogManager.logInfo(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, RuntimeMetadataPlugin.Util.getString("VDBDeleteUtility.1", new Object[] {vdbID})); //$NON-NLS-1$
            } 
        }
    }

    /**
     * For a given VDB version, get all sessionIDs logged in to it.
     */
    protected Collection getSessionsLoggedInToVDB( String VDBName, String VDBVersion )
            throws MetaMatrixComponentException {
        Collection sessionIDs = Collections.EMPTY_LIST;

        try {
            sessionIDs = getSessionServiceProxy().getSessionsLoggedInToVDB(VDBName, VDBVersion);
        } catch (SessionServiceException e) {
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.VDBDU_0003, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.VDBDU_0003));
        } catch (ServiceException e) {
            throw new ComponentNotFoundException(e, ErrorMessageKeys.VDBDU_0003, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.VDBDU_0003));
        }
        return sessionIDs;
    }

    
    

    /**
     * Convenience method that will create a SessionServiceProxy
     */
    private SessionServiceInterface getSessionServiceProxy() throws ServiceException {
    	SessionServiceInterface sessionServiceProxy =
                PlatformProxyHelper.getSessionServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);
        return sessionServiceProxy;
    }

    
    
   
    
    
    
    /**
     * For a given VDB version, delete all AuthorizationPolicies associated with it.
     */
    public abstract void deleteAuthorizationPoliciesForVDB(String VDBName,
                                                            String VDBVersion) throws MetaMatrixComponentException;
    
    
    /**
     * Check for any sessions logged into the specified VDB.
     * @param vdbID 
     * @param sessionID
     * @return true if there are no sessions logged in to the specivied VDB,
     * or if there is one session logged in and it is the specified sessionID.
     * @since 4.3
     */
    public abstract boolean checkSessions(VirtualDatabaseID vdbID,
                                          MetaMatrixSessionID sessionID) throws MetaMatrixComponentException;
    
    /**
     * Check for any sessions logged into the specified VDB.
     * @param vdbID 
     * @return true if there are no sessions logged in to the specivied VDB.
     * @since 4.3
     */
    public abstract boolean checkSessions(VirtualDatabaseID vdbID) throws MetaMatrixComponentException;
}
