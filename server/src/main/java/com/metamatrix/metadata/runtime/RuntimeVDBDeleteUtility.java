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
import java.util.Iterator;

import org.teiid.metadata.RuntimeMetadataPlugin;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.metadata.runtime.util.LogRuntimeMetadataConstants;
import com.metamatrix.metadata.util.ErrorMessageKeys;
import com.metamatrix.platform.security.api.AuthorizationObjectEditor;
import com.metamatrix.platform.security.api.AuthorizationPolicyID;
import com.metamatrix.platform.security.api.AuthorizationRealm;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.security.api.service.AuthorizationServiceInterface;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.util.PlatformProxyHelper;


/** 
 * Implementation of RuntimeVDBDeleteUtility to be used with a running server.
 * This implementation requires a running SessionService and AuthenticationService.
 * <p>
 * This implementation uses the Session service to determine when it is safe to delete a
 * VDB version that has been marked for delete.  It is safe to delete a VDB version
 * when no more user sessions are logged in to it.

 * @since 4.3
 */
public class RuntimeVDBDeleteUtility extends AbstractVDBDeleteUtility {

    private static final SessionToken fakeToken = new SessionToken();

    
    /**
     * Check for any sessions logged into the specified VDB.
     * @param vdbID 
     * @param sessionID
     * @return true if there are no sessions logged in to the specivied VDB,
     * or if there is one session logged in and it is the specified sessionID.
     * @since 4.3
     */
    public boolean checkSessions(VirtualDatabaseID vdbID, MetaMatrixSessionID sessionID) throws MetaMatrixComponentException {
        Collection sessionIDs = this.getSessionsLoggedInToVDB(vdbID.getName(), vdbID.getVersion());
        
        return (sessionIDs.isEmpty() || 
                 (sessionIDs.size() == 1 && sessionIDs.contains(sessionID)));    
    }
    
    /**
     * Check for any sessions logged into the specified VDB.
     * @param vdbID 
     * @return true if there are no sessions logged in to the specivied VDB.
     * @since 4.3
     */
    public boolean checkSessions(VirtualDatabaseID vdbID) throws MetaMatrixComponentException {
        Collection sessionIDs = this.getSessionsLoggedInToVDB(vdbID.getName(), vdbID.getVersion());
        
        return (sessionIDs.isEmpty());
    
    }
    
    
    
    /**
     * For a given VDB version, delete all AuthorizationPolicies associated with it.
     */
    public void deleteAuthorizationPoliciesForVDB( String VDBName, String VDBVersion )
            throws MetaMatrixComponentException {
        AuthorizationServiceInterface authProxy = getAuthorizationServiceProxy();
        AuthorizationRealm realm = new AuthorizationRealm(VDBName, VDBVersion);
        Collection policyIDs = null;
        try {
            policyIDs = authProxy.getPolicyIDsInRealm(fakeToken, realm);
        } catch ( AuthorizationException e ) {
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.VDBDU_0004, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.VDBDU_0004));
        } catch ( Exception e ) {
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.VDBDU_0004, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.VDBDU_0004));
        }
        AuthorizationObjectEditor aoe = new AuthorizationObjectEditor();

        boolean hasRemoveActions = false;
        Iterator policyIDItr = policyIDs.iterator();
        while ( policyIDItr.hasNext() ) {
            AuthorizationPolicyID aPolicyID = (AuthorizationPolicyID) policyIDItr.next();
            aoe.remove(aPolicyID);
            hasRemoveActions = true;
        }

        if (!hasRemoveActions) {
            return;
        }
        try {
            authProxy.executeTransaction(fakeToken, aoe.getDestination().popActions());
            
            LogManager.logInfo(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, RuntimeMetadataPlugin.Util.getString("VDBDeleteUtility.2", new Object[] {VDBName, VDBVersion})); //$NON-NLS-1$
            
        } catch ( InvalidSessionException e ) {
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.VDBDU_0005, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.VDBDU_0005, new Object[]{new Integer(policyIDs.size()), VDBName, VDBVersion} ));
        } catch ( AuthorizationException e ) {
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.VDBDU_0005, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.VDBDU_0005, new Object[]{new Integer(policyIDs.size()), VDBName, VDBVersion} ));
        } catch ( Exception e ) {
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.VDBDU_0005, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.VDBDU_0005, new Object[]{new Integer(policyIDs.size()), VDBName, VDBVersion} ));
        }
    }
    
    /**
     * Convenience method that will create an AuthorizationServiceProxy
     */
    private AuthorizationServiceInterface getAuthorizationServiceProxy() throws ServiceException {
        return PlatformProxyHelper.getAuthorizationServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);
    }
}
