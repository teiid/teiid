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

package com.metamatrix.platform.admin.apiimpl;

import com.metamatrix.admin.AdminMessages;
import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.security.MetaMatrixSecurityException;
import com.metamatrix.api.exception.security.SessionServiceException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.dqp.internal.process.DQPWorkContext;
import com.metamatrix.platform.admin.api.PlatformAdminLogConstants;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.security.api.service.AuthorizationServiceInterface;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.platform.security.util.RolePermissionFactory;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.util.PlatformProxyHelper;

/**
 * This class is used by all <SubSystem>AdminAPIImpl to do general tasks such as
 * session vlidation and authorization role checking.
 */
public class AdminHelper implements IAdminHelper {

    /**
     * Exception message sent to client if session service is down; indicates
     * that an session service was not available, client
     * privileges are unknown, and therefore a client's session id will not be
     * validated.
     * @see #validateSession(SessionID)
     */
    private static final String SESSION_SERVICE_DOWN_MSG = AdminPlugin.Util.getString(AdminMessages.ADMIN_0010);

    // Service Proxies
    private AuthorizationServiceInterface authAdmin;
    private SessionServiceInterface sessionAdmin;

    // RolePermission Factory
    RolePermissionFactory roleFactory;

    /**
     * ctor
     */
    public AdminHelper() {
        // Init authorization svc proxy
        authAdmin = PlatformProxyHelper.getAuthorizationServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);

        // Init session svc proxy
        sessionAdmin = PlatformProxyHelper.getSessionServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);

        // Init RolePermissionFactory
        roleFactory = new RolePermissionFactory();
    }

    /**
     * Checks that user is in the necessary role, throws an exception if
     * not.  Methods of this administrative API may require that a caller be
     * in one or more security roles.
     * @param adminToken a valid SessionToken object representing the session
     * of the caller attempting an administrative operation
     * @param roleName String name of role to be checked for caller membership
     * @throws AuthorizationException if caller is <i>not</i> in the role, and
     * therefore not authorized to make the operation
     * @throws ComponentNotFoundException if the authorization service could
     * not be communicated with due to a bad service instance or proxy
     */
    public void checkForRequiredRole(SessionToken adminToken, String roleName)
    throws AuthorizationException, ComponentNotFoundException {
    	if (LogManager.isMessageToBeRecorded(PlatformAdminLogConstants.CTX_ADMIN_API, MessageLevel.TRACE)) {
			LogManager.logTrace(PlatformAdminLogConstants.CTX_ADMIN_API,
			                    "Checking owner of session token " + adminToken + " for membership in role: " + roleName); //$NON-NLS-1$
		}
        boolean isAuthorized = false;
        try{
            isAuthorized = authAdmin.isCallerInRole(adminToken, roleName);
        } catch (MetaMatrixSecurityException e){
            String msg = AdminPlugin.Util.getString(AdminMessages.ADMIN_0009, adminToken);
            throw new ComponentNotFoundException(e, AdminMessages.ADMIN_0009, msg);
        } catch (ServiceException e){
            String msg = AdminPlugin.Util.getString(AdminMessages.ADMIN_0009, adminToken);
            throw new ComponentNotFoundException(e, AdminMessages.ADMIN_0009, msg);
        } catch (Exception e){
            String msg = AdminPlugin.Util.getString(AdminMessages.ADMIN_0009, adminToken);
            throw new ComponentNotFoundException(e, AdminMessages.ADMIN_0009, msg);
        }

        if (!isAuthorized){
            String msg = AdminPlugin.Util.getString(AdminMessages.ADMIN_0008, adminToken, roleName.toString());
            throw new AuthorizationException(AdminMessages.ADMIN_0008, msg);
        }
    	if (LogManager.isMessageToBeRecorded(PlatformAdminLogConstants.CTX_ADMIN_API, MessageLevel.TRACE)) {
			LogManager.logTrace(PlatformAdminLogConstants.CTX_ADMIN_API,
			                    "Verified owner of session token " + adminToken + " is in role: " + roleName); //$NON-NLS-1$
		}
    }

    /**
     * Get the <code>SessionToken</code> and validate that the session is active
     * for the specified <code>MetaMatrixSessionID</code>.
     * @param sessionID the <code>MetaMatrixSessionID</code> for the session in
     * question.
     * @return The <code>SessionToken</code> for the session in question.
     * @throws InvalidSessionException If session has expired or doesn't exist
     * @throws ComponentNotFoundException If couldn't find needed service component
     * @deprecated use {@link DQPWorkContext} instead
     */
    public SessionToken validateSession(MetaMatrixSessionID sessionID)
    throws InvalidSessionException, ComponentNotFoundException {
    	if (LogManager.isMessageToBeRecorded(PlatformAdminLogConstants.CTX_ADMIN_API, MessageLevel.TRACE)) {
			LogManager.logTrace(PlatformAdminLogConstants.CTX_ADMIN_API,
			                    "Validating user session with session ID \"" + sessionID + "\""); //$NON-NLS-1$
		}
        SessionToken token = null;
        try {
            token = sessionAdmin.validateSession(sessionID).getSessionToken();
        } catch (InvalidSessionException e) {
            throw e;
        } catch (SessionServiceException e) {
            throw new ComponentNotFoundException(e,AdminMessages.ADMIN_0010, SESSION_SERVICE_DOWN_MSG);
        } catch (ServiceException e) {
            String msg = AdminPlugin.Util.getString(AdminMessages.ADMIN_0013);
            throw new ComponentNotFoundException(e,msg);
        }

    	if (LogManager.isMessageToBeRecorded(PlatformAdminLogConstants.CTX_ADMIN_API, MessageLevel.TRACE)) {
			LogManager.logTrace(PlatformAdminLogConstants.CTX_ADMIN_API,
			                    "Validated user session with session ID \"" + sessionID + "\""); //$NON-NLS-1$
		}
        return token;
    }

}
