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

import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.InvalidSessionException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogContextsUtil.PlatformAdminConstants;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.SessionToken;

/**
 * Static implementation of the AdminHelper.<br>
 * This class is used by all <SubSystem>AdminAPIImpl to do general tasks such as
 * session validation and authorization role checking.
 */
public class AdminAPIHelper {

    // The singleton
    private static AdminHelper adminHelper;

    // Get singleton init'ing as nessary
    private synchronized static AdminHelper getAdminHelper() {

        if ( adminHelper == null ) {
            initialize();
        }
        return adminHelper;
    }

    // Init singleton
    private synchronized static void initialize() {
        adminHelper = new AdminHelper();
    }

    /**
     * Checks that user is in the necessary role, throws an exception if
     * not.  Methods of this administrative API may require that a caller be
     * in one or more security roles.
     * @param adminToken a valid SessionToken object representing the session
     * of the caller attempting an administrative operation
     * @param roleName String name of role to be checked for caller membership
     * @param methodSignature the signature with arguments of the method this admin is attempting to call.
     * @throws AuthorizationException if caller is <i>not</i> in the role, and
     * therefore not authorized to make the operation
     * @throws ComponentNotFoundException if the authorization service could
     * not be communicated with due to a bad service instance or proxy
     */
    public static void checkForRequiredRole(SessionToken adminToken, String roleName, String methodSignature)
    throws AuthorizationException, ComponentNotFoundException {
        Object[] msgParts = null;
        boolean msgWillBeRecorded = LogManager.isMessageToBeRecorded(PlatformAdminConstants.CTX_AUDIT_ADMIN, MessageLevel.INFO);
        if (msgWillBeRecorded) {
            // Audit Admin attempt
            if (methodSignature != null) {
                methodSignature = methodSignature.split("\\(")[0]; //$NON-NLS-1$
            }
            msgParts = new Object[] {adminToken.getUsername(), adminToken.getSessionID().toString(), 
                                        roleName, methodSignature};
            LogManager.logInfo(PlatformAdminConstants.CTX_AUDIT_ADMIN, AdminPlugin.Util.getString("Admin_Audit_request", msgParts)); //$NON-NLS-1$
        }
        
        getAdminHelper().checkForRequiredRole(adminToken, roleName);
        
        // Audit Admin granted
        if (msgWillBeRecorded) {
            LogManager.logInfo(PlatformAdminConstants.CTX_AUDIT_ADMIN,AdminPlugin.Util.getString("Admin_Audit_granted", msgParts)); //$NON-NLS-1$
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
     */
    public static SessionToken validateSession(MetaMatrixSessionID sessionID)
    throws InvalidSessionException, ComponentNotFoundException {
        return getAdminHelper().validateSession(sessionID);
    }

}
