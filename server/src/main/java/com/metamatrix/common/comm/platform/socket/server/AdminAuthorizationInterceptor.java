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

package com.metamatrix.common.comm.platform.socket.server;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.metamatrix.admin.api.exception.AdminProcessingException;
import com.metamatrix.admin.api.server.ServerAdmin;
import com.metamatrix.admin.util.AdminMethodRoleResolver;
import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.common.comm.platform.CommPlatformPlugin;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogContextsUtil.PlatformAdminConstants;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.dqp.internal.process.DQPWorkContext;
import com.metamatrix.platform.admin.apiimpl.IAdminHelper;
import com.metamatrix.platform.security.api.SessionToken;

/**
 * Call authorization service to make sure the current admin user has the
 * proper admin role(s) to perform the method.
 */
public class AdminAuthorizationInterceptor implements InvocationHandler {
	
    private final IAdminHelper authorizationService;
    private final AdminMethodRoleResolver methodNames;
    private final ServerAdmin serverAdmin;
    
    /**
     * Ctor. 
     * @param securityContextFactory
     * @param authorizationService
     * @param methodNames
     * @since 4.3
     */
    public AdminAuthorizationInterceptor(
            IAdminHelper authorizationService,
            AdminMethodRoleResolver methodNames, ServerAdmin serverAdmin) {

        ArgCheck.isNotNull(authorizationService);
        ArgCheck.isNotNull(methodNames);
        this.authorizationService = authorizationService;
        this.methodNames = methodNames;
        this.serverAdmin = serverAdmin;
    }

    /**
     * 
     * @param invocation
     * @param securityContext
     * @throws AuthorizationException
     * @throws MetaMatrixProcessingException
     * @since 4.3
     */
    public Object invoke(Object proxy, Method method, Object[] args)
	throws Throwable {
        // Validate user's admin session is active
        SessionToken adminToken = DQPWorkContext.getWorkContext().getSessionToken();

		// Verify that the admin user is authorized to perform the given operation
		String requiredRoleName = methodNames.getRoleNameForMethod(method.getName());
		
		if (!AdminMethodRoleResolver.ANONYMOUS_ROLE.equals(requiredRoleName)) {
            
            Object[] msgParts = null;
            boolean msgWillBeRecorded = LogManager.isMessageToBeRecorded(PlatformAdminConstants.CTX_AUDIT_ADMIN, MessageLevel.INFO);
            if (msgWillBeRecorded) {
                msgParts = buildAuditMessage(adminToken, requiredRoleName, method);
                LogManager.logInfo(PlatformAdminConstants.CTX_AUDIT_ADMIN,
                                       CommPlatformPlugin.Util.getString("AdminAuthorizationInterceptor.Admin_Audit_request", msgParts)); //$NON-NLS-1$
            }

            try {
                authorizationService.checkForRequiredRole(adminToken, requiredRoleName);
                LogManager.logInfo(PlatformAdminConstants.CTX_AUDIT_ADMIN, CommPlatformPlugin.Util.getString("AdminAuthorizationInterceptor.Admin_granted", msgParts)); //$NON-NLS-1$
            } catch (AuthorizationException err) {
                if ( msgParts == null ) {
                    msgParts = buildAuditMessage(adminToken, requiredRoleName, method);
                }
                String errMsg = CommPlatformPlugin.Util.getString("AdminAuthorizationInterceptor.Admin_not_authorized", msgParts); //$NON-NLS-1$
                LogManager.logWarning(PlatformAdminConstants.CTX_AUDIT_ADMIN, errMsg);
                throw new AdminProcessingException(errMsg);
            } catch (ComponentNotFoundException err) {
                if ( msgParts == null ) {
                    msgParts = buildAuditMessage(adminToken, requiredRoleName, method);
                }
                String errMsg = CommPlatformPlugin.Util.getString("AdminAuthorizationInterceptor.Admin_not_authorized", msgParts); //$NON-NLS-1$
                LogManager.logWarning(PlatformAdminConstants.CTX_AUDIT_ADMIN, errMsg);
                throw new AdminProcessingException(errMsg);
            }
        }
        try {
        	return method.invoke(this.serverAdmin, args);
        } catch (InvocationTargetException e) {
        	throw e.getTargetException();
        }
    }

    /** 
     * Builds an audit msg using given values including method signature string from given invocation using method
     * name and argument values.
     * @param securityContext
     * @param adminToken
     * @param requiredRoleName
     * @param invocation
     * @return 
     * @since 5.0
     */
    private Object[] buildAuditMessage(SessionToken adminToken, String requiredRoleName, Method invocation) {
        return new Object[] {adminToken.getUsername(), adminToken.getSessionID().toString(), requiredRoleName, invocation.getName()};
    }

}
