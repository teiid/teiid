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

package org.teiid.transport;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

import org.teiid.adminapi.AdminRoles;
import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.admin.RolesAllowed;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.client.ExceptionUtil;
import com.metamatrix.common.comm.platform.CommPlatformPlugin;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.dqp.service.AuditMessage;
import com.metamatrix.dqp.service.AuthorizationService;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.platform.security.api.SessionToken;

/**
 * Call authorization service to make sure the current admin user has the
 * proper admin role(s) to perform the method.
 */
public class AdminAuthorizationInterceptor implements InvocationHandler {
	
    private final Object service;
    private AuthorizationService authAdmin;


    /**
     * Ctor. 
     * @param securityContextFactory
     * @param authorizationService
     * @param methodNames
     * @since 4.3
     */
    public AdminAuthorizationInterceptor(AuthorizationService authorizationService, Object service) {
        ArgCheck.isNotNull(authorizationService);
        this.authAdmin = authorizationService;
        this.service = service;
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
        SessionToken adminToken = DQPWorkContext.getWorkContext().getSessionToken();

    	Method serviceMethod = service.getClass().getMethod(method.getName(), method.getParameterTypes());
    	RolesAllowed allowed = serviceMethod.getAnnotation(RolesAllowed.class);
        if (allowed == null) {
        	allowed = method.getAnnotation(RolesAllowed.class);
        	if (allowed == null) {
        		allowed = serviceMethod.getDeclaringClass().getAnnotation(RolesAllowed.class);
        		if (allowed == null) {
            		allowed = method.getDeclaringClass().getAnnotation(RolesAllowed.class);
                }
        	}
        }
        if (allowed == null || allowed.value() == null) {
        	throw new MetaMatrixRuntimeException("Could not determine roles allowed for admin method"); //$NON-NLS-1$
        }

        boolean authorized = false;
        AuditMessage msg = new AuditMessage(LogConstants.CTX_ADMIN_API, Arrays.toString(allowed.value())+"-request", adminToken.getUsername(),  new Object[]{method.getName()}); //$NON-NLS-1$
        LogManager.log(MessageLevel.INFO, LogConstants.CTX_AUDITLOGGING, msg);

        for (int i = 0; i < allowed.value().length; i++) {
        	String requiredRoleName = allowed.value()[i];
			if (AdminRoles.RoleName.ANONYMOUS.equalsIgnoreCase(requiredRoleName)) {
				authorized = true;
				break;
			}
	            
            if (authAdmin.isCallerInRole(adminToken, requiredRoleName)) {
            	authorized = true;
            	break;
            }
        }
        if (!authorized) {
            msg = new AuditMessage(LogConstants.CTX_ADMIN_API, Arrays.toString(allowed.value())+"-denied", adminToken.getUsername(),  new Object[]{method.getName()}); //$NON-NLS-1$
            LogManager.log(MessageLevel.INFO, LogConstants.CTX_AUDITLOGGING, msg);
        	
        	Object[] msgParts = buildAuditMessage(adminToken, Arrays.toString(allowed.value()), method); 
            String errMsg = CommPlatformPlugin.Util.getString("AdminAuthorizationInterceptor.Admin_not_authorized", msgParts); //$NON-NLS-1$
            throw ExceptionUtil.convertException(method, new AuthorizationException(errMsg));
        }
        try {
        	return method.invoke(service, args);
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
