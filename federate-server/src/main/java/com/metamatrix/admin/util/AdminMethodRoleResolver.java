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

package com.metamatrix.admin.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.admin.api.server.AdminRoles;
import com.metamatrix.admin.api.exception.AdminException;
import com.metamatrix.admin.api.exception.AdminProcessingException;
import com.metamatrix.admin.api.exception.AdminComponentException;

import com.metamatrix.common.util.PropertiesUtils;


/**
 * Allows access to required role(s) for admin methods.
 * @since 4.3
 */
public class AdminMethodRoleResolver {
	
	public static final String ANONYMOUS_ROLE="Anonymous"; //$NON-NLS-1$
	
    private Map methodsToRoles = new HashMap();

    /**
     *
     * @since 4.3
     */
    public AdminMethodRoleResolver() {
        super();
    }

    /**
     * Determines the admin role required to call the given <code>method</code>.
     * @param methodName the name of the method for which the required admin role is sought.
     * @return The admin role required to call the admin method.
     * @throws AdminException
     * @since 4.3
     */
    public String getRoleNameForMethod(String methodName) throws AdminException {
        if (! methodsToRoles.containsKey(methodName) ) {
            Object[] params = new Object[] {methodName};
            String msg = AdminPlugin.Util.getString("AdminMethodRoleResolver.Unknown_method", params); //$NON-NLS-1$
            throw new AdminProcessingException(msg);
        }
        String roleName = (String)methodsToRoles.get(methodName);
        return roleName;
    }

    /**
     * Could hard-code as they are now - all over.  Could look up in repos.
     * @throws AdminException
     * @since 4.3
     */
    public void init() throws AdminException {
        Properties properties = null;
        try {
            properties = PropertiesUtils.loadAsResource(this.getClass(), DEFAULT_METHOD_ROLES_FILE);
        } catch (Exception err) {
            AdminException e = new AdminComponentException("Unable to load " + DEFAULT_METHOD_ROLES_FILE + " file: " + err.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$);
            e.setStackTrace(err.getStackTrace());
            throw e;
        }
        Properties allProps = new Properties();
        allProps.putAll(properties);

        Collection validRoleNames = AdminRoles.getAllRoleNames();
        Iterator keyItr = allProps.keySet().iterator();
        while (keyItr.hasNext()) {
            String key = (String)keyItr.next();
            String roleName = allProps.getProperty(key);
            if ( roleName == null || roleName.length() == 0 ) {
                Object[] params = new Object[] {key};
                String msg = AdminPlugin.Util.getString("AdminMethodRoleResolver.No_roles_defined_for_method", params); //$NON-NLS-1$
                throw new AdminComponentException(msg);
            }
            if ( ! validRoleNames.contains(roleName) && !ANONYMOUS_ROLE.equals(roleName)) {
                Object[] params = new Object[] {key, roleName};
                String msg = AdminPlugin.Util.getString("AdminMethodRoleResolver.Invalid_role_defined_for_method", params); //$NON-NLS-1$
                throw new AdminComponentException(msg);
            }
            methodsToRoles.put(key, roleName);
        }
    }

    private static final String DEFAULT_METHOD_ROLES_FILE = "methodroles.properties"; //$NON-NLS-1$
}
