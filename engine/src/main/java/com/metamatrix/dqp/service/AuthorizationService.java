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

package com.metamatrix.dqp.service;

import java.util.Collection;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.query.eval.SecurityFunctionEvaluator;

/**
 * This service provides a means to check whether a connection is authorized to access
 * various data resources.
 */
public interface AuthorizationService extends ApplicationService, SecurityFunctionEvaluator {

    public static final int ACTION_READ = 0;
    public static final int ACTION_CREATE = 1;
    public static final int ACTION_UPDATE = 2;
    public static final int ACTION_DELETE = 3;

    public static final int CONTEXT_QUERY = 0;
    public static final int CONTEXT_INSERT = 1;
    public static final int CONTEXT_UPDATE = 2;
    public static final int CONTEXT_DELETE = 3;
    public static final int CONTEXT_PROCEDURE = 4;
    
    public static final String DEFAULT_WSDL_USERNAME = "metamatrixwsdluser";
    
    /**
     * Determine which of a set of resources a connection does not have permission to
     * perform the specified action.
     * @param connectionID Connection ID identifying the connection (and thus the user credentials)
     * @param action Action connection wishes to perform
     * @param resources Resources the connection wishes to perform the action on, Collection of String
     * @param context Auditing context
     * @return Collection Subset of resources
     * @throws MetaMatrixComponentException If an error occurs in the service while checking resources
     */
    Collection getInaccessibleResources(String connectionID, int action, Collection resources, int context) throws MetaMatrixComponentException;

    /**
     * Determine whether entitlements checking is enabled on the server.
     * @return <code>true</code> iff server-side entitlements checking is enabled.
     */
    boolean checkingEntitlements();
}
