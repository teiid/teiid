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

package com.metamatrix.platform.security.api;



/**
 * This interface is implemented by classes that are able to create AuthorizationPermission
 * instances, and is invoked by the service providers during loading and saving of policies from and to
 * data storage.
 */
public interface AuthorizationPermissionFactory {

    /**
     * Get the class that this factory creates instances of.
     * @return the class of the instances returned by this factory's <code>create</code> methods.
     */
    Class getPermissionClass();

    /**
     * Create the AuthorizationResource type for the permission type that this factory creates instances of.
     * @return A new resource instance of the appropriate type.
     */
    AuthorizationResource createResource(String name);

    /**
     * Create a new authorization permission for the specified resource.
     * @param resource the resource identifier
     * @param realm the realm into which this resource belongs
     * @param actions the actions for the resource
     * @param contentModifier the content modifier (may be null)
     */
    AuthorizationPermission create(AuthorizationResource resource, AuthorizationRealm realm, AuthorizationActions actions, String contentModifier);

    /**
     * Create a new authorization permission for the specified resource.
     * @param resource the resource identifier
     * @param realm the realm into which this resource belongs
     */
    AuthorizationPermission create(String resource, AuthorizationRealm realm);

    /**
     * Create a new authorization permission for the specified resource with the given actions.
     * @param resource the resource identifier
     * @param realm the realm into which this resource belongs
     * @param actions the actions for the resource
     */
    AuthorizationPermission create(String resource, AuthorizationRealm realm, AuthorizationActions actions);
}





