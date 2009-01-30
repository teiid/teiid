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

package com.metamatrix.data.pool;

import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;

public interface ConnectorIdentityFactory {

    /**
     * Create an identity object based on a security context.  This method determines
     * how different security contexts are treated within the connection pool.  For 
     * example, using a {@link SingleIdentity} specifies that ALL contexts are treated
     * equally and thus use the same pool.
     * 
     * If single identity is not supported then an exception should be thrown when a
     * null context is supplied.
     * 
     * Implementors of this class may use a different implementation of the 
     * {@link ConnectorIdentity} interface to similarly affect pooling.
     *  
     * @param context The context provided by the Connector Manager
     * @return The associated connector identity
     * @throws ConnectorException If a null context is not accepted or an error occurs while creating the identity.
     */
    ConnectorIdentity createIdentity(SecurityContext context) throws ConnectorException;
    
}
