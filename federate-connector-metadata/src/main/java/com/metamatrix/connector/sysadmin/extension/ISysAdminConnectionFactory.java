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

/*
 */
package com.metamatrix.connector.sysadmin.extension;


import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;

/**
 * The implementation of the connection factory will provide an instance of ISysAdminSource object
 * that is responsible for connecting to the actual source.
 */
public interface ISysAdminConnectionFactory  {
    
    /**
     * Init is called to enable the factory to perform any pre-processing setup that may
     * be necessary. 
     * @param environment
     * @throws ConnectorException
     * @since 4.3
     */
    void init(final ConnectorEnvironment environment) throws ConnectorException ;
    
    /** 
     * @see com.metamatrix.connector.object.extension.source.BaseSourceConnectionFactory#getObjectSource(com.metamatrix.data.pool.ConnectorIdentity)
     * @since 4.3
     */
    ISysAdminSource getObjectSource(final SecurityContext context) throws ConnectorException ;

}
