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

package com.metamatrix.dqp.service;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationService;


/** 
 * Registry interface for the DQP Services
 * @since 4.3
 */
public interface DQPServiceRegistry {

    /**
     * Register other services with the configuration service, so that they get
     * notified about the events such as VDB being added, deleted etc. 
     * @param serviceName - Name of the service
     * @param service - Service
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    public void registerService(String serviceName, ApplicationService service) 
        throws MetaMatrixComponentException;
    
    /**
     * Find a service registered with Config Service
     * @param serviceName
     * @return ApplicationService
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    public ApplicationService lookupService(String serviceName) 
        throws MetaMatrixComponentException;
          
}
