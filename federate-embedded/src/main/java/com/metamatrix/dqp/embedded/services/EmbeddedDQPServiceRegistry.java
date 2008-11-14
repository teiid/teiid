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

package com.metamatrix.dqp.embedded.services;

import java.util.HashMap;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.dqp.service.DQPServiceRegistry;


/** 
 * This base class for all the configuration services. This hides the registration and 
 * discovery of the other services.
 * @since 4.3
 */
public class EmbeddedDQPServiceRegistry implements DQPServiceRegistry {

    HashMap serviceMap = new HashMap();
    
    /** 
     * @see com.metamatrix.dqp.service.ConfigurationService#registerService(java.lang.String, com.metamatrix.dqp.service.ApplicationService)
     * @since 4.3
     */
    public void registerService(String serviceName, ApplicationService service) 
        throws MetaMatrixComponentException {
        serviceMap.put(serviceName, service);
    }
    
    /** 
     * @see com.metamatrix.dqp.service.DQPServiceRegistry#lookupService(java.lang.String)
     * @since 4.3
     */
    public ApplicationService lookupService(String serviceName) 
        throws MetaMatrixComponentException {
        return (ApplicationService)serviceMap.get(serviceName);
    }
}
