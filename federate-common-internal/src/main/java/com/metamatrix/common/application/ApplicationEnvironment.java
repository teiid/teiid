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

package com.metamatrix.common.application;

import java.util.Properties;

/**
 * The environment is available internally to the application as a means 
 * of finding application services of a particular type or to retrieve
 * other information about the application itself.  
 */
public interface ApplicationEnvironment {

    /**
     * Get the properties used to initialize the application.
     * @return Properties
     */
    public Properties getApplicationProperties();

    /**
     * Bind a service for a particular service type.  If a service
     * is already bound, the existing service is first unbound.
     * @param type The type of service
     * @param service The service instance
     */
    public void bindService(String type, ApplicationService service);
    
    /**
     * Unbind the current service for a particular type.
     * @param type The type of service
     */
    public void unbindService(String type);
    
    /**
     * Find a service of the specified type.  Return null if none exists.
     * @param type Type of service
     * @return Service if one exists, null otherwise
     */
    public ApplicationService findService(String type);
}
