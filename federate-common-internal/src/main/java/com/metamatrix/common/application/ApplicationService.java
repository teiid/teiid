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

import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;

/**
 * This defines the basic interface for an application service.  The lifecycle 
 * of a service is generally as follows:
 * <ul>
 * <li>Initialize - configure the service</li>
 * <li>Start - performed by application prior to start the service running in an environment</li>
 * <li>Bind - performed by application when the service is bound into the environment</li>
 * <li>Unbind - performed by application prior to unbinding the service from the environment</li>
 * <li>Stop - performed by application after the service is unbound from the environment</li>
 * </ul>
 */
public interface ApplicationService {

    /**
     * Initialize the service with the specified properties.
     * @param props Initialialization properties
     * @throws ApplicationInitializationException If an error occurs during initialization
     */
    public void initialize(Properties props) throws ApplicationInitializationException;

    /**
     * Start the service with the specified environment.  The environment can
     * be used to find other services or resources.
     * @param environment Environment 
     * @throws ApplicationLifecycleException If an error occurs while starting
     */    
    public void start(ApplicationEnvironment environment) throws ApplicationLifecycleException;
    
    /**
     * Bind the service into the environment.
     * @throws ApplicationLifecycleException If an error occurs while binding
     */    
    public void bind() throws ApplicationLifecycleException;
    
    /**
     * Unbind the service from the environment.
     * @throws ApplicationLifecycleException If an error occurs while unbinding
     */    
    public void unbind() throws ApplicationLifecycleException;
    
    /**
     * Stop the service.
     * @throws ApplicationLifecycleException If an error occurs while starting
     */    
    public void stop() throws ApplicationLifecycleException;
    

}
