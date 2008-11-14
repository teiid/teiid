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
 * Date: Jun 24, 2003
 * Time: 11:51:15 AM
 */
package com.metamatrix.common.application;

import java.util.Properties;

import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;

/**
 * Interface Application.
 *
 * <p>Contains application lifecycle methods.</p>
 *
 */
public interface Application {

    /* ########## Configuration ########## */

    /**
     * Initialize the application using the specified properties
     * @param props Properties
     * @throws ApplicationInitializationException when an error occurs durring
     * initialization.
     */
    void initialize(Properties props) throws ApplicationInitializationException;

    /**
     * Install an application service, which should have been already been configured
     * by an ApplicationBootstrapper.
     * @param service Service to install
     * @throws ApplicationInitializationException If an error occurs during installation
     */
    void installService(String type, ApplicationService service) throws ApplicationInitializationException;
    

    /* ########## Lifecycle ########## */
    
    /**
     * Start the application.
     */
    void start() throws ApplicationLifecycleException;

    /**
     * Stop the application.
     */
    void stop() throws ApplicationLifecycleException;
    
}
