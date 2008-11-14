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

package com.metamatrix.common.config.bootstrap;

import java.util.Properties;
import java.util.Enumeration;

import com.metamatrix.core.CoreConstants;

/**
 * Interface to abstract how the bootstrap properties are obtained
 * for the current configuration.
 */
public class SystemCurrentConfigBootstrap implements CurrentConfigBootstrap {

    public static final String NO_CONFIGURATION = CoreConstants.NO_CONFIGURATION;

    /**
     * Get the properties that should be used to bootstrap the current configuration.
     * @return the bootstrap properties.
     */
    public Properties getBootstrapProperties() throws Exception {
        return System.getProperties();
    }

    /**
     * Get the properties that should be used to bootstrap the current configuration,
     * using the specified default properties.
     * @return defaults the default bootstrap properties; may be null
     * @return the bootstrap properties.
     */
    public Properties getBootstrapProperties( Properties defaults ) throws Exception {
        Properties system = System.getProperties();
        if ( defaults == null ) {
            return system;
        }

        Properties p = new Properties(defaults);
        Enumeration enumeration = system.propertyNames();
        while (enumeration.hasMoreElements() ) {
            String name = (String) enumeration.nextElement();
            p.setProperty( name, system.getProperty(name) );
        }
        return p;
    }
}
