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

package com.metamatrix.platform.config.spi;

import com.metamatrix.common.config.api.Configuration;

/**
 * <p>This interface has the labels of the three well-known system
 * configurations:
 * <ol>
 * <li> The {@link #OPERATIONAL} config
 * <li> The {@link #NEXT_STARTUP} config
 * <li> The {@link #STARTUP} config
 * </ol>
 * These three labels can be used by an service provider to designate
 * any stored configurations as one of the three.
 * </p>
 */
public interface SystemConfigurationNames {

    /**
     * The name of the Operational system configuration, which models
     * the desired runtime state of the system
     */
//    public static final String OPERATIONAL = Configuration.OPERATIONAL;

    /**
     * The name of the Next Startup system configuration, which is a model
     * of how the system should next start up
     */
    public static final String NEXT_STARTUP = Configuration.NEXT_STARTUP;

    /**
     * The name of the Next Startup system configuration, which is a model
     * of how the system started up
     */
    public static final String STARTUP = Configuration.STARTUP;

}

