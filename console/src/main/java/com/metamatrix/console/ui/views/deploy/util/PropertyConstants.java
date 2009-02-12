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

//#############################################################################
package com.metamatrix.console.ui.views.deploy.util;

import com.metamatrix.common.config.api.HostType;

/**
 * @version 1.0
 * @author Dan Florian
 */
public interface PropertyConstants {

    //
    // Service
    //
    static final String ESSENTIAL_PROP =
        DeployPkgUtils.getString("property.service.essential"); //$NON-NLS-1$

    //
    // Process
    //
//    static final String LOG_PROP =
//        DeployPkgUtils.getString("property.process.logfile"); //$NON-NLS-1$
//    static final String MAX_HEAP_PROP =
//        DeployPkgUtils.getString("property.process.maxheap"); //$NON-NLS-1$
//    static final String MIN_HEAP_PROP =
//        DeployPkgUtils.getString("property.process.minheap"); //$NON-NLS-1$

    //
    // Host
    //
    static final String PORT_PROP = HostType.PORT_NUMBER;
//        DeployPkgUtils.getString("property.host.portnumber"); //$NON-NLS-1$


}
