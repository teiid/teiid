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

package com.metamatrix.common.application;

import java.util.HashMap;
import java.util.Map;

/**
 * The environment is available internally to the application as a means 
 * of finding application services of a particular type or to retrieve
 * other information about the application itself.  
 */
public class ApplicationEnvironment {

    private Map<String, ApplicationService> services = new HashMap<String, ApplicationService>();

    /* 
     * @see com.metamatrix.common.application.ApplicationEnvironment#bindService(java.lang.String, com.metamatrix.common.application.ApplicationService)
     */
    public void bindService(String type, ApplicationService service) {
        this.services.put(type, service);
    }

    /* 
     * @see com.metamatrix.common.application.ApplicationEnvironment#unbindService(java.lang.String)
     */
    public void unbindService(String type) {
        this.services.remove(type);
    }

    /* 
     * @see com.metamatrix.common.application.ApplicationEnvironment#findService(java.lang.String)
     */
    public ApplicationService findService(String type) {
        return this.services.get(type);
    }
    
}
