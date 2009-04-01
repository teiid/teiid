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

package com.metamatrix.common.config.api;


public interface Host extends ComponentDefn {
    public static final ComponentTypeID HOST_COMPONENT_TYPE_ID = new ComponentTypeID(HostType.COMPONENT_TYPE_NAME);
    
    /**
     *  Returns the directory where log files created on this
     *  host machine should be located.
     *  This maybe null, therfore, the logfile will goto the 
     *  current directory.
     * @return
     * @since 4.3
     */

    String getLogDirectory();
    
    /**
     *  Returns the directory where data files created on this
     *  host machine should be located.
     *  This maybe null, therfore, the data files will goto the 
     *  current directory.
     * @return
     * @since 4.3
     */    
    String getDataDirectory();
    
    /**
     *  Returns the temp directory that is used when setting
     *  the java.io.tmpdir envornment variable
     * @return
     * @since 4.3
     */    
    String getTempDirectory();    
    
        
    /**
     * Return the address that should be used to bind to the host 
     * @return
     * @since 4.3
     */
    String getBindAddress();
    
    /**
     * Return the physical host address.  The physical may or may not 
     * be the same as the logical host name (@see #getID().getFullName()) 
     * @return
     * @since 4.3
     */
    String getHostAddress();   
    
    String getConfigDirectory();
    
}

