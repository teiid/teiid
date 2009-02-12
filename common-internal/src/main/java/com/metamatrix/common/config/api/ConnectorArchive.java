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



/** 
 * ConnectorArchive defines a bundle a.k.a zip file for Connector Type.
 * This bundle will package all the nessasary artifacts that would be needed
 * by a given connector type.
 *  
 * @since 4.3.2
 */
public interface ConnectorArchive {

    /**
     * Return the connector types bundled in this connector archive 
     * @return connector type array; never null
     */
    ConnectorBindingType[] getConnectorTypes();

    /**
     * Extention modules needed by the connector type, in order to deploy correctly
     * in the DQP or in server.   
     * @return array of extension modules; or zero length array
     */
    ExtensionModule[] getExtensionModules(ConnectorBindingType type);
    
    
    /**
     * Get the Manifest files contents from the CAF file 
     * @return byte[] contents of the manifest file
     */
    byte[] getManifestContents();
}
