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

package com.metamatrix.common.config.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.config.api.ConnectorArchive;
import com.metamatrix.common.config.api.ConnectorBindingType;
import com.metamatrix.common.config.api.ExtensionModule;


/** 
 * ConnectorArchive defines a bundle a.k.a zip file for Connector Types.
 * This bundle will package all the nessasary artifacts that would be needed
 * by a given connector types.
 *  
 * @since 4.3.2
 */
public class BasicConnectorArchive implements ConnectorArchive{ 
    ArrayList connectorTypes = new ArrayList();
    HashMap extensionModules = new HashMap(); 
    byte[] manifest = null;
    
    /**
     *  ctor
     */
    public BasicConnectorArchive() {  }
    
    /**
     * Return the connector types in this connector archive 
     * @return connector type[]; never null
     * @throws MetaMatrixComponentException
     */
    public ConnectorBindingType[] getConnectorTypes(){
        return (ConnectorBindingType[])connectorTypes.toArray(new ConnectorBindingType[connectorTypes.size()]);
    }

    /**
     * Extention modules needed by the connector type, in order to deploy correctly
     * in the DQP or in server.   
     * @return array of extension modules; or zero length array
     * @throws MetaMatrixComponentException
     */
    public ExtensionModule[] getExtensionModules(ConnectorBindingType type) {
        List list = (List)extensionModules.get(type);
        if (list != null) {
            return (ExtensionModule[])list.toArray(new ExtensionModule[list.size()]);
        }
        return new ExtensionModule[0];
    }
    
    /**
     * Add a Connector Type to the archive 
     * @param type - adds to the archive
     */
    public void addConnectorType(ConnectorBindingType type) {
        if (type != null) {
            this.connectorTypes.add(type);
        }        
    }
    
    /**
     * Adds the Extension module to the archive. Based on the Extension module name it will
     * be either added to type shared or individual directories underneath the archive.  
     * @param type
     * @param extModule
     */
    public void addExtensionModule(ConnectorBindingType type, ExtensionModule extModule) {
        List list = (List)extensionModules.get(type);
        if (list == null) {
            list = new ArrayList();
        }
                
        // always add to the local listing of the connector type
        list.add(extModule);
        extensionModules.put(type, list);        
    }

    /** 
     * @see com.metamatrix.common.config.api.ConnectorArchive#getManifestContents()
     * @since 4.3
     */
    public byte[] getManifestContents() {
        return this.manifest;
    }
    
    /**
     * This is used by the importer to add the contents of the manifest 
     */
    public void addMainfestContents(byte[] contents) {
        this.manifest = contents;
    }
}
