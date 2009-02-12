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

package com.metamatrix.common.vdb.api;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ConnectorBinding;

/**
 * Date Dec 3, 2002
 *
 *  To import a VDB, the following information is required:
 *  1. VDB Jar {@link #setVDBJar(Object)}.  This jar will provide
 *     the model and their model info. 
 *  2. Add a model to connector binding mapping {@link #addModelToConnectorMapping(String, String)}
 *     that indicates which binding should be used for that model
 *  3. set VDB name (version will be assign at creation time)
 * 
 *  Optional information:
 *  1. Add a ConnectorBinding {@link #addConnectorBinding(ConnectorBinding)}. If this
 *     already exist in the configration, it will not be loaded.  The
 *     model will reference the existing binding.
 *     If not added, the connector binding mapping will indicate an 
 *     assumed already existing binding.
 *  2. Add a ConnectorType {@link #addConnectorType(ComponentType). If
 *     already exist in the configration, it will not be loaded.  The
 *     binding will reference the existing type.
 */
public interface VDBDefn extends VDBInfo {

    /**
     * This will let the uses stream the VDB contents the way they would need.
     * hanging on to whole byte[] may be costly in terms of memory. 
     * @return
     * @since 4.3
     */ 
    VDBStream getVDBStream();
      

    /**
     * Returns the vdbVersion.
     * @return String
     */
    String getVersion();

    /**
     * Returns the connector types that are used in this VDB.
     * The connector types are optional, if
     * not specified on the import, then the
     * connector type(s) is assumed to already 
     * exist in configuration
     * <key> connector type name <value> ComponentType {@see com.metamatrix.common.config.api.ComponentType }
     */
    Map<String, ComponentType> getConnectorTypes();
    

    /**
     * Returns the component type for the specified componentTypeName. 
     * This is a helper method for mapping the connector binding type
     * to its respective connector component type
     * @param componentTypeName
     * @return ComponentType
     */
    ComponentType getConnectorType(String componentTypeName);    

    /**
     * Returns the connector bindings that are used in this VDB.
     * The connector bindings are optional, if not specified on the import, 
     * then the connector binding(s) is assumed to already 
     * exist in configuration
     * <key> connector binding name <value> ConnectorBinding {@see com.metamatrix.common.config.api.ConnectorBinding }
     * @return Map <ConnectorBindings> of connector types
     */
    Map<String, ConnectorBinding> getConnectorBindings();


    /**
     * Returns the connector binding for the specified connector binding name. 
     * @param routingUUID
     * @return ConnectorBinding
     */
    ConnectorBinding getConnectorBindingByName(String connectorBindingName);
    
    /**
     * Returns the connector binding for the specified routing UUID. 
     * This is a helper method for mapping the modelBindingMappings
     * between the model and the connector binding.
     * @param routingUUID
     * @return ConnectorBinding
     */
    ConnectorBinding getConnectorBindingByRouting(String routingUUID);
    
    
    /**
     * Returns the model to connector binding mappings.  
     * <key> Model Name
     * <value> List of ConnectorBinding routing UUIDs.
     * @return Map 
     */
    Map getModelToBindingMappings();
                            
    /**
     * Returns the collection of all the model names contained
     * in the vdb archive.
     * @return Collection
     * 
     */
    Collection<String> getModelNames(); 
    
           
    /**
     * Get the Materialization model in this VDB if one exists. 
     * @return the Materialization model or <code>null</code> if none exists.
     * @since 4.2
     */
    ModelInfo getMatertializationModel();
    
   
    /**
     * Returns a short indicating if the status of the VirtualDatabase. There are four
     * status of the VirtualDatabase: "Incomplete", "Inactive",  "Active", and "Deleted".
     * @see {@link MetadataConstants.VDB_STATUS VDB_STATUS}
     * @return boolean true indicates marked for deletion
     */
     short getStatus();    
   
     boolean isActiveStatus();
    
    /**
     * Returns <code>true</code> if the VDBDefn was built from a .VDB file
     * that had a validity error; 
     * @return
     * @since 4.2
     */
    boolean doesVDBHaveValidityError();
    
    /**
     * During the load of the VDB, if there are any validity errors reported in the
     * VDB manifest file, they will be loaded into Defn file for observation or for the
     * logging.
     * @return list of errors, empty list if none logged.
     */
    String[] getVDBValidityErrors();
    

    /**
     * Is the given resource file is visible to the user.  
     * @param resourcePath - path in the VDB file
     * @return true if visible; false otherwise
     */
    boolean isVisible(String resourcePath);
       
    
    /**
     * Get the XML contents of data roles defined for the VDB;
     * @return xml contents; null if data roles not defined.
     */
    char[] getDataRoles();
    
    /**
     * Header Properties
     * @return
     */
    Properties getHeaderProperties();
}
