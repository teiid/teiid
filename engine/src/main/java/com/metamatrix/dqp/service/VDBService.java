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

package com.metamatrix.dqp.service;

import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseException;

/**
 * This interface defines methods which are specific to dealing with VDBs
 * and their management in a server/dqp.
 */
public interface VDBService extends ApplicationService {
    
    /**
     * Get connector bindings for a particular vdb model.  For most models, there will be exactly one.  For 
     * multi-source models, there may be more than one.  The {@link VDBConfiguration#getMultiSourceModels(String, String)}
     * method can be used to determine which models are multi-source models.
     * @param vdbName VDB name
     * @param vdbVersion VDB version
     * @param modelName Model name
     * @return list of {@link java.lang.String} names of connectors
     */
    public List<String> getConnectorBindingNames(String vdbName, String vdbVersion, String modelName)  
        throws MetaMatrixComponentException;
    
    
    /**
     * Get model visibility
     * @param vdbName VDB name
     * @param vdbVersion VDB version
     * @param modelName The name of the model
     * @return Visibility constant, as defined in this class
     */
    public int getModelVisibility(String vdbName, String vdbVersion, String modelName)  
        throws MetaMatrixComponentException;

    /**
     * Get visibility of the file at the given path in the vdb.
     * @param vdbName VDB name
     * @param vdbVersion VDB version
     * @param pathInVDB The path to the model in the VDB
     * @return Visibility constant, as defined in this class
     */
    public int getFileVisibility(String vdbName, String vdbVersion, String pathInVDB)  
        throws MetaMatrixComponentException;    
    
    /**
     * Get all multi-source model names (models that can be bound to multiple connector bindings) for this VDB
     * name and version. 
     * @param vdbName VDB name
     * @param vdbVersion VDB version
     * @return List<String> of model names that are multi-source models.
     * @since 4.2
     */
    public List getMultiSourceModels(String vdbName, String vdbVersion) 
        throws MetaMatrixComponentException;
    
    /**
     * Get the list of VDBs available from  the service
     * @return List of {@link com.metamatrix.metadata.runtime.admin.vdb.VDBDefn}
     * @since 4.3
     */
    public List<VDBArchive> getAvailableVDBs() 
        throws MetaMatrixComponentException;
            
    /**
     * Change the status of the VDB  
     * @param vdbName - Name of the VDB
     * @param vdbVersion - Version of the VDB
     * @param status - 
     * @since 4.3
     */
    public void changeVDBStatus(String vdbName, String vdbVersion, int status) 
        throws ApplicationLifecycleException, MetaMatrixComponentException;   
    
    // to be removed later..
    public String getConnectorName(String connectorBindingID) throws MetaMatrixComponentException;    
    
    public String getActiveVDBVersion(String vdbName, String vdbVersion) throws MetaMatrixComponentException, VirtualDatabaseException;
    
    public VDBArchive getVDB(String vdbName, String vdbVersion) throws MetaMatrixComponentException;
}
