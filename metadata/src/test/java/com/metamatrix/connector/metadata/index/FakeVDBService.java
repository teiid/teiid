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

package com.metamatrix.connector.metadata.index;

import java.io.InputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.dqp.service.VDBService;


/** 
 * @since 4.3
 */
public class FakeVDBService implements
                           VDBService {
    public Collection publicModels = new HashSet();
    
    public Collection publicFiles = new HashSet();

    /** 
     * @see com.metamatrix.dqp.service.VDBService#getConnectorBindingNames(java.lang.String, java.lang.String, java.lang.String)
     * @since 4.3
     */
    public List getConnectorBindingNames(String vdbName,
                                         String vdbVersion,
                                         String modelName) throws MetaMatrixComponentException {
        return null;
    }

    /** 
     * @see com.metamatrix.dqp.service.VDBService#getModelVisibility(java.lang.String, java.lang.String, java.lang.String)
     * @since 4.3
     */
    public int getModelVisibility(String vdbName,
                                  String vdbVersion,
                                  String modelName) throws MetaMatrixComponentException {
        if(this.publicModels.contains(modelName)) {
            return ModelInfo.PUBLIC;
        }
        return ModelInfo.PRIVATE;
    }

    /** 
     * @see com.metamatrix.dqp.service.VDBService#getFileVisibility(java.lang.String, java.lang.String, java.lang.String)
     * @since 4.3
     */
    public int getFileVisibility(String vdbName,
                                 String vdbVersion,
                                 String pathInVDB) throws MetaMatrixComponentException {
        if(this.publicFiles.contains(pathInVDB)) {
            return ModelInfo.PUBLIC;
        }
        return ModelInfo.PRIVATE;
    }

    /** 
     * @see com.metamatrix.dqp.service.VDBService#getVDBResource(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public InputStream getVDBResource(String vdbName,
                                    String vdbVersion) throws MetaMatrixComponentException {
        return null;
    }

    /** 
     * @see com.metamatrix.dqp.service.VDBService#getMultiSourceModels(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public List getMultiSourceModels(String vdbName,
                                     String vdbVersion) throws MetaMatrixComponentException {
        return null;
    }


    /** 
     * @see com.metamatrix.dqp.service.VDBService#getAvailableVDBs()
     * @since 4.3
     */
    public List getAvailableVDBs() throws MetaMatrixComponentException {
        return null;
    }

    /** 
     * @see com.metamatrix.dqp.service.VDBService#getVDBStatus(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public int getVDBStatus(String vdbName,
                            String vdbVersion) throws MetaMatrixComponentException {
        return 0;
    }

    /** 
     * @see com.metamatrix.dqp.service.VDBService#changeVDBStatus(java.lang.String, java.lang.String, int)
     * @since 4.3
     */
    public void changeVDBStatus(String vdbName,
                                String vdbVersion,
                                int status) throws ApplicationLifecycleException,
                                           MetaMatrixComponentException {
    }

    /** 
     * @see com.metamatrix.dqp.service.VDBService#getConnectorName(java.lang.String)
     * @since 4.3
     */
    public String getConnectorName(String connectorBindingID) {
        return null;
    }

    /** 
     * @see com.metamatrix.common.application.ApplicationService#initialize(java.util.Properties)
     * @since 4.3
     */
    public void initialize(Properties props) throws ApplicationInitializationException {
    }

    /** 
     * @see com.metamatrix.common.application.ApplicationService#start(com.metamatrix.common.application.ApplicationEnvironment)
     * @since 4.3
     */
    public void start(ApplicationEnvironment environment) throws ApplicationLifecycleException {
    }

    /** 
     * @see com.metamatrix.common.application.ApplicationService#stop()
     * @since 4.3
     */
    public void stop() throws ApplicationLifecycleException {
    }
    
    @Override
    public String getActiveVDBVersion(String vdbName, String vdbVersion) {
    	throw new UnsupportedOperationException();
    }
    
    @Override
    public VDBArchive getVDB(String vdbName, String vdbVersion)
    		throws MetaMatrixComponentException {
    	throw new UnsupportedOperationException();
    }

}
