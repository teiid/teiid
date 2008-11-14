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

package com.metamatrix.dqp.internal.process.capabilities;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.vdb.api.VDBDefn;
import com.metamatrix.dqp.internal.process.DQPWorkContext;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.dqp.service.AutoGenDataService;
import com.metamatrix.dqp.service.DataService;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.capabilities.BasicSourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;

/**
 */
public class TestConnectorCapabilitiesFinder extends TestCase {

    /**
     * Constructor for TestConnectorCapabilitiesFinder.
     * @param name
     */
    public TestConnectorCapabilitiesFinder(String name) {
        super(name);
    }

    public void testFind() throws Exception {
        String vdbName = "myvdb"; //$NON-NLS-1$
        String vdbVersion = "1"; //$NON-NLS-1$
        String modelName = "model"; //$NON-NLS-1$
        String functionName = "fakeFunction"; //$NON-NLS-1$
        
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setFunctionSupport("fakeFunction", true); //$NON-NLS-1$
        RequestMessage request = new RequestMessage(null);
        DQPWorkContext workContext = new DQPWorkContext();
        workContext.setVdbName(vdbName);
        workContext.setVdbVersion(vdbVersion);
        
        VDBService vdbService = new TheVdbService(vdbName, vdbVersion, modelName); 
        DataService dataService = new AutoGenDataService(caps);
        ConnectorCapabilitiesFinder finder = new ConnectorCapabilitiesFinder(vdbService, dataService, request, workContext);
        
        // Test
        SourceCapabilities actual = finder.findCapabilities(modelName);
        assertEquals("Did not get expected capabilities", true, actual.supportsFunction(functionName)); //$NON-NLS-1$
    }

    private static class TheVdbService implements VDBService {
//        private String vdbName;
//        private String vdbVersion;
        private String modelName;
        
        public TheVdbService(String vdbName, String vdbVersion, String modelName) {
//            this.vdbName = vdbName;            
//            this.vdbVersion = vdbVersion;
            this.modelName = modelName;
        }
        
        public boolean isActiveVDB(String vdbName, String vdbVersion) throws MetaMatrixComponentException {
            return false;
        }

        public List getConnectorBindingNames(String vdbName, String vdbVersion, String modelName) throws MetaMatrixComponentException {
            if(vdbName.equalsIgnoreCase(vdbName) && vdbVersion.equalsIgnoreCase(vdbVersion)) {
                ArrayList bindings = new ArrayList(1);
                bindings.add(this.modelName);
                return bindings;
            }
            return Collections.EMPTY_LIST;
        }

        public String getConnectorName(String connectorBindingID) {
            // or just return null?
            return connectorBindingID;
        }

        public int getModelVisibility(String vdbName, String vdbVersion, String modelName) throws MetaMatrixComponentException {
            return 0;
        }

        public int getFileVisibility(String vdbName, String vdbVersion, String modelName) throws MetaMatrixComponentException {
            return 0;
        }

        public String getVDBResourceFile(String vdbName, String vdbVersion) throws MetaMatrixComponentException {
            return null;
        }

        public void initialize(Properties props) throws ApplicationInitializationException {
        }

        public void start(ApplicationEnvironment environment) throws ApplicationLifecycleException {
        }

        public void bind() throws ApplicationLifecycleException {
        }

        public void unbind() throws ApplicationLifecycleException {
        }

        public void stop() throws ApplicationLifecycleException {
        }     
        
        public List getMultiSourceModels(String vdbName,
                                         String vdbVersion) throws MetaMatrixComponentException {
            return Collections.EMPTY_LIST;
        }
        public QueryMetadataInterface getQueryMetadata(Properties props, String vdbName,
                                                       String vdbVersion) throws MetaMatrixComponentException {
            return null;
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
         * @see com.metamatrix.dqp.service.VDBService#deployVDB(com.metamatrix.common.vdb.api.VDBDefn)
         * @since 4.3
         */
        public VDBDefn deployVDB(VDBDefn vdb) 
            throws ApplicationLifecycleException,MetaMatrixComponentException {
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
        public void changeVDBStatus(String vdbName,String vdbVersion,int status) throws ApplicationLifecycleException,MetaMatrixComponentException {
        }

        /** 
         * @see com.metamatrix.dqp.service.VDBService#getAvailableVDBs()
         * @since 4.3
         */
        public List getAvailableVDBs() throws MetaMatrixComponentException {
            return null;
        }
    }
    
}
