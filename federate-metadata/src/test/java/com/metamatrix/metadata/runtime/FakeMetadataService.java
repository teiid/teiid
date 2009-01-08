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

package com.metamatrix.metadata.runtime;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.connector.metadata.IndexFile;
import com.metamatrix.connector.metadata.MetadataConnectorConstants;
import com.metamatrix.connector.metadata.MultiObjectSource;
import com.metamatrix.connector.metadata.PropertyFileObjectSource;
import com.metamatrix.connector.metadata.internal.IObjectSource;
import com.metamatrix.core.util.TempDirectoryMonitor;
import com.metamatrix.dqp.service.FakeVDBService;
import com.metamatrix.dqp.service.MetadataService;
import com.metamatrix.dqp.service.metadata.IndexSelectorSource;
import com.metamatrix.modeler.core.index.IndexSelector;
import com.metamatrix.modeler.internal.core.index.CompositeIndexSelector;
import com.metamatrix.modeler.internal.core.index.RuntimeIndexSelector;
import com.metamatrix.modeler.transformation.metadata.ServerMetadataFactory;
import com.metamatrix.query.metadata.QueryMetadataInterface;


public class FakeMetadataService implements ApplicationService, MetadataService, IndexSelectorSource {
    private IndexSelector systemIndexSelector;
    private RuntimeIndexSelector runtimeIndexSelector;
    private boolean useOnlySystemVdb = false;
    
    
    public FakeMetadataService(String vdbFile){
    	runtimeIndexSelector =  new RuntimeIndexSelector(vdbFile);
    }

    public FakeMetadataService(URL vdbFile) throws IOException {
    	runtimeIndexSelector =  new RuntimeIndexSelector(vdbFile);
    }

    
    public void setSystemIndexSelector(IndexSelector systemIndexSelector) {
        this.systemIndexSelector = systemIndexSelector;
    }
    
    public void useOnlySystemVdb() {
        useOnlySystemVdb = true;
    }

    private IndexSelector getIndexSelector(String vdbName, String vdbVersion) throws MetaMatrixComponentException {
        TempDirectoryMonitor.turnOn();
        
        IndexSelector vdbSelector = runtimeIndexSelector;
        if (useOnlySystemVdb) {
            return systemIndexSelector;
        }
        if (systemIndexSelector == null) {
            return vdbSelector;
        }
        ArrayList selectors = new ArrayList();
        selectors.add(vdbSelector);
        //selectors.add(systemIndexSelector);
        CompositeIndexSelector result = new CompositeIndexSelector(selectors);

        return result;
    }
    
    public void clear() {
        if (runtimeIndexSelector != null) {
            runtimeIndexSelector.clearVDB();
            runtimeIndexSelector = null;
        }
        TempDirectoryMonitor.removeAll();
    }

    public void initialize(Properties props) throws ApplicationInitializationException {

    }

    public void start(ApplicationEnvironment environment) throws ApplicationLifecycleException {

    }

    public void stop() throws ApplicationLifecycleException {

    }

	public QueryMetadataInterface lookupMetadata(String vdbName,String vdbVersion) throws MetaMatrixComponentException {
		return ServerMetadataFactory.getInstance().createCachingServerMetadata(runtimeIndexSelector);
	}

	public IObjectSource getMetadataObjectSource(String vdbName,String vdbVersion) throws MetaMatrixComponentException {
		IndexSelector indexSelector = getIndexSelector(vdbName, vdbVersion);
		
		// build up sources to be used by the index connector
		IObjectSource indexFile = new IndexFile(indexSelector, vdbName, vdbVersion, new FakeVDBService());

		PropertyFileObjectSource propertyFileSource = new PropertyFileObjectSource();
		IObjectSource multiObjectSource = new MultiObjectSource(indexFile, MetadataConnectorConstants.PROPERTIES_FILE_EXTENSION,propertyFileSource);

		// return an adapter object that has access to all sources
		return multiObjectSource;	
	}
}
