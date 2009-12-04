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

package com.metamatrix.dqp.embedded.services;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.teiid.connector.metadata.runtime.Datatype;
import org.teiid.connector.metadata.runtime.MetadataStore;
import org.teiid.metadata.CompositeMetadataStore;
import org.teiid.metadata.TransformationMetadata;
import org.teiid.metadata.index.IndexMetadataFactory;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.core.CoreConstants;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.service.ConfigurationService;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.DataService;
import com.metamatrix.dqp.service.MetadataService;
import com.metamatrix.dqp.service.VDBLifeCycleListener;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.metadata.runtime.api.MetadataSource;
import com.metamatrix.vdb.runtime.VDBKey;


/** 
 * @since 4.3
 */
public class EmbeddedMetadataService extends EmbeddedBaseDQPService implements MetadataService {
        
    private VDBLifeCycleListener listener = new VDBLifeCycleListener() {
        public void loaded(String vdbName, String vdbVersion) {
        }
        public void unloaded(String vdbName, String vdbVersion) {
        	LogManager.logTrace(LogConstants.CTX_DQP, new Object[] {"QueryMetadataCache Removing vdb from cache", vdbName, vdbVersion});  //$NON-NLS-1$ 
            if(vdbName != null && vdbVersion != null) {
    	        final VDBKey vdbID = toVdbID(vdbName, vdbVersion);
                vdbToQueryMetadata.remove(vdbID);
            }
        }            
    };
    
	private static class QueryMetadataHolder {
		TransformationMetadata qmi;
	}
    
    // vdbID to QueryMetadataInterfaceHolder map
    private Map<VDBKey, QueryMetadataHolder> vdbToQueryMetadata = Collections.synchronizedMap(new HashMap<VDBKey, QueryMetadataHolder>());
    // RuntimeIndexSelector for the system vdb    
    private VDBArchive systemVDBSelector;

    // boolean for the cache being valid
    private boolean isCacheValid = true;
	private MetadataStore systemMetadataStore;
    
    /**
     * Look up metadata for the given vdbName, version at the given filecontent.
     * @throws MetaMatrixComponentException 
     */
    private TransformationMetadata lookupMetadata(final String vdbName, final String vdbVersion, MetadataSource iss, DataService dataService) throws MetaMatrixComponentException {
    	assertIsValidCache();        
        VDBKey vdbID = toVdbID(vdbName, vdbVersion);
        QueryMetadataHolder qmiHolder = null;
        // Enter a synchronized block to find the holder of a QueryMetadataInterface for a VDB
        synchronized(vdbToQueryMetadata) {
            qmiHolder = vdbToQueryMetadata.get(vdbID);
            if ( qmiHolder == null ) {
            	qmiHolder = new QueryMetadataHolder();
                vdbToQueryMetadata.put(vdbID, qmiHolder);
            }
        }
        synchronized (qmiHolder) {
        	if (qmiHolder.qmi == null) {
        		qmiHolder.qmi = loadMetadata(vdbID, iss, dataService);
        	}
		}
        return qmiHolder.qmi;
    }
    
    private void assertIsValidCache() {
        if(!this.isCacheValid) {
            throw new MetaMatrixRuntimeException(DQPPlugin.Util.getString("QueryMetadataCache.cache_not_valid"));             //$NON-NLS-1$
        }
    }

    private TransformationMetadata loadMetadata(final VDBKey vdbID, final MetadataSource runtimeSelector, DataService dataService) throws MetaMatrixComponentException {
        // check cache status
        assertIsValidCache();

        List<MetadataStore> metadataStores = new ArrayList<MetadataStore>();
        try {
			metadataStores.add(loadMetadataStore(runtimeSelector));
	        Set<String> modelNames = runtimeSelector.getConnectorMetadataModelNames();
	        if (!modelNames.isEmpty()) {
		        for (String modelName : modelNames) {
		        	MetadataStore connectorMetadata = null;
		        	String savedMetadata = "/runtime-inf/" + modelName.toLowerCase() + ".ser"; //$NON-NLS-1$ //$NON-NLS-2$
	        		if (runtimeSelector.cacheConnectorMetadata()) {
		        		connectorMetadata = loadMetadataStore(runtimeSelector, savedMetadata);
		        	}
		        	if (connectorMetadata == null) {
		        		connectorMetadata = dataService.getConnectorMetadata(vdbID.getName(), vdbID.getVersion(), modelName, runtimeSelector.getModelInfo(modelName).getProperties());
		        	}
		        	if (runtimeSelector.cacheConnectorMetadata()) {
		        		saveMetadataStore(runtimeSelector, connectorMetadata, savedMetadata);
		        	}
		        	metadataStores.add(connectorMetadata);
				}
	        }
			metadataStores.add(systemMetadataStore);
		} catch (IOException e) {
			throw new MetaMatrixComponentException(e);
		}
        // build a composite selector for the runtimeselectors of this vdb and system vdb
        CompositeMetadataStore composite = new CompositeMetadataStore(metadataStores, runtimeSelector);
        return new TransformationMetadata(composite);
    }
    
	private void saveMetadataStore(final MetadataSource runtimeSelector,
			MetadataStore connectorMetadata, String savedMetadata)
			throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(connectorMetadata);
		oos.close();
		runtimeSelector.saveFile(new ByteArrayInputStream(baos.toByteArray()), savedMetadata);
	}
	
	private MetadataStore loadMetadataStore(final MetadataSource vdb) throws IOException {
    	String savedMetadata = "/runtime-inf/" + vdb.getName().toLowerCase() + ".vdb.ser"; //$NON-NLS-1$ //$NON-NLS-2$
		MetadataStore store = loadMetadataStore(vdb, savedMetadata);
		if (store == null) {
			store = new IndexMetadataFactory(vdb).getMetadataStore();
			saveMetadataStore(vdb, store, savedMetadata);
		}
		return store;
	}

	private MetadataStore loadMetadataStore(final MetadataSource runtimeSelector, String savedMetadata) throws IOException {
		File f = runtimeSelector.getFile(savedMetadata);
		if (f != null) {
			ObjectInputStream ois = null;
			try {
				ois = new ObjectInputStream(new FileInputStream(f));
				return (MetadataStore)ois.readObject();
			} catch (Throwable e) {
				LogManager.logDetail(LogConstants.CTX_DQP, e, "invalid metadata in file", savedMetadata);  //$NON-NLS-1$
			} finally {
				if (ois != null) {
					ois.close();
				}
			}
		}
		return null;
	}

	public Map<String, Datatype> getBuiltinDatatypes() {
		Collection<Datatype> datatypes = this.systemMetadataStore.getDatatypes();
		Map<String, Datatype> datatypeMap = new HashMap<String, Datatype>();
		for (Class<?> typeClass : DataTypeManager.getAllDataTypeClasses()) {
			for (Datatype datatypeRecordImpl : datatypes) {
				if (datatypeRecordImpl.getJavaClassName().equals(typeClass.getName())) {
					datatypeMap.put(DataTypeManager.getDataTypeName(typeClass), datatypeRecordImpl);
					break;
				}
			}
		}
		return datatypeMap;
	}

    /**
     * Return unique id for a vdb
     */
    private VDBKey toVdbID(final String vdbName, final String vdbVersion) {
        return new VDBKey(vdbName, vdbVersion);
    }


    /** 
     * @see com.metamatrix.dqp.embedded.services.EmbeddedBaseDQPService#initializeService(java.util.Properties)
     * @since 4.3
     */
    public void initializeService(Properties properties) throws ApplicationInitializationException {
    }

    /** 
     * @see com.metamatrix.dqp.embedded.services.EmbeddedBaseDQPService#startService(com.metamatrix.common.application.ApplicationEnvironment)
     * @since 4.3
     */
    public void startService(ApplicationEnvironment environment) throws ApplicationLifecycleException {
        try {
            ConfigurationService configSvc = this.getConfigurationService();
            this.systemVDBSelector = VDBArchive.loadVDB(configSvc.getSystemVdb(), configSvc.getWorkDir());
            this.systemMetadataStore = loadMetadataStore(this.systemVDBSelector);
            configSvc.register(listener);
        } catch (IOException e) {
            throw new ApplicationLifecycleException(e, DQPPlugin.Util.getString("QueryMetadataCache.Failed_creating_Runtime_Index_Selector._4", CoreConstants.SYSTEM_VDB));  //$NON-NLS-1$
        } catch (MetaMatrixComponentException e) {
        	throw new ApplicationLifecycleException(e, DQPPlugin.Util.getString("QueryMetadataCache.Failed_creating_Runtime_Index_Selector._4", CoreConstants.SYSTEM_VDB));  //$NON-NLS-1$
        }
    }

    /** 
     * @see com.metamatrix.dqp.embedded.services.EmbeddedBaseDQPService#stopService()
     * @since 4.3
     */
    public void stopService() throws ApplicationLifecycleException {
    	getConfigurationService().unregister(this.listener);
    	LogManager.logTrace(LogConstants.CTX_DQP, new Object[] {"QueryMetadataCache Clearing VDB cache"});  //$NON-NLS-1$
        // mark cache invalid
        isCacheValid = false;
        // Clear the holders ...
        vdbToQueryMetadata.clear();

        // Clean up the directory for the System VDB ...
        if (this.systemVDBSelector != null) {
            // selector should no longer be used
            this.systemVDBSelector.close();
        }
    }

    /** 
     * @see com.metamatrix.dqp.service.MetadataService#lookupMetadata(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public TransformationMetadata lookupMetadata(String vdbName, String vdbVersion) 
        throws MetaMatrixComponentException {
    	VDBService vdbService = ((VDBService)lookupService(DQPServiceNames.VDB_SERVICE));
    	DataService dataService = ((DataService)lookupService(DQPServiceNames.DATA_SERVICE));
		return lookupMetadata(vdbName, vdbVersion, vdbService.getVDB(vdbName, vdbVersion), dataService);
    }
    

	public CompositeMetadataStore getMetadataObjectSource(String vdbName, String vdbVersion) throws MetaMatrixComponentException {
		return lookupMetadata(vdbName, vdbVersion).getMetadataStore();
	}
	
}
