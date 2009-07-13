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

package org.teiid.metadata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.connector.metadata.IndexFile;
import org.teiid.connector.metadata.MetadataConnectorConstants;
import org.teiid.connector.metadata.MultiObjectSource;
import org.teiid.connector.metadata.PropertyFileObjectSource;
import org.teiid.connector.metadata.runtime.ConnectorMetadata;
import org.teiid.connector.metadata.runtime.DatatypeRecordImpl;
import org.teiid.metadata.index.IndexMetadataStore;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.DataTypeManager.DefaultDataTypes;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.connector.metadata.internal.IObjectSource;
import com.metamatrix.core.CoreConstants;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.service.DataService;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.metadata.runtime.api.MetadataSource;
import com.metamatrix.query.metadata.MetadataStore;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.vdb.runtime.VDBKey;


/** 
 * This caches QueryMetadataInterface implementations for all vdbs, each implementation has access to
 * metadata for a given vdb and the system vdb.
 * @since 4.2
 */
public class QueryMetadataCache {
	
	private static class QueryMetadataHolder {
		QueryMetadataInterface qmi;
	}
    
    // vdbID to QueryMetadataInterfaceHolder map
    private Map<VDBKey, QueryMetadataHolder> vdbToQueryMetadata = Collections.synchronizedMap(new HashMap<VDBKey, QueryMetadataHolder>());
    // map between vdbID and CompositeIndexSelector for the vdb (RuntimeSelector for the vdb and system vdb)
    private Map<VDBKey, CompositeMetadataStore> vdbToCompositeSelector = Collections.synchronizedMap(new HashMap<VDBKey, CompositeMetadataStore>());
    // RuntimeIndexSelector for the system vdb    
    private final VDBArchive systemVDBSelector;

    // boolean for the cache being valid
    private boolean isCacheValid = true;
	private IndexMetadataStore indexMetadataStore;
    
    /** 
     * Constructor given a URL to a system vdb. 
     * @since 4.2
     */
    public QueryMetadataCache(final URL systemVdbUrl) throws MetaMatrixComponentException {
        try {
            this.systemVDBSelector = new VDBArchive(systemVdbUrl.openStream());
            this.indexMetadataStore = new IndexMetadataStore(this.systemVDBSelector);
        } catch(IOException e) {
            throw new MetaMatrixComponentException(e, DQPPlugin.Util.getString("QueryMetadataCache.Failed_creating_Runtime_Index_Selector._4", CoreConstants.SYSTEM_VDB));  //$NON-NLS-1$
        }        
    }
    
    /** 
     * Constructor given the contents of a system vdb. 
     * @since 4.2
     */
    public QueryMetadataCache(final byte[] systemVdbContent) throws MetaMatrixComponentException {
        try {
	        this.systemVDBSelector = new VDBArchive(new ByteArrayInputStream(systemVdbContent));
            this.indexMetadataStore = new IndexMetadataStore(this.systemVDBSelector);
	    } catch(IOException e) {
	        throw new MetaMatrixComponentException(e, DQPPlugin.Util.getString("QueryMetadataCache.Failed_creating_Runtime_Index_Selector._4", CoreConstants.SYSTEM_VDB));  //$NON-NLS-1$
	    }        
    }

    /**
     * Get the composite selector fot the given vdbName, version. 
     */
    private CompositeMetadataStore getCompositeSelector(final String vdbName, final String vdbVersion) {
        // check cache status
        assertIsValidCache();
        VDBKey vdbID = toVdbID(vdbName, vdbVersion);
        return this.vdbToCompositeSelector.get(vdbID);
    }
    
    public IObjectSource getCompositeMetadataObjectSource(String vdbName, String vdbVersion, VDBService vdbService){
    	CompositeMetadataStore indexSelector = getCompositeSelector(vdbName, vdbVersion);

		// build up sources to be used by the index connector
		IObjectSource indexFile = new IndexFile(indexSelector, vdbName, vdbVersion, vdbService);

		PropertyFileObjectSource propertyFileSource = new PropertyFileObjectSource();
		IObjectSource multiObjectSource = new MultiObjectSource(indexFile, MetadataConnectorConstants.PROPERTIES_FILE_EXTENSION,propertyFileSource);
    	return multiObjectSource;
    }
    
    /**
     * Look up metadata for the given vdbName, version at the given filecontent.
     * @throws MetaMatrixComponentException 
     */
    public QueryMetadataInterface lookupMetadata(final String vdbName, final String vdbVersion, MetadataSource iss, DataService dataService) throws MetaMatrixComponentException {
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

    private QueryMetadataInterface loadMetadata(final VDBKey vdbID, final MetadataSource runtimeSelector, DataService dataService) throws MetaMatrixComponentException {
        // check cache status
        assertIsValidCache();

        List<MetadataStore> metadataStores = new ArrayList<MetadataStore>();
        try {
			metadataStores.add(new IndexMetadataStore(runtimeSelector));
	        Set<String> modelNames = runtimeSelector.getConnectorMetadataModelNames();
	        if (!modelNames.isEmpty()) {
		        for (String modelName : modelNames) {
		        	ConnectorMetadata connectorMetadata = null;
		        	String savedMetadata = "/META-INF/" + modelName.toLowerCase() + ".ser"; //$NON-NLS-1$ //$NON-NLS-2$
	        		if (runtimeSelector.cacheConnectorMetadata()) {
		        		File f = runtimeSelector.getFile(savedMetadata);
		        		if (f != null) {
		        			ObjectInputStream ois = null;
		        			try {
			        			ois = new ObjectInputStream(new FileInputStream(f));
			        			connectorMetadata = (ConnectorMetadata)ois.readObject();
		        			} catch (Exception e) {
		        				
		        			} finally {
		        				if (ois != null) {
				        			ois.close();
		        				}
		        			}
		        		}
		        	}
		        	if (connectorMetadata == null) {
		        		connectorMetadata = dataService.getConnectorMetadata(vdbID.getName(), vdbID.getVersion(), modelName, runtimeSelector.getModelInfo(modelName).getProperties());
		        	}
		        	if (runtimeSelector.cacheConnectorMetadata()) {
		        		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		        		ObjectOutputStream oos = new ObjectOutputStream(baos);
		        		oos.writeObject(connectorMetadata);
		        		oos.close();
		        		runtimeSelector.saveFile(new ByteArrayInputStream(baos.toByteArray()), savedMetadata);
		        	}
		        	metadataStores.add(new ConnectorMetadataStore(modelName, connectorMetadata));
				}
	        }
			metadataStores.add(indexMetadataStore);
		} catch (IOException e) {
			throw new MetaMatrixComponentException(e);
		}
        // build a composite selector for the runtimeselectors of this vdb and system vdb
        CompositeMetadataStore composite = new CompositeMetadataStore(metadataStores, runtimeSelector);
        vdbToCompositeSelector.put(vdbID, composite);
        QueryMetadataInterface result = new TransformationMetadata(composite);
        return result;        
    }

	public Map<String, DatatypeRecordImpl> getBuiltinDatatypes() throws MetaMatrixComponentException {
		Collection<DatatypeRecordImpl> datatypes = this.indexMetadataStore.getDatatypes();
		Map<String, DatatypeRecordImpl> datatypeMap = new HashMap<String, DatatypeRecordImpl>();
		for (String typeName : DataTypeManager.getAllDataTypeNames()) {
			for (DatatypeRecordImpl datatypeRecordImpl : datatypes) {
				if (datatypeRecordImpl.getName().equals("int")) { //$NON-NLS-1$
					datatypeMap.put(DefaultDataTypes.INTEGER, datatypeRecordImpl);
				} else if (datatypeRecordImpl.getName().equals("XMLLiteral")) { //$NON-NLS-1$
					datatypeMap.put(DataTypeManager.DefaultDataTypes.XML, datatypeRecordImpl);
				} else if (datatypeRecordImpl.getName().equals(typeName)) {
					datatypeMap.put(typeName, datatypeRecordImpl);
				}
			}
		}
		return datatypeMap;
	}

    /**
     * Clears all state on this cache and also deletes any indexfiles
     * associated with the cache.  
     * @since 4.2
     */
    public void clearCache() {
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

        // Clear the cache of selectors ...
        vdbToCompositeSelector.clear();
    }

    /**
     * Remove cache for a given vdb, called when a vdb is actually deleted.
     * Also deletes any temp files associated with the vdb.
     */
    public void removeFromCache(final String vdbName, final String vdbVersion) {
        LogManager.logTrace(LogConstants.CTX_DQP, new Object[] {"QueryMetadataCache Removing vdb from cache", vdbName, vdbVersion});  //$NON-NLS-1$ 
        if(vdbName != null && vdbVersion != null) {
	        final VDBKey vdbID = toVdbID(vdbName, vdbVersion);
            vdbToQueryMetadata.remove(vdbID);
            vdbToCompositeSelector.remove(vdbID);
        }
    }

    /**
     * Return unique id for a vdb
     */
    private VDBKey toVdbID(final String vdbName, final String vdbVersion) {
        return new VDBKey(vdbName, vdbVersion);
    }

}
