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

package com.metamatrix.dqp.service.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.connector.metadata.IndexFile;
import com.metamatrix.connector.metadata.MetadataConnectorConstants;
import com.metamatrix.connector.metadata.MultiObjectSource;
import com.metamatrix.connector.metadata.PropertyFileObjectSource;
import com.metamatrix.connector.metadata.internal.IObjectSource;
import com.metamatrix.core.CoreConstants;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.modeler.core.index.IndexSelector;
import com.metamatrix.modeler.internal.core.index.CompositeIndexSelector;
import com.metamatrix.modeler.internal.core.index.RuntimeIndexSelector;
import com.metamatrix.modeler.internal.core.workspace.ModelFileUtil;
import com.metamatrix.modeler.transformation.metadata.ServerMetadataFactory;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.vdb.runtime.VDBKey;


/** 
 * This caches QueryMetadataInterface implementations for all vdbs, each implementation has access to
 * metadata for a given vdb and the system vdb.
 * @since 4.2
 */
public class QueryMetadataCache {
    
    // vdbID to QueryMetadataInterfaceHolder map
    private Map vdbToQueryMetadata = new HashMap();
    // map between vdbID and CompositeIndexSelector for the vdb (RuntimeSelector for the vdb and system vdb)
    private Map vdbToCompositeSelector = new HashMap();
    // map between vdbID and RuntimeIndexSelector for the vdb
    private Map vdbToRuntimeSelector = new HashMap();
    // RuntimeIndexSelector for the system vdb    
    private final RuntimeIndexSelector systemVDBSelector;

    // boolean for the cache being valid
    private boolean isCacheValid = true;

    /** 
     * Constructor givena URL to a system vdb. 
     * @since 4.2
     */
    public QueryMetadataCache(final URL systemVdbUrl) throws MetaMatrixComponentException {
        try {
            this.systemVDBSelector = getRuntimeIndexSelector(systemVdbUrl);
        } catch(Exception e) {
            throw new MetaMatrixComponentException(e, DQPPlugin.Util.getString("QueryMetadataCache.Failed_creating_Runtime_Index_Selector._4", CoreConstants.SYSTEM_VDB));  //$NON-NLS-1$
        }        
    }

    /** 
     * Constructor given the contents of a system vdb. 
     * @since 4.2
     */
    public QueryMetadataCache(final byte[] sysemVdbContent) throws MetaMatrixComponentException {
        try {
	        this.systemVDBSelector = getRuntimeIndexSelector(CoreConstants.SYSTEM_VDB, sysemVdbContent);
	    } catch(Exception e) {
	        throw new MetaMatrixComponentException(e, DQPPlugin.Util.getString("QueryMetadataCache.Failed_creating_Runtime_Index_Selector._4", CoreConstants.SYSTEM_VDB));  //$NON-NLS-1$
	    }        
    }

    /** 
     * Constructor given the filePath to a system vdb. 
     * @since 4.2
     */
    public QueryMetadataCache(final String filePath) throws MetaMatrixComponentException {
        try {
            URL systemVdbUrl = new URL( "file:///" + filePath); //$NON-NLS-1$
            this.systemVDBSelector = getRuntimeIndexSelector(systemVdbUrl);
        } catch(Exception e) {
            throw new MetaMatrixComponentException(e, DQPPlugin.Util.getString("QueryMetadataCache.Failed_creating_Runtime_Index_Selector._4", CoreConstants.SYSTEM_VDB));  //$NON-NLS-1$
        }        
    }

    /**
     * Check if this query metadata cache is valid, if the
     * cache has been cleared it is no longer valid. 
     * @return
     * @since 4.3
     */
    public boolean isValid() {
        return this.isCacheValid;
    }

    /**
     * Get the composite selector fot the given vdbName, version. 
     */
    public IndexSelector getCompositeSelector(final String vdbName, final String vdbVersion) {
        // check cache status
        assertIsValidCache();
        VDBKey vdbID = toVdbID(vdbName, vdbVersion);
        return (IndexSelector) this.vdbToCompositeSelector.get(vdbID);
    }
    
    public IObjectSource getCompositeMetadataObjectSource(String vdbName, String vdbVersion, VDBService vdbService){
		IndexSelector indexSelector = getCompositeSelector(vdbName, vdbVersion);

		// build up sources to be used by the index connector
		IObjectSource indexFile = new IndexFile(indexSelector, vdbName, vdbVersion, vdbService);

		PropertyFileObjectSource propertyFileSource = new PropertyFileObjectSource();
		IObjectSource multiObjectSource = new MultiObjectSource(indexFile, MetadataConnectorConstants.PROPERTIES_FILE_EXTENSION,propertyFileSource);
    	return multiObjectSource;
    }
    
    /**
     * Look up metadata for the given vdbName, version. This method will return null if no cached
     * QMI could be found.
     */
    public QueryMetadataInterface lookupMetadata(final String vdbName, final String vdbVersion) {
        // check cache status
        assertIsValidCache();        
        VDBKey vdbID = toVdbID(vdbName, vdbVersion);
        QueryMetadataInterfaceHolder qmiHolder = (QueryMetadataInterfaceHolder)vdbToQueryMetadata.get(vdbID);
        if(qmiHolder != null) {
	        // Get the QueryMetadataInterface from the holder; this is synchronized 
	        // and will do the loading if required
	        return qmiHolder.getQueryMetadataInteface();
        }
        return null;
    }

    /**
     * Look up metadata for the given vdbName, version at the given filecontent.
     */
    public QueryMetadataInterface lookupMetadata(final String vdbName, final String vdbVersion, final byte[] vdbContent) throws MetaMatrixComponentException{
        // check cache status
        QueryMetadataInterfaceHolder qmiHolder = createMetadataHolder(vdbName, vdbVersion);
        return qmiHolder.getQueryMetadataInteface(vdbContent);
    }
    
    /**
     * Look up metadata for the given vdbName, version at the given filecontent.
     */
    public QueryMetadataInterface lookupMetadata(final String vdbName, final String vdbVersion, final InputStream vdbContent) throws MetaMatrixComponentException{
        QueryMetadataInterfaceHolder qmiHolder = createMetadataHolder(vdbName,vdbVersion);
        return qmiHolder.getQueryMetadataInteface(vdbContent);
    }
    
	private QueryMetadataInterfaceHolder createMetadataHolder(final String vdbName, final String vdbVersion) {
		assertIsValidCache();        
        VDBKey vdbID = toVdbID(vdbName, vdbVersion);
        QueryMetadataInterfaceHolder qmiHolder = null;
        // Enter a synchronized block to find the holder of a QueryMetadataInterface for a VDB
        synchronized(vdbToQueryMetadata) {
            qmiHolder = (QueryMetadataInterfaceHolder)vdbToQueryMetadata.get(vdbID);
            if ( qmiHolder == null ) {
                // Didn't find one, so create it and put back in the map ...
                qmiHolder = new QueryMetadataInterfaceHolder(vdbID,vdbName);
                vdbToQueryMetadata.put(vdbID, qmiHolder);
            }
        }
		return qmiHolder;
	}    
    
     

    private void assertIsValidCache() {
        if(!isValid()) {
            throw new MetaMatrixRuntimeException(DQPPlugin.Util.getString("QueryMetadataCache.cache_not_valid"));             //$NON-NLS-1$
        }
    }

    /**
     * There a QMI holder per vdb, the holder shields metadata lookups by differrent thereads for differrent
     * vdbs from having to wait for metadata being loaded for a single vdb. If there are 2 threads for
     * 2 vdbs, each thread can have their own instance of the holder and load their own metadata without
     * having to wait for other thread to finish.   
     * @since 4.2
     */
    protected class QueryMetadataInterfaceHolder {
        private final VDBKey vdbId;
        private final String vdbName;
        private QueryMetadataInterface qmi;
        protected QueryMetadataInterfaceHolder(VDBKey vdbId, String vdbName) {
            this.vdbId = vdbId;
            this.vdbName = vdbName;
        }
        protected QueryMetadataInterface getQueryMetadataInteface() {
            return this.qmi;
        }
        protected synchronized QueryMetadataInterface getQueryMetadataInteface(final byte[] vdbContent) throws MetaMatrixComponentException {
            if ( this.qmi == null ) {
                this.qmi = QueryMetadataCache.this.loadMetadata(this.vdbId, getRuntimeIndexSelector(this.vdbName, vdbContent));
            }
            return this.qmi;
        }
        protected synchronized QueryMetadataInterface getQueryMetadataInteface(InputStream vdbContent) throws MetaMatrixComponentException {
            if ( this.qmi == null ) {
                this.qmi = QueryMetadataCache.this.loadMetadata(this.vdbId, getRuntimeIndexSelector(this.vdbName, vdbContent));
            }
            return this.qmi;
        }        
    }

    private QueryMetadataInterface loadMetadata(final VDBKey vdbID, final RuntimeIndexSelector runtimeSelector) throws MetaMatrixComponentException {
        // check cache status
        assertIsValidCache();
        vdbToRuntimeSelector.put(vdbID,runtimeSelector);

        // build a composite selector for the runtimeselectors of this vdb and system vdb
        List selectors = new ArrayList(2);
        selectors.add(runtimeSelector);
        if(this.systemVDBSelector != null && this.systemVDBSelector.isValid()) {        
            selectors.add(this.systemVDBSelector);
        }
        IndexSelector composite = new CompositeIndexSelector(selectors);
        vdbToCompositeSelector.put(vdbID, composite);

        QueryMetadataInterface result = ServerMetadataFactory.getInstance().createCachingServerMetadata(composite);
        // being used for performance tuning, not used anymore
//        if (false) {
//            result = new TimingMetadataInterface( result, ServerPerformance.METADATA, ConnectorManagerImpl.getPerformanceStatisticsSource() );
//        }        
        return result;        
    }
    
    private RuntimeIndexSelector getRuntimeIndexSelector(final String vdbName, byte[] vdbContents) {
        RuntimeIndexSelector runtimeSelector = null;
        try {
            String vdbFileName = vdbName + '.' + ModelFileUtil.EXTENSION_VDB;
            runtimeSelector = new RuntimeIndexSelector(vdbFileName, vdbContents);
            // force the extraction of indexes for the vdb            
            runtimeSelector.getIndexes();
        } catch (IOException e) {            
            throw new MetaMatrixRuntimeException(e, DQPPlugin.Util.getString("QueryMetadataCache.Failed_creating_Runtime_Index_Selector._4", vdbName));  //$NON-NLS-1$
        }
        return runtimeSelector;
    }
    
    private RuntimeIndexSelector getRuntimeIndexSelector(final String vdbName, InputStream vdbContents) {
        RuntimeIndexSelector runtimeSelector = null;
        try {
            String vdbFileName = vdbName + '.' + ModelFileUtil.EXTENSION_VDB;
            runtimeSelector = new RuntimeIndexSelector(vdbFileName, vdbContents);
            // force the extraction of indexes for the vdb            
            runtimeSelector.getIndexes();
        } catch (IOException e) {            
            throw new MetaMatrixRuntimeException(e, DQPPlugin.Util.getString("QueryMetadataCache.Failed_creating_Runtime_Index_Selector._4", vdbName));  //$NON-NLS-1$
        }
        return runtimeSelector;
    }    

    private RuntimeIndexSelector getRuntimeIndexSelector(final URL path) {
        RuntimeIndexSelector runtimeSelector = null;
        try {
            runtimeSelector = new RuntimeIndexSelector(path);
            // force the extraction of indexes for the vdb            
            runtimeSelector.getIndexes();
        } catch (IOException e) {            
            throw new MetaMatrixRuntimeException(e, DQPPlugin.Util.getString("QueryMetadataCache.Failed_creating_Runtime_Index_Selector._4", path));  //$NON-NLS-1$
        }
        return runtimeSelector;
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

        // Clean up the selectors (cleans up temp directory for each VDB) ...
        for(final Iterator iter = vdbToRuntimeSelector.values().iterator();iter.hasNext();){
            RuntimeIndexSelector selector = (RuntimeIndexSelector)iter.next();
            // selector should no longer be used
            selector.setValid(false);
            selector.clearVDB();
        }
        vdbToRuntimeSelector.clear();

        // Clean up the directory for the System VDB ...
        if (this.systemVDBSelector != null) {
            // selector should no longer be used
            this.systemVDBSelector.setValid(false);	            
            this.systemVDBSelector.clearVDB();
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
	        // Remove the holder ...
	        QueryMetadataInterfaceHolder qmiHolder = null;
	        RuntimeIndexSelector selector = null;
	        synchronized(vdbToQueryMetadata) {
	            qmiHolder = (QueryMetadataInterfaceHolder)vdbToQueryMetadata.get(vdbID);
	            if ( qmiHolder != null ) {
	                vdbToQueryMetadata.remove(vdbID);
	            }
		        // Remove from the cache of selectors ...
	            vdbToCompositeSelector.remove(vdbID);	// CompositeIndexSelector; should not clean up!
		        selector = (RuntimeIndexSelector)vdbToRuntimeSelector.remove(vdbID); // clean up in a moment
	        }

	        // Clean up the selector (cleans up temp directory for VDB) ...
	        if ( selector != null ) {
	            selector.clearVDB();
	        }
        }
    }

    /**
     * Return unique id for a vdb
     */
    private VDBKey toVdbID(final String vdbName, final String vdbVersion) {
        return new VDBKey(vdbName, vdbVersion);
    }

    /**
     *  Only to be used by UnitTests
     */
    public QueryMetadataInterface testLoadMetadata(final String vdbName, final String vdbVersion, final String filePath) throws MetaMatrixComponentException {
        VDBKey vdbID = toVdbID(vdbName, vdbVersion);
        try {
            return this.loadMetadata(vdbID, getRuntimeIndexSelector(new URL( "file:///" + filePath))); //$NON-NLS-1$
        } catch(Exception e) {
            throw new MetaMatrixComponentException(e);
        }
    }
}
