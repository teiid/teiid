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

package org.teiid.dqp.internal.process;

import java.util.HashMap;
import java.util.Map;

import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.query.QueryPlugin;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.translator.TranslatorException;


/**
 */
public class CachedFinder implements CapabilitiesFinder {

	private static BasicSourceCapabilities SYSTEM_CAPS = new BasicSourceCapabilities();
	
    private ConnectorManagerRepository connectorRepo;
    private VDBMetaData vdb;
    
    private Map<String, SourceCapabilities> userCache = new HashMap<String, SourceCapabilities>();
    
    /**
     * Construct a CacheFinder that wraps another finder
     * @param internalFinder Finder to wrap
     */
    public CachedFinder(ConnectorManagerRepository repo, VDBMetaData vdb) {
        this.connectorRepo = repo;
        this.vdb = vdb;
    	userCache.put(CoreConstants.SYSTEM_MODEL, SYSTEM_CAPS);
    	userCache.put(CoreConstants.ODBC_MODEL, SYSTEM_CAPS);
    	userCache.put(CoreConstants.SYSTEM_ADMIN_MODEL, SYSTEM_CAPS);
    }

    /**
     * Find capabilities used the cache if possible, otherwise do the lookup.
     */
    public SourceCapabilities findCapabilities(String modelName) throws TeiidComponentException {
    	SourceCapabilities caps = userCache.get(modelName);
        if(caps != null) {
            return caps;
        }
        TranslatorException exception = null;
        ModelMetaData model = vdb.getModel(modelName);
        for (String sourceName:model.getSourceNames()) {
        	try {
        		ConnectorManager mgr = this.connectorRepo.getConnectorManager(sourceName);
        		if (mgr == null) {
        			throw new TranslatorException(QueryPlugin.Util.getString("CachedFinder.no_connector_found", sourceName, modelName, sourceName)); //$NON-NLS-1$
        		}
        		caps = mgr.getCapabilities();
        		break;
            } catch(TranslatorException e) {
            	if (exception == null) {
            		exception = e;
            	}
            }        	
        }

        if (exception != null) {
        	throw new TeiidComponentException(exception);
        }
        
        if (caps == null) {
        	throw new TeiidRuntimeException("No sources were given for the model " + modelName); //$NON-NLS-1$
        }
        
        userCache.put(modelName, caps);
        return caps;
    }
        
}
