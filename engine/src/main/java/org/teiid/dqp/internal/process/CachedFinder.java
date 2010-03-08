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
import org.teiid.connector.api.ConnectorException;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManager;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManagerRepository;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.core.CoreConstants;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.query.optimizer.capabilities.BasicSourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;

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
    }

    /**
     * Find capabilities used the cache if possible, otherwise do the lookup.
     */
    public SourceCapabilities findCapabilities(String modelName) throws MetaMatrixComponentException {
    	SourceCapabilities caps = userCache.get(modelName);
        if(caps != null) {
            return caps;
        }
        ConnectorException exception = null;
        ModelMetaData model = vdb.getModel(modelName);
        for (String sourceName:model.getSourceNames()) {
        	try {
        		ConnectorManager mgr = this.connectorRepo.getConnectorManager(model.getSourceJndiName(sourceName));
        		if (mgr == null) {
        			throw new ConnectorException(DQPPlugin.Util.getString("CachedFinder.no_connector_found", model.getSourceJndiName(sourceName), modelName, sourceName));
        		}
        		caps = mgr.getCapabilities();
        		break;
            } catch(ConnectorException e) {
            	if (exception == null) {
            		exception = e;
            	}
            }        	
        }

        if (exception != null) {
        	throw new MetaMatrixComponentException(exception);
        }
        
        userCache.put(modelName, caps);
        return caps;
    }
        
}
