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

/*
 * Date: Sep 10, 2003
 * Time: 3:58:41 PM
 */
package org.teiid.dqp.internal.datamgr.impl;

import java.util.Properties;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.teiid.connector.api.CacheScope;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.ConnectorPropertyNames;
import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.language.ILanguageFactory;
import org.teiid.dqp.internal.cache.DQPContextCache;
import org.teiid.dqp.internal.datamgr.language.LanguageFactoryImpl;
import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.cache.Cache;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.queue.WorkerPool;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.service.BufferService;
import com.metamatrix.dqp.service.DQPServiceNames;

/**
 * Default Connector Environment. 
 */
public class ConnectorEnvironmentImpl implements ConnectorEnvironment {
    
    private final class ContextClassLoaderPreservingRunnable implements
			Runnable {
		private final Runnable arg0;
		private final ClassLoader cl = Thread.currentThread().getContextClassLoader();

		private ContextClassLoaderPreservingRunnable(Runnable arg0) {
			this.arg0 = arg0;
		}

		@Override
		public void run() {
			ClassLoader current = Thread.currentThread().getContextClassLoader();
			Thread.currentThread().setContextClassLoader(cl);
			try {
				arg0.run();
			} finally {
				Thread.currentThread().setContextClassLoader(current);
			}
		}
	}

	private static final TypeFacility TYPE_FACILITY = new TypeFacilityImpl();
    
    private ConnectorLogger logger;
    private Properties properties;
    private ApplicationEnvironment env;
    private WorkerPool workerPool;
    
    
    public ConnectorEnvironmentImpl(Properties connectorProperties, ConnectorLogger logger, ApplicationEnvironment env) {
    	this(connectorProperties, logger, env, null);
    }
    /**
     * ctor  
     * @param connectorProperties - Properties required for this Connector
     * @param logger - Logger to be used by the Connector
     * @param env - Connector Environment.
     */
    public ConnectorEnvironmentImpl(Properties connectorProperties, ConnectorLogger logger, ApplicationEnvironment env, WorkerPool workerPool) {
        this.properties = connectorProperties;
        this.logger = logger;
        this.env = env;
        this.workerPool = workerPool;
    }
    
    /**  
     * @see org.teiid.connector.api.ConnectorEnvironment#getProperties()
     */
    public Properties getProperties() {
        return this.properties;
    }

    /**  
     * @see org.teiid.connector.api.ConnectorEnvironment#getConnectorName() 
     */
    public String getConnectorName() {
        return this.properties.getProperty(ConnectorPropertyNames.CONNECTOR_BINDING_NAME);
    }

    /**
     * Aquire the logger that the connector using this environment will
     * use to log messages.
     * @return The {@link com.metamatrix.data.ConnectorLogger} for this Connector.
     */
    public ConnectorLogger getLogger() {
        return this.logger;
    }
   
    /**
     * Implement the InternalConnectorEnvironment to allow access for internal connectors
     * to standard Connector Manager resources.  For now this is just access to other
     * connector manager services.
     * @param resourceName Resource name - for now only valid names are DQP service names
     * @return The service as requested 
     * @see com.metamatrix.dqp.datamgr.InternalConnectorEnvironment#findResource(java.lang.String)
     */
    public Object findResource(String resourceName) {
        return env.findService(resourceName);
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorEnvironment#getLanguageFactory()
     */
    public ILanguageFactory getLanguageFactory() {
        return LanguageFactoryImpl.INSTANCE;
    }

    /**  
     * @see org.teiid.connector.api.ConnectorEnvironment#getTypeFacility()
     */
    public TypeFacility getTypeFacility() {
        return TYPE_FACILITY;
    }

	@Override
	public void execute(Runnable command) {
		if (this.workerPool != null) {
			this.workerPool.execute(new ContextClassLoaderPreservingRunnable(command));
		} else {
			command.run();
		}
	}               
	
	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
			long initialDelay, long period, TimeUnit unit) {
		if (this.workerPool != null) {
			return this.workerPool.scheduleAtFixedRate(new ContextClassLoaderPreservingRunnable(command), initialDelay, period, unit);
		}
		return null;
	}
	
	@Override
	public Object getFromCache(CacheScope scope, Object key) {
		DQPWorkContext context = DQPWorkContext.getWorkContext();
		checkScopeValidity(scope, context);

		Cache cache = getScopedCache(scope, context);
		if (cache != null) {
			return cache.get(key);
		}
		return null;
	}
	
	@Override
	public void storeInCache(CacheScope scope, Object key, Object value) {
		DQPWorkContext context = DQPWorkContext.getWorkContext();
		checkScopeValidity(scope, context);
		Cache cache = getScopedCache(scope, context);
		if (cache != null) {
			cache.put(key, value);
		}
	}
	
	private Cache getScopedCache(CacheScope scope, DQPWorkContext context) {
		BufferService service = (BufferService) findResource(DQPServiceNames.BUFFER_SERVICE);
		if (service != null) {
			DQPContextCache contextCache = service.getContextCache();
			switch (scope) {
			case SERVICE:
				return contextCache.getServiceScopedCache(properties.getProperty(ConnectorPropertyNames.CONNECTOR_ID));
			case SESSION:
				return contextCache.getSessionScopedCache(context.getSessionToken().getSessionIDValue());
			case VDB:
				return contextCache.getVDBScopedCache(context.getVdbName(), context.getVdbVersion());
			case GLOBAL:
				return contextCache.getGlobalScopedCache();
			}
		}
		return null;
	}
	
	
	private void checkScopeValidity(CacheScope scope, DQPWorkContext context) {
		if (scope == CacheScope.REQUEST) {
			throw new IllegalStateException(DQPPlugin.Util.getString("ConnectorEnvironmentImpl.request_scope_error")); //$NON-NLS-1$
		}
		
		if (scope == CacheScope.SESSION) {
			if (context == null || context.getSessionToken() == null) {
				throw new IllegalStateException(DQPPlugin.Util.getString("ConnectorEnvironmentImpl.session_scope_error")); //$NON-NLS-1$
			}
		}
		else if (scope == CacheScope.VDB) {
			if (context == null || context.getVdbName() == null || context.getVdbVersion() == null) {
				throw new IllegalStateException(DQPPlugin.Util.getString("ConnectorEnvironmentImpl.vdb_scope_error")); //$NON-NLS-1$
			}
		}
	}
}
