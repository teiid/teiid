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

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.internal.ConnectorPropertyNames;
import org.teiid.connector.language.ILanguageFactory;
import org.teiid.dqp.internal.datamgr.language.LanguageFactoryImpl;

import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.queue.WorkerPool;

/**
 * Default Connector Environment. 
 */
public class ConnectorEnvironmentImpl implements ConnectorEnvironment {
    
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
	public void execute(Runnable arg0) {
		if (this.workerPool != null) {
			this.workerPool.execute(arg0);
		} else {
			arg0.run();
		}
		
	}               
}
