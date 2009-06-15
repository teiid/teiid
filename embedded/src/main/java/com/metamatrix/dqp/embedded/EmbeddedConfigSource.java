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

package com.metamatrix.dqp.embedded;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.teiid.dqp.internal.cache.DQPContextCache;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.application.DQPConfigSource;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.dqp.embedded.services.EmbeddedBufferService;
import com.metamatrix.dqp.embedded.services.EmbeddedConfigurationService;
import com.metamatrix.dqp.embedded.services.EmbeddedDataService;
import com.metamatrix.dqp.embedded.services.EmbeddedMetadataService;
import com.metamatrix.dqp.embedded.services.EmbeddedTransactionService;
import com.metamatrix.dqp.embedded.services.EmbeddedVDBService;
import com.metamatrix.dqp.service.DQPServiceNames;

/**
 * This class is main hook point for the Embedded DQP configuration. This classe's
 * responsibility is to encapsulate the knowledge of creating of the various application
 * services used the DQP.
 * 
 */
@Singleton
public class EmbeddedConfigSource implements DQPConfigSource {
    
	private Properties props;
    private boolean useTxn;
    private URL dqpURL;
    
    @Inject
    DQPContextCache contextCache;
    
    /**  
    * Based the configuration file load the DQP services
    * @param configFile
    * @throws ApplicationInitializationException
    */    
    @Inject public EmbeddedConfigSource(@Named("BootstrapURL") URL dqpURL, @Named("DQPProperties") Properties connectionProperties) {
        this.dqpURL = dqpURL;
        this.props = connectionProperties;
        useTxn = PropertiesUtils.getBooleanProperty(props, EmbeddedTransactionService.TRANSACTIONS_ENABLED, true);
    }  

    /** 
     * @see com.metamatrix.common.application.DQPConfigSource#getProperties()
     */
    public Properties getProperties() {
        return this.props;
    }
    
	@Override
	public void updateBindings(Binder binder) {
		if (contextCache != null) {
			binder.bind(DQPContextCache.class).toInstance(contextCache);
		}
		binder.bind(URL.class).annotatedWith(Names.named("BootstrapURL")).toInstance(this.dqpURL); //$NON-NLS-1$
	}

	@Override
	public Map<String, Class<? extends ApplicationService>> getDefaultServiceClasses() {
		Map<String, Class<? extends ApplicationService>> result = new HashMap<String, Class<? extends ApplicationService>>();
		result.put(DQPServiceNames.CONFIGURATION_SERVICE, EmbeddedConfigurationService.class);
		result.put(DQPServiceNames.BUFFER_SERVICE, EmbeddedBufferService.class);
		result.put(DQPServiceNames.VDB_SERVICE, EmbeddedVDBService.class);
		result.put(DQPServiceNames.METADATA_SERVICE, EmbeddedMetadataService.class);
		result.put(DQPServiceNames.DATA_SERVICE, EmbeddedDataService.class);
		if (useTxn) {
			result.put(DQPServiceNames.TRANSACTION_SERVICE, EmbeddedTransactionService.class);
		}
		return result;
	}

}
