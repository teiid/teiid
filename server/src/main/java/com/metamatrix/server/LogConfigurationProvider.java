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
package com.metamatrix.server;

import java.util.Collection;
import java.util.EventObject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.log.LogConfiguration;
import com.metamatrix.common.log.LogConfigurationImpl;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.common.messaging.MessagingException;
import com.metamatrix.common.util.LogContextsUtil;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.event.EventObjectListener;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.platform.config.event.ConfigurationChangeEvent;

@Singleton
class LogConfigurationProvider implements Provider<LogConfiguration> {

	private static final String LOG_CONTEXT_PROPERTY_NAME = "metamatrix.log.contexts"; //$NON-NLS-1$	
	private static final String LOG_LEVEL_PROPERTY_NAME = "metamatrix.log"; //$NON-NLS-1$
	
	@Inject
	MessageBus messsgeBus;

	
	@Override
	public LogConfiguration get() {
		
            //final LogConfiguration orig = CurrentConfiguration.getInstance().getConfiguration().getLogConfiguration();
        	Map<String, Integer> contextMap = new HashMap<String, Integer>();
        	final LogConfiguration orig = new LogConfigurationImpl(contextMap);
                        
            try {
				this.messsgeBus.addListener(ConfigurationChangeEvent.class, new EventObjectListener() {
					public void processEvent(EventObject obj) {
						if(obj instanceof ConfigurationChangeEvent){
							try {
								Configuration currentConfig = CurrentConfiguration.getInstance().getConfiguration();
								int level = Integer.parseInt(currentConfig.getProperty(LOG_LEVEL_PROPERTY_NAME));
								String[] contexts = getContext();
								
						    	Map<String, Integer> contextMap = new HashMap<String, Integer>();
						    	for(String context:contexts) {
						    		contextMap.put(context, level);
						    	}
								
								LogConfigurationImpl newConfig = new LogConfigurationImpl(contextMap);
								LogManager.setLogConfiguration(newConfig);
							} catch( ConfigurationException ce ) {
								LogManager.logError(LogContextsUtil.CommonConstants.CTX_MESSAGE_BUS, ce, ce.getMessage());
							}
						}
					}	
					
					private String[] getContext() {
						String[] result = null;
						Collection discardedContexts = null;
						String discardedContextsString = CurrentConfiguration.getInstance().getProperties().getProperty(LOG_CONTEXT_PROPERTY_NAME);
						if (discardedContextsString != null){
							discardedContexts = StringUtil.split(discardedContextsString,","); //$NON-NLS-1$
						}

						//get the Set of all contexts, remove the ones which are
						//currently "discarded"
						Set contextsSet = new HashSet(LogContextsUtil.ALL_CONTEXTS);
						if (discardedContexts != null){
							contextsSet.removeAll(discardedContexts);
						}
						result = new String[contextsSet.size()];
						result = (String[]) contextsSet.toArray(result);
						return result;
					}					
				});
			} catch (MessagingException e) {
				throw new MetaMatrixRuntimeException(e);
			}
			return orig;
	}
}
