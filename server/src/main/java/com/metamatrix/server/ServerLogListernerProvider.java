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

import java.io.FileNotFoundException;
import java.util.EventObject;
import java.util.Properties;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.log.DbLogListener;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.common.messaging.MessagingException;
import com.metamatrix.common.util.LogContextsUtil;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.event.EventObjectListener;
import com.metamatrix.core.log.FileLimitSizeLogWriter;
import com.metamatrix.core.log.LogListener;
import com.metamatrix.internal.core.log.PlatformLog;
import com.metamatrix.platform.config.event.ConfigurationChangeEvent;

@Singleton
class ServerLogListernerProvider extends FileLogListenerProvider {
    public static final String LOG_DB_ENABLED = "metamatrix.log.jdbcDatabase.enabled"; //$NON-NLS-1$
	
	@Inject
	MessageBus messsgeBus;
	
	@Override
	public LogListener get() {
        
		final PlatformLog log = new PlatformLog();

		try {
			FileLimitSizeLogWriter flw = buildFileLogger();		
			log.addListener(flw);
			
		} catch (FileNotFoundException e) {
			throw new MetaMatrixRuntimeException(e);
		}
				
		final DbLogListener dbLogger = buildDBLogger();
		log.addListener(dbLogger);	
        
		try {
			
			this.messsgeBus.addListener(ConfigurationChangeEvent.class, new EventObjectListener() {
			
				public void processEvent(EventObject obj) {
					if(obj instanceof ConfigurationChangeEvent){
						if(obj instanceof ConfigurationChangeEvent){
							try {
								Configuration currentConfig = CurrentConfiguration.getInstance().getConfiguration();
								Properties props = currentConfig.getProperties();
								boolean enabled = PropertiesUtils.getBooleanProperty(props, LOG_DB_ENABLED, true);
								dbLogger.enableDBLogging(enabled);
							} catch( ConfigurationException ce ) {
								LogManager.logError(LogContextsUtil.CommonConstants.CTX_MESSAGE_BUS, ce, ce.getMessage());
							}
						}
					}
				}					
			});
			
		} catch (MessagingException e) {
			throw new MetaMatrixRuntimeException(e);
		}	
		return log;
	}

	private DbLogListener buildDBLogger() {
		Properties currentProps = CurrentConfiguration.getInstance().getProperties();
		Properties resultsProps = PropertiesUtils.clone(currentProps, null, true, false);
		return new DbLogListener(resultsProps);
	}
	
}
