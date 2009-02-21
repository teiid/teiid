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
package com.metamatrix.jdbc;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.metamatrix.common.application.DQPConfigSource;
import com.metamatrix.common.log.LogConfiguration;
import com.metamatrix.common.log.config.BasicLogConfiguration;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;

@Singleton
class LogConfigurationProvider implements Provider<LogConfiguration> {

	@Inject
	DQPConfigSource configSource;
		
	@Override
	public LogConfiguration get() {
        String logLevel = configSource.getProperties().getProperty(DQPEmbeddedProperties.DQP_LOGLEVEL);
        int level = 0;
        if(logLevel != null && logLevel.trim().length() > 0) {
            try {
                level = Integer.parseInt(logLevel);                        
            } catch(NumberFormatException e) {
                throw new MetaMatrixRuntimeException(DQPEmbeddedPlugin.Util.getString("DQPComponent.Unable_to_parse_level") + logLevel);      //$NON-NLS-1$
            }            
        }
        return new BasicLogConfiguration(level);
	}

}
