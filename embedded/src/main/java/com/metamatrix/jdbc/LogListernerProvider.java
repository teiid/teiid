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

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.metamatrix.common.application.DQPConfigSource;
import com.metamatrix.common.protocol.URLHelper;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.log.FileLimitSizeLogWriter;
import com.metamatrix.core.log.JavaLogWriter;
import com.metamatrix.core.log.LogListener;
import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;
import com.metamatrix.internal.core.log.PlatformLog;

@Singleton
class LogListernerProvider implements Provider<LogListener> {
	@Inject
	DQPConfigSource configSource;
	
	@Override
	public LogListener get() {
    	URL dqpURL = (URL)configSource.getProperties().get(EmbeddedDataSource.DQP_BOOTSTRAP_FILE);
        String logFile = configSource.getProperties().getProperty(DQPEmbeddedProperties.DQP_LOGFILE);
        String instanceId = configSource.getProperties().getProperty(DQPEmbeddedProperties.DQP_IDENTITY, "0"); //$NON-NLS-1$        
        
        // Configure Logging            
        try {
        	String dqpURLString = dqpURL.toString(); 
        	dqpURL = URLHelper.buildURL(dqpURLString);
        	if (logFile != null) {
                String modifiedLogFileName = logFile;                    
                int dotIndex = logFile.lastIndexOf('.');
                if (dotIndex != -1) {
                    modifiedLogFileName = logFile.substring(0,dotIndex)+"_"+instanceId+"."+logFile.substring(dotIndex+1); //$NON-NLS-1$ //$NON-NLS-2$
                }
                else {
                    modifiedLogFileName = logFile+"_"+instanceId; //$NON-NLS-1$
                }
                URL logURL = URLHelper.buildURL(dqpURL, modifiedLogFileName);
                File file = new File(logURL.getPath());
                PlatformLog log = new PlatformLog();
                log.addListener(new FileLimitSizeLogWriter(file, false));
                return log;
        	}
    		return new JavaLogWriter();
        } catch (MalformedURLException e) {
        	throw new MetaMatrixRuntimeException(e);
        }
	}

}
