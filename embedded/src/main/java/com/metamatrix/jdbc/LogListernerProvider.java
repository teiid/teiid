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
import java.util.Properties;

import org.apache.log4j.Logger;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.metamatrix.core.log.LogListener;
import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;

@Singleton
class LogListernerProvider implements Provider<LogListener> {
	
	@Inject @Named("DQPProperties")
	Properties props;
	
	@Override
	public LogListener get() {
        String logDirectory = this.props.getProperty(DQPEmbeddedProperties.DQP_LOGDIR);
        String processName = this.props.getProperty(DQPEmbeddedProperties.PROCESSNAME);        

    	File logFile = new File(logDirectory, "teiid_"+processName+".log"); //$NON-NLS-1$ //$NON-NLS-2$
    	System.setProperty("dqp.log4jFile", logFile.getAbsolutePath()); //$NON-NLS-1$ // hack
    	return new Log4jListener();
	}

	/**
	 * Log4J Listener
	 */
	static class Log4jListener implements LogListener{

		@Override
		public void log(int level, String context, Object msg) {
			Logger log4j = Log4JUtil.getLogger(context);
			log4j.log(Log4JUtil.convert2Log4JLevel(level), msg);
		}

		public void log(int level, String context, Throwable t, Object msg) {
			Logger log4j = Log4JUtil.getLogger(context);
			log4j.log(Log4JUtil.convert2Log4JLevel(level), msg, t);
		}
						
		@Override
		public void shutdown() {
		}

	}
}
