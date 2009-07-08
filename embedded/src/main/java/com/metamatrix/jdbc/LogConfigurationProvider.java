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

import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.metamatrix.common.log.LogConfiguration;

@Singleton
class LogConfigurationProvider implements Provider<LogConfiguration> {

	@Override
	public LogConfiguration get() {
        return new Log4JLogConfiguration();
	}

	
	static class Log4JLogConfiguration implements LogConfiguration {

		@Override
		public Set<String> getContexts() {
			return Log4JUtil.getContexts();
		}

		@Override
		public int getLogLevel(String context) {
			Logger log = Log4JUtil.getLogger(context);
			return Log4JUtil.convert2MessageLevel(log.getLevel());			
		}

		@Override
		public boolean isEnabled(String context, int level) {
	    	if ( context == null ) {
	            return false;
	        }
	    	Level logLevel = Log4JUtil.convert2Log4JLevel(level);
	        if ( logLevel == Level.OFF) {
	            return false;
	        }
	        Logger log = Log4JUtil.getLogger(context);
	        return log.isEnabledFor(logLevel);
		}
		
		@Override
		public void setLogLevel(String context, int level) {
			Logger log = Log4JUtil.getLogger(context);
			log.setLevel(Log4JUtil.convert2Log4JLevel(level));			
		}
	}
}
