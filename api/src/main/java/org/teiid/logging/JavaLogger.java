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

package org.teiid.logging;

import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/** 
 * Write to Java logging
 */
public class JavaLogger implements org.teiid.logging.Logger {
	
	private ConcurrentHashMap<String, Logger> loggers = new ConcurrentHashMap<String, Logger>();
	
	@Override
	public boolean isEnabled(String context, int msgLevel) {
		Logger logger = getLogger(context);
    	
    	Level javaLevel = convertLevel(msgLevel);
    	return logger.isLoggable(javaLevel);
	}

	private Logger getLogger(String context) {
		Logger logger = loggers.get(context);
		if (logger == null) {
			logger = Logger.getLogger(context);
			loggers.put(context, logger);
		}
		return logger;
	}

    public void log(int level, String context, Object msg) {
    	Logger logger = getLogger(context);
    	
    	Level javaLevel = convertLevel(level);
		logger.log(javaLevel, msg.toString());
    }
    
    public void log(int level, String context, Throwable t, Object msg) {
    	Logger logger = getLogger(context);
    	
    	Level javaLevel = convertLevel(level);
		logger.log(javaLevel, msg != null ? msg.toString() : null, t);
    }
    
    public Level convertLevel(int level) {
    	switch (level) {
    	case MessageLevel.CRITICAL:
    	case MessageLevel.ERROR:
    		return Level.SEVERE;
    	case MessageLevel.WARNING:
    		return Level.WARNING;
    	case MessageLevel.INFO:
    		return Level.FINE;
    	case MessageLevel.DETAIL:
    		return Level.FINER;
    	case MessageLevel.TRACE:
    		return Level.FINEST;
    	}
    	return Level.ALL;
    }

    public void shutdown() {
    }

}
