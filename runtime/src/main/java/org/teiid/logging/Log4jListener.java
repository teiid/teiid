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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


/**
 * Log4J Listener
 */
public class Log4jListener implements org.teiid.logging.Logger {
	
	@Override
	public boolean isEnabled(String context, int level) {
    	if ( context == null ) {
            return false;
        }
    	Level logLevel = convert2Log4JLevel(level);
        if ( logLevel == Level.OFF) {
            return false;
        }
        Logger log = getLogger(context);
        return log.isEnabledFor(logLevel);
	}

	@Override
	public void log(int level, String context, Object msg) {
		Logger log4j = getLogger(context);
		log4j.log(convert2Log4JLevel(level), msg);
	}

	public void log(int level, String context, Throwable t, Object msg) {
		Logger log4j = getLogger(context);
		log4j.log(convert2Log4JLevel(level), msg, t);
	}
	
	/**
	 * Convert {@link MessageLevel} to {@link Level}
	 * @param level
	 * @return
	 */
    public static Level convert2Log4JLevel(int level) {
    	switch (level) {
    	case MessageLevel.CRITICAL:
    		return Level.FATAL;
    	case MessageLevel.ERROR:
    		return Level.ERROR;
    	case MessageLevel.WARNING:
    		return Level.WARN;
    	case MessageLevel.INFO:
    		return Level.INFO;
    	case MessageLevel.DETAIL:
    	case MessageLevel.TRACE:
    		return Level.DEBUG;
    	case MessageLevel.NONE:
    		return Level.OFF;
    	}
    	return Level.DEBUG;
    }		
    
	/**
	 * Convert  {@link Level} to {@link MessageLevel}
	 * @param level
	 * @return
	 */
    public static int convert2MessageLevel(Level level) {
    	switch (level.toInt()) {
    	case Level.FATAL_INT:
    		return MessageLevel.CRITICAL;
    	case Level.ERROR_INT:
    		return MessageLevel.ERROR;
    	case Level.WARN_INT:
    		return MessageLevel.WARNING;
    	case Level.INFO_INT:
    		return MessageLevel.INFO;
    	case Level.DEBUG_INT:
    		return MessageLevel.DETAIL; 
    	case Level.OFF_INT:
    		return MessageLevel.NONE;
    	}
    	return MessageLevel.DETAIL;
    }	    
    
    /**
     * Get the logger for the given context.
     * @param context
     * @return
     */
	public static Logger getLogger(String context) {
		return Logger.getLogger(context);
	}  
					
	@Override
	public void shutdown() {
	}

}