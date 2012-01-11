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

package org.teiid.jboss;

import org.jboss.logging.Logger;
import org.jboss.logging.Logger.Level;
import org.teiid.logging.MessageLevel;

public class JBossLogger implements org.teiid.logging.Logger {

	@Override
	public boolean isEnabled(String context, int level) {
    	if ( context == null ) {
            return false;
        }
    	Level logLevel = convert2JbossLevel(level);
        Logger log = getLogger(context);
        return log.isEnabled(logLevel);
	}

	@Override
	public void log(int level, String context, Object msg) {
		Logger logger = getLogger(context);
		logger.log(convert2JbossLevel(level), msg);
	}

	@Override
	public void log(int level, String context, Throwable t, Object msg) {
		Logger logger = getLogger(context);
		logger.log(convert2JbossLevel(level), msg, t);
	}
	
	/**
	 * Convert {@link MessageLevel} to {@link Level}
	 * @param level
	 * @return
	 */
    public static Level convert2JbossLevel(int level) {
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
    		return Level.DEBUG;
    	case MessageLevel.TRACE:
    		return Level.TRACE;
    	}
    	return Level.DEBUG;
    }		
    
	/**
	 * Convert  {@link Level} to {@link MessageLevel}
	 * @param level
	 * @return
	 */
    public static int convert2MessageLevel(Level level) {
    	switch (level) {
    	case FATAL:
    		return MessageLevel.CRITICAL;
    	case ERROR:
    		return MessageLevel.ERROR;
    	case WARN:
    		return MessageLevel.WARNING;
    	case INFO:
    		return MessageLevel.INFO;
    	case DEBUG:
    		return MessageLevel.DETAIL; 
    	case TRACE:
    		return MessageLevel.NONE;
    	}
    	return MessageLevel.DETAIL;
    }	    
    
    /**
     * Get the logger for the given context.
     * @param context
     * @return
     */
	private Logger getLogger(String context) {
		return Logger.getLogger(context);
	}  
					
	@Override
	public void shutdown() {
	}

}
