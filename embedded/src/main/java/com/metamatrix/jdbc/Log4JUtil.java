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

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.metamatrix.core.log.MessageLevel;

class Log4JUtil {
	private static final String ROOT_CONTEXT = "org.teiid."; //$NON-NLS-1$
	
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
		Logger log4j = null;
		if (context.indexOf('.') == -1) {
			log4j = Logger.getLogger(ROOT_CONTEXT+context);
		}
		else {
			log4j = Logger.getLogger(context);
		}
		return log4j;
	}    
	
	public static Set<String> getContexts(){
		HashSet<String> contexts = new HashSet<String>();
		contexts.add(ROOT_CONTEXT+com.metamatrix.dqp.util.LogConstants.CTX_DQP);
		contexts.add(ROOT_CONTEXT+com.metamatrix.dqp.util.LogConstants.CTX_CONNECTOR);
		contexts.add(ROOT_CONTEXT+com.metamatrix.dqp.util.LogConstants.CTX_BUFFER_MGR);
		contexts.add(ROOT_CONTEXT+com.metamatrix.dqp.util.LogConstants.CTX_STORAGE_MGR);
		contexts.add(ROOT_CONTEXT+com.metamatrix.dqp.util.LogConstants.CTX_TXN_LOG);
		contexts.add(ROOT_CONTEXT+com.metamatrix.dqp.util.LogConstants.CTX_EXTENSION_SOURCE);
		contexts.add(ROOT_CONTEXT+com.metamatrix.dqp.util.LogConstants.CTX_COMMANDLOGGING);
		contexts.add(ROOT_CONTEXT+com.metamatrix.dqp.util.LogConstants.CTX_AUDITLOGGING);
		contexts.add(ROOT_CONTEXT+com.metamatrix.dqp.util.LogConstants.CTX_QUERY_SERVICE);

		contexts.add(ROOT_CONTEXT+com.metamatrix.common.util.LogConstants.CTX_CONFIG);
		contexts.add(ROOT_CONTEXT+com.metamatrix.common.util.LogConstants.CTX_COMMUNICATION);
		contexts.add(ROOT_CONTEXT+com.metamatrix.common.util.LogConstants.CTX_POOLING);
		contexts.add(ROOT_CONTEXT+com.metamatrix.common.util.LogConstants.CTX_SESSION);
		contexts.add(ROOT_CONTEXT+com.metamatrix.common.util.LogConstants.CTX_AUTHORIZATION);
		contexts.add(ROOT_CONTEXT+com.metamatrix.common.util.LogConstants.CTX_MEMBERSHIP);
		contexts.add(ROOT_CONTEXT+com.metamatrix.common.util.LogConstants.CTX_AUTHORIZATION_ADMIN_API);
		contexts.add(ROOT_CONTEXT+com.metamatrix.common.util.LogConstants.CTX_SERVER);
		contexts.add(ROOT_CONTEXT+com.metamatrix.common.util.LogConstants.CTX_ADMIN);
		
		contexts.add("com.arjuna"); //$NON-NLS-1$
		contexts.add("org.jboss"); //$NON-NLS-1$
		contexts.add("org.teiid"); //$NON-NLS-1$
		return contexts;
	}
}
