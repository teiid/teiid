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

package com.metamatrix.dqp.embedded.admin;

import org.teiid.adminapi.EmbeddedLogger;

import com.metamatrix.core.log.LogListener;
import com.metamatrix.core.log.MessageLevel;


public class DQPLogListener implements LogListener {
    
    private EmbeddedLogger logger;
    
    public DQPLogListener(EmbeddedLogger logger) {
        this.logger = logger;
    }

    public void log(int level, String context, Object msg) {
        logger.log(convertLevel(level), System.currentTimeMillis(), context, Thread.currentThread().getName(), msg.toString(), null);        
    }

    public void log(int level, String context, Throwable t, Object msg) {
    	logger.log(convertLevel(level), System.currentTimeMillis(), context, Thread.currentThread().getName(), msg.toString(), t);
    }    
    
	private int convertLevel(int level) {
		int logLevel = EmbeddedLogger.INFO;
        
        switch(level) {
            case MessageLevel.WARNING:
                logLevel = EmbeddedLogger.WARNING;
                break;
            case MessageLevel.ERROR:
                logLevel = EmbeddedLogger.ERROR;
                break;
            case MessageLevel.DETAIL:
                logLevel = EmbeddedLogger.DETAIL;
                break;
            case MessageLevel.TRACE:
                logLevel = EmbeddedLogger.TRACE;
                break;
            case MessageLevel.NONE:
                logLevel = EmbeddedLogger.NONE;
                break;
                
            default:
                logLevel = EmbeddedLogger.INFO;
        }
		return logLevel;
	}
    
    public void shutdown() {
    }
}
