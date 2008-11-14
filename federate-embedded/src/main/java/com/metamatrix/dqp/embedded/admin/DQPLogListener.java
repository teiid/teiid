/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import com.metamatrix.admin.api.embedded.EmbeddedLogger;
import com.metamatrix.core.log.LogListener;
import com.metamatrix.core.log.LogMessage;
import com.metamatrix.core.log.MessageLevel;


public class DQPLogListener implements LogListener {
    
    private EmbeddedLogger logger;
    
    public DQPLogListener(EmbeddedLogger logger) {
        this.logger = logger;
    }

    public void logMessage(LogMessage msg) {
        
        int logLevel = EmbeddedLogger.INFO;
        
        switch(msg.getLevel()) {
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
        logger.log(logLevel, msg.getTimestamp(), msg.getContext(), msg.getThreadName(), msg.getText(), msg.getException());
    }

    public void shutdown() {
    }
}
