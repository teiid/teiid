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

package com.metamatrix.cdk.api;

import java.util.Date;

import com.metamatrix.data.api.ConnectorLogger;

/**
 * Base logger class that redirects all messages to one method and 
 * provides some default formatting methods and level control.
 */
public abstract class BaseLogger implements ConnectorLogger {

    public static final int OFF = 0;
    public static final int ERROR = 1;
    public static final int WARNING = 2;
    public static final int INFO = 3;
    public static final int DETAIL = 4;
    public static final int TRACE = 5;

    private int logLevel = INFO;
    
    public int getLevel() {
        return this.logLevel; 
    }
    
    public void setLevel(int level) {
        this.logLevel = level;
    }

    protected Date getCurrentTimestamp() {
        return new Date();
    }
    
    protected String getCurrentTimestampString() {
        return getCurrentTimestamp().toString();
    }
    
    protected abstract void log(int level, String message, Throwable error);

    public void logError(String message) {
        log(ERROR, message, null);
    }

    public void logError(String message, Throwable error) {
        log(ERROR, message, error);
    }

    public void logWarning(String message) {
        log(WARNING, message, null);
    }

    public void logInfo(String message) {
        log(INFO, message, null);
    }

    public void logDetail(String message) {
        log(DETAIL, message, null);
    }

    public void logTrace(String message) {
        log(TRACE, message, null);
    }

}
