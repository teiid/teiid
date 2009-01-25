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

//#############################################################################
package com.metamatrix.console.ui.views.runtime.util;

import com.metamatrix.platform.service.api.ServiceState;

public interface ServiceStateConstants {

    // state indexes
    public static final int OPEN = ServiceState.STATE_OPEN;
    public static final int CLOSED = ServiceState.STATE_CLOSED;
    public static final int FAILED = ServiceState.STATE_FAILED;
    public static final int INIT_FAILED = ServiceState.STATE_INIT_FAILED;
    public static final int NOT_INITIALIZED = ServiceState.STATE_NOT_INITIALIZED;
    public static final int NOT_REGISTERED = ServiceState.STATE_NOT_REGISTERED;
    public static final int DATA_SOURCE_UNAVAILABLE = ServiceState.STATE_DATA_SOURCE_UNAVAILABLE;
    
    public static final int START = 0;
    public static final int STOP = 1;
    public static final int STOP_NOW = 2;
    public static final int SHOWQUEUE = 3;
    public static final int SHOWQUEUES = 4;
    public static final int SHOWPROCESS = 5;
    public static final int SHOW_SERVICE_ERROR = 6;

    public static final int TOTAL_OPERATIONS = 7;

    // operations indices
    // (Button position in the list)
    public static final int START_ORDINAL_POSITION = 0;
    public static final int STOP_ORDINAL_POSITION = 1;
    public static final int STOP_NOW_ORDINAL_POSITION = 2;
    public static final int SHOW_SERVICE_ERROR_ORDINAL_POSITION = 3;
    public static final int SHOWQUEUE_ORDINAL_POSITION = 3;
    public static final int SHOWQUEUES_ORDINAL_POSITION = 3;
    public static final int SHOWPROCESS_ORDINAL_POSITION = 3;


    public static final int TOTAL_DISPLAYED_OPERATIONS = 4;

}

