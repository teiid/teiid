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

//#############################################################################
package com.metamatrix.console.ui.views.runtime.model;

public interface StatisticsConstants {

    static final int PROC_HOST_INDEX = 0;
    static final int TOTAL_PROCS_INDEX = 1;
    static final int SYNCHED_PROCS_INDEX = 2;
    static final int NOT_REGISTERED_PROCS_INDEX = 3;
    static final int NOT_DEPLOYED_PROCS_INDEX = 4;
    static final int NUM_PROCESS_STATS = NOT_DEPLOYED_PROCS_INDEX + 1;

    static final int SERV_HOST_INDEX = 0;
    static final int TOTAL_SERVS_INDEX = 1;
    static final int RUNNING_INDEX = 2;
    static final int SYNCHED_SERVS_INDEX = 3;
    static final int NOT_REGISTERED_SERVS_INDEX = 4;
    static final int NOT_DEPLOYED_SERVS_INDEX = 5;
    static final int FAILED_INDEX = 6;
    static final int STOPPED_INDEX = 7;
    static final int INIT_FAILED_INDEX = 8;
    static final int NOT_INIT_INDEX = 9;
    static final int DATA_SOURCE_UNAVAILABLE_INDEX = 10;
    static final int NUM_SERV_STATS = DATA_SOURCE_UNAVAILABLE_INDEX + 1;
}

