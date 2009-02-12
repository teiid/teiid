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

package com.metamatrix.connector.monitor;

import java.io.Serializable;
import java.util.Date;

/**
 * Used within the server report the status of a Connector.
 * Simple data holder for an indicator of whether the connector is alive and how many connections
 * the connector has to it's data source.
 */
public class ConnectionStatus implements Serializable {
    public static final int DEFAULT_CONNECTION_COUNT = 0;

    public AliveStatus aliveStatus;
    public int connectionCount = DEFAULT_CONNECTION_COUNT;
    public Exception exception;
    public Date failureDate;


    public ConnectionStatus(AliveStatus aliveStatus, int connectionCount, Exception exception, Date failureDate) {
        this.aliveStatus = aliveStatus;
        this.connectionCount = connectionCount;
        this.exception = exception;
        this.failureDate = failureDate;
    }


    public ConnectionStatus(AliveStatus aliveStatus, int connectionCount) {
        this.aliveStatus = aliveStatus;
        this.connectionCount = connectionCount;
    }

    public ConnectionStatus(AliveStatus aliveStatus) {
        this(aliveStatus, 0);
    }

    public int getConnectionCount() {
        return this.connectionCount;
    }

    public AliveStatus getStatus() {
        return this.aliveStatus;
    }
}
