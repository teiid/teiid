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

package com.metamatrix.connector.yahoo;

import com.metamatrix.data.api.*;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;

/**
 * Serves as a connection for the Yahoo connector.  Since there is no actual
 * connection, this "connection" doesn't really have any state.  
 */
public class YahooConnection implements Connection {

    private static final ConnectorCapabilities CAPABILITIES = new YahooCapabilities();

    private ConnectorEnvironment env;

    /**
     * 
     */
    public YahooConnection(ConnectorEnvironment env) {
        this.env = env;
    }

    /* 
     * @see com.metamatrix.data.Connection#getCapabilities()
     */
    public ConnectorCapabilities getCapabilities() {
        return CAPABILITIES;
    }

    /* 
     * @see com.metamatrix.data.Connection#createSynchExecution(com.metamatrix.data.language.ICommand, com.metamatrix.data.metadata.runtime.RuntimeMetadata)
     */
    public Execution createExecution(int executionMode, ExecutionContext executionContext, RuntimeMetadata metadata) {
        return new YahooExecution(env, metadata);
    }

    /* 
     * @see com.metamatrix.data.Connection#getMetadata()
     */
    public ConnectorMetadata getMetadata() {
        // Don't support
        return null;
    }

    /* 
     * @see com.metamatrix.data.Connection#close()
     */
    public void release() {
        // nothing to do
    }

}
