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

package com.metamatrix.dqp.internal.datamgr.impl;

import java.util.Properties;

import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.application.basic.BasicEnvironment;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.monitor.ConnectionStatus;
import com.metamatrix.dqp.internal.datamgr.ConnectorID;


public class FakeConnectorManagerImpl extends ConnectorManager {
    boolean poolIsOpen = false;
    private ConnectorEnvironment connectorEnvironment;
    
    public FakeConnectorManagerImpl() {
        this.connectorEnvironment = new ConnectorEnvironmentImpl(null, null, null);
    }
    
    public String getName() {
        return "fake"; //$NON-NLS-1$
    }

    public ConnectorID getConnectorID() {
        return null;
    }

    public void clearPool(boolean force) {        
    }
    
    public ConnectionStatus getStatus() {      
        return null;
    }
    
    public void start()  {
        poolIsOpen = true;
    }
    
    public void initialize(Properties p) {        
    }
    
    public void installService(String s, ApplicationService a) {        
    }
    
    public void stop() {        
    }
    
    public void restart() {
        
    }
    
    public ConnectorEnvironment getConnectorEnvironment() {
        return connectorEnvironment;
    }
 
    public boolean started() {
        return this.poolIsOpen;
    }
    
    @Override
    public ApplicationEnvironment getEnvironment() {
    	return new BasicEnvironment();
    }
    
}
