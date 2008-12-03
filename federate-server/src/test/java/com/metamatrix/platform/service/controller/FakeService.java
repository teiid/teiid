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

package com.metamatrix.platform.service.controller;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;

import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.config.api.DeployedComponentID;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.vm.controller.VMControllerID;

public class FakeService implements FakeServiceInterface {

    private int test1Count;
    
    public void die() {
    }
    public void dieNow(){
    }
    public int getCurrentState() {
        return 0;
    }
    public String getHostname() {
        return null;
    }
    public ServiceID getID() {
        return null;
    }
    public Properties getProperties() {
        return null;
    }
    public Collection getQueueStatistics() {
        return null;
    }
    public WorkerPoolStats getQueueStatistics(String name) {
        return null;
    }
    public void checkState() {
    }
    public String getServiceType() {
        return null;
    }
    public Date getStartTime() {
        return null;
    }
    public Date getStateChangeTime() {
        return null;
    }
    public VMControllerID getVMID() {
        return null;
    }
    public boolean isAlive() {
        return false;
    }
    public void resume() {
    }
    public void suspend() {
    }

    public void test1() {
        test1Count++;
    }

    public Collection test2() {
        return new HashSet();
    }
    
    public boolean test3() {
        return false;
    }
    
    public Collection test4()  {
        return Arrays.asList(new Object[] {new Integer(1)});
    }
    
    public int getTest1Count() {
        return this.test1Count;
    }
	public void init(ServiceID id, DeployedComponentID deployedComponentID,
			Properties props, ClientServiceRegistry listenerRegistry){
	}
	public void setInitException(Throwable t) {
	}
	public void updateState(int state) {
	}
}
