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

package com.metamatrix.admin.server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.teiid.adminapi.Cache;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ClassLoaderManager;
import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.config.api.DeployedComponentID;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.common.stats.ConnectionPoolStats;
import com.metamatrix.platform.service.api.CacheAdmin;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.ServiceInterface;
import com.metamatrix.platform.service.controller.ServiceData;


public class FakeCacheAdmin implements CacheAdmin, ServiceInterface {

    protected static Set clearedCaches = new HashSet();
    int state;
    ServiceID id;
    
    public FakeCacheAdmin(ServiceID id){
    	this.id = id;
    }
        
    protected static void clearState() {
        clearedCaches.clear();
    }
    
    
    
    public Map getCaches() throws MetaMatrixComponentException {
        
        Map map = new HashMap();
        map.put("cache1", Cache.PREPARED_PLAN_CACHE); //$NON-NLS-1$
        map.put("cache2", Cache.CONNECTOR_RESULT_SET_CACHE); //$NON-NLS-1$
        map.put("cache3", Cache.CODE_TABLE_CACHE); //$NON-NLS-1$
        map.put("cache4", Cache.QUERY_SERVICE_RESULT_SET_CACHE); //$NON-NLS-1$
        
        return map;
    }

    public void clearCache(String name, Properties props) throws MetaMatrixComponentException {
        clearedCaches.add(name);
    }

    public void checkState() {
    }

    public void die() {
    }

    public void dieNow(){
    }

    public int getCurrentState() {
        return this.state;
    }

    public String getHostname(){
        return null;
    }

    public ServiceID getID(){
        return null;
    }

    public Properties getProperties(){
        return null;
    }

    public Collection getQueueStatistics(){
        List results = new ArrayList();
        WorkerPoolStats stats = new WorkerPoolStats();
        stats.name = "pool"; //$NON-NLS-1$
        stats.queued = (int) id.getID();
        stats.totalSubmitted = (int) id.getID();
        
        results.add(stats);
        return results;
    }
    
    public WorkerPoolStats getQueueStatistics(String name) {
        return null;
    }

    public String getServiceType()  {
        return null;
    }

    public Date getStartTime() {
        return null;
    }

    public Date getStateChangeTime(){
        return null;
    }

    public String getProcessName(){
        return null;
    }

    public void init(ServiceID id,
                     DeployedComponentID deployedComponentID,
                     Properties props,
                     ClientServiceRegistry listenerRegistry, ClassLoaderManager clManager) {
    }

    public boolean isAlive() {
        return false;
    }



	public void setInitException(Throwable t) {
		
	}

	public void updateState(int state) {
		this.state = state;
	}

	@Override
	public Throwable getInitException() {
		return null;
	}
	
	@Override
	public ServiceData getServiceData() {
		return null;
	}

	@Override
	public Collection<ConnectionPoolStats> getConnectionPoolStats() {
		return null;
	}
	
	

}
