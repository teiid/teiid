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

package com.metamatrix.platform.admin.apiimpl.runtime;


import java.util.Map;
import java.util.Collection;

import com.metamatrix.common.pooling.api.ResourcePoolStatistics;
import com.metamatrix.common.pooling.impl.BasicResourcePoolStatistics;
import com.metamatrix.common.config.api.ResourceDescriptorID;
import com.metamatrix.platform.admin.api.runtime.ResourcePoolStats;

public class ResourcePoolStatsImpl implements ResourcePoolStats {

    private BasicResourcePoolStatistics poolStats;
    private String hostName;
    private String processName;
    private Collection resourcesStats;
    private ResourceDescriptorID id;


    public ResourcePoolStatsImpl(ResourcePoolStatistics statistics, ResourceDescriptorID id, String host, String process, Collection resStats) {
        this.poolStats = (BasicResourcePoolStatistics) statistics;
        this.id = id;
        this.hostName = host;
        this.processName = process;
        this.resourcesStats = resStats;

    }

    /**
     * The Pool name.
     * @return The name of the Pool.
     */
    public String getPoolName() {
        return this.poolStats.getResourceDescriptorID().getName();
        // dont return the parent name 
    //    return this.poolStats.getResourceDescriptorID().getFullName();
    }

    /**
     * The Host name for this Pool.
     * @return The Host name on which this Pool is running.
     */
    public     String getHostName() {
        return this.hostName;
    }

    /**
     * The Process name for this Pool.
     * @return The Process name in which this Pool is running.
     */
    public String getProcessName() {
        return this.processName;
    }

    /**
     * The Resource Pool's type.
     * @return The type name for this Resource Pool.
     */
    public String getPoolType() {
        return this.poolStats.getComponentTypeID().getName();
    }

    /**
     * The Map of (String)StatisticName->(Object){@link com.metamatrix.common.pooling.api.PoolStatistic}
     * available for this Pool.
     * @return The Map of all statistics available for this Pool.
     */
    public Map getPoolStatistics(){
        try {
            return poolStats.getStatistics();
        } catch (Exception e) {
        }
        return null;
    }

    /**
     * Collection of ResourceStatistics objects for all resources in the pool.
     * @return Collection of all resource statistics available for this Pool.
     */
    public Collection getResourcesStatistics() {
        return this.resourcesStats;
    }

    /**
     * Return the ResourceDescriptorID that identifies the pool
     * @return ResourceDescriptorID
     */
    public ResourceDescriptorID getID() {
        return id;
    }
}
