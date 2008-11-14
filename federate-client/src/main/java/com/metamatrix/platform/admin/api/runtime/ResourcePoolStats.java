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

package com.metamatrix.platform.admin.api.runtime;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import com.metamatrix.common.config.api.ResourceDescriptorID;

/**
 * Statistics an information about a particular Resource Pool that can be
 * displayed in a Table.
 */
public interface ResourcePoolStats extends Serializable {

    /**
     * The Pool name.
     * @return The name of the Pool.
     */
    String getPoolName();

    /**
     * The Host name for this Pool.
     * @return The Host name on which this Pool is running.
     */
    String getHostName();

    /**
     * The Process name for this Pool.
     * @return The Process name in which this Pool is running.
     */
    String getProcessName();

    /**
     * The Resource Pool's type.
     * @return The type name for this Resource Pool.
     */
    String getPoolType();

    /**
     * The Map of (String)StatisticName->(Object)StatisticValue available
     * for this Pool.
     * @return The Map of all statistics available for this Pool.
     */
    Map getPoolStatistics();

    /**
     * Collection of ResourceStatistics objects for all resources in the pool.
     * @return Collection of all resource statistics available for this Pool.
     */
    Collection getResourcesStatistics();

    /**
     * Return the ResourceDescriptorID that identifies the pool
     * @return ResourceDescriptorID
     */
    ResourceDescriptorID getID();
}
