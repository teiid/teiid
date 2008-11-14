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

package com.metamatrix.common.pooling.api;

import java.util.Collection;
import java.util.Map;

import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.ResourceDescriptorID;

public interface ResourcePoolStatistics {


    /**
    * Add a {@link PoolStatistic} to be monitored 
    * @return <code>PoolStatistic</code> value of the monitor statistic value
    */
   void addStatistic(PoolStatistic statistic);

    /**
    * Returns the {@link PoolStatistic} of the monitored statistic object
    * @return <code>PoolStatistic</code> value of the monitor statistic value
    */
   PoolStatistic getStatistic(String statName);
    /**
    * Returns the descriptor id for the resource pool.  
    * @return ResourceDescriptorID pool ID
    */
   ResourceDescriptorID getResourceDescriptorID();
   
   /**
    * Returns the {@link com.metamatrix.common.config.ComponentTypeID} that
    * identifies the type of pool these statistics represent.
    * @return ComponentTypeID is the type id
    */
   ComponentTypeID getComponentTypeID();
   
    /**
    * Returns a collection of all the statistics being gather for this pool  
    * @return Collection of statistic names
    */
   Collection getStatisticNames(); 
   
   /**
    * Returns the map of the {@link PoolStatistic}s keyed by the statistic name
    * @return Map of PoolStatistics
    */
   Map getStatistics(); 

   /**
   * Called to increment the value by 1 for the  tracked statistic
   * @param statName is one of the defined statistics
   */
//   void increment(String statName);
   
   /**
   * Called to increment the value for the  tracked statistic
   * @param statName is one of the defined statistics
   * @param value is the value to increment the statistic by
   */
//   void increment(String statName, long value);   
   
   
   /**
   * Called to decrement the value by 1 for the  tracked statistic
   * @param statName is one of the defined statistics
   */   
//   void decrement(String statName);
   
   /**
   * Called to decrement the value for the  tracked statistic
   * @param statName is one of the defined statistics
   * @param value is the value to decrement the statistic by
   */
//   void decrement(String statName, long value);    

}

