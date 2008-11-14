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

package com.metamatrix.common.pooling.impl;


import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.ResourceDescriptorID;
import com.metamatrix.common.pooling.api.PoolStatistic;
import com.metamatrix.common.pooling.api.ResourcePool;
import com.metamatrix.common.pooling.api.ResourcePoolStatistics;

public class BasicResourcePoolStatistics implements ResourcePoolStatistics, Serializable {

	
    private ResourceDescriptorID id;
    private ComponentTypeID typeId;

    private Map stats;


    public BasicResourcePoolStatistics(ResourcePool pool) {
        this.id = pool.getResourceDescriptorID();
        this.typeId = pool.getComponentTypeID();
        stats = new HashMap();

    }

    public void addStatistic(PoolStatistic statistic) {
            stats.put(statistic.getName(), statistic);    
    }
    

   public ResourceDescriptorID getResourceDescriptorID() {
      return id;
   }    
    
    public ComponentTypeID getComponentTypeID() {
         return this.typeId;
    }

  
     public Collection getStatisticNames() {
      		return stats.keySet();
     }
     
     public Map getStatistics() {
        Map copy = new HashMap(this.stats.size());
        Iterator it = this.stats.keySet().iterator();
        while (it.hasNext()) {
            String name = (String) it.next();
            copy.put(name, this.getStatistic(name));
        }            
        return copy;  
     }



    /**
    * Returns the value of the monitored statistic object
    * This is a Double, or Long
    * @return <code>Number</code> value of the monitor statistic value
    */
   public synchronized PoolStatistic getStatistic(String statName) {
      if (stats.containsKey(statName)) {
          PoolStatistic stat = (PoolStatistic) stats.get(statName);
          return stat;
      }
      return null;
   }


    

   public synchronized void increment(String statName) {
      if (stats.containsKey(statName)) {
          BasicPoolStatistic stat = (BasicPoolStatistic) stats.get(statName);
          stat.increment();
      }       
   }
   
   public synchronized void increment(String statName, long value) {
       
      if (stats.containsKey(statName)) {
          BasicPoolStatistic stat = (BasicPoolStatistic) stats.get(statName);
          stat.increment(value);
      }       
   }   

   
   
   public synchronized void decrement(String statName) {
      if (stats.containsKey(statName)) {
          BasicPoolStatistic stat = (BasicPoolStatistic) stats.get(statName);
          stat.decrement();
      }       
   }  

   public synchronized void decrement(String statName, long value) {
       
      if (stats.containsKey(statName)) {
          BasicPoolStatistic stat = (BasicPoolStatistic) stats.get(statName);
          stat.decrement(value);
      }         
   }
   
   public String toString() {
       Map s = getStatistics();
       StringBuffer result = new StringBuffer("\nPool: "); //$NON-NLS-1$
       result.append(getResourceDescriptorID().getFullName());

        Iterator it = s.keySet().iterator();
        while (it.hasNext()) {
            String name = (String) it.next();
            PoolStatistic ps =  (PoolStatistic) s.get(name);
            result.append("\nStatistic: "); //$NON-NLS-1$
            result.append(ps.getName());
            result.append("  value: "); //$NON-NLS-1$
            result.append(ps.getValue().toString());
        }
        result.append("\n"); //$NON-NLS-1$
        return result.toString();       

      
   } 

}
