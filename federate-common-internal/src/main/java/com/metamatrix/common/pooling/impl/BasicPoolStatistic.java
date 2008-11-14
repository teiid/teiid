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
import java.util.Date;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.pooling.api.PoolStatistic;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.util.ArgCheck;

public abstract class BasicPoolStatistic implements PoolStatistic , Serializable {
    
    public static final long serialVersionUID = -984913469025279555L;

    // sum the values
    public static final int TYPE_SUM = 1;
    // calculate average of the values
    public static final int TYPE_AVG = 2;
    // store the greatest value
    public static final int TYPE_HIGHEST = 3;
    // store the lowest value
    public static final int TYPE_LOWEST = 4;


    private String name;
    private String displayName;
    private String description;
    private int aggregateType;

    // tracks when this statistic began tracking the value
    private long startDateTime;

    protected BasicPoolStatistic(String statName, String displayName, String description, int aggregateType) {
        if(statName == null){
            ArgCheck.isNotNull(statName, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0010));
        }
        if(displayName == null){
            ArgCheck.isNotNull(displayName, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0011));
        }
        if(description == null){
            ArgCheck.isNotNull(description, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0012));
        }

        this.name = statName;
        this.displayName = displayName;
        this.description = description;
        this.aggregateType = aggregateType;
        performReset();
    }

    public String getDisplayName() {
        return this.displayName;
    }

   /**
    * Returns the description of the statistic
    * @return String description of statistic
    */
    public String getDescription() {
        return this.description;
    }

    /**
    * Returns the name of the statistic
    * @return String statistic name
    */
    public String getName(){
        return this.name;
    }



   /**
    * Returns the aggregate type code that indicates
    * how the statistic value can be used with
    * other statistics
    * @return int aggregate type code
    */
    public int getAggregationType() {
        return this.aggregateType;
    }


    public long getStartDateTime() {
        return this.startDateTime;
    }

    protected void performReset() {
        this.startDateTime = new Date().getTime();
        reset();
    }


    /**
    * Increment the statistic
    */
    public void increment() {}


    /**
    * Decrement the statistic
    */
    public void decrement() {}


    public void increment(long value) {}

    public void decrement(long value) {}


    /**
    * Called to reset the statistic
    */
    public abstract void reset();

}
