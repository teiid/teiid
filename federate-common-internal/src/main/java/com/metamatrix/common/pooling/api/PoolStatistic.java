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

/**
 * Created on May 9, 2002
 * 
 * A PoolStatistic represents one statistic contained
 * in the {@link ResourcePoolStatistics ResourcePoolStatistics}.
 *
 */
public interface PoolStatistic {
  
    
    /**
    * The average aggregate type indicates the statistic value
    * can be averaged in the context with other statistics
    * of the same name.
    */
    public static final int AVG_AGGREGATE_TYPE = 1;
       
    /**
    * The sum aggregate type indicates the statistic value
    * can be summed in the context with other statistics
    * of the same name.
    */
    public static final int SUM_AGGREGATE_TYPE = 2;
    
    /**
    * The min aggregate type indicates the statistic value
    * can be used in the context with other statistics
    * of the same name to determine the minimim statistic value.
    */
    public static final int MIN_AGGREGATE_TYPE = 3;
       
    /**
    * The max aggregate type indicates the statistic value
    * can be used in the context with other statistics
    * of the same name to determine the maximum statistic value.
    */
    public static final int MAX_AGGREGATE_TYPE = 4;
    
    /**
    * The most resent aggregate type indicates the statistic value
    * can be used in the context with other statistics
    * of the same name to determine the most resently used.
    */
    public static final int MOST_RESENT_AGGREGATE_TYPE = 5;
       
    /**
    * The lset resent aggregate type indicates the statistic value
    * can be used in the context with other statistics
    * of the same name to determine the lest resently used.
    */
    public static final int LEAST_RESENT_AGGREGATE_TYPE = 6;
    
    /**
    * The common aggregate type indicates the statistic value
    * can be used in the context with other statistics
    * of the same name to determine which value is most commonly used.
    */
    public static final int COMMON_AGGREGATE_TYPE = 7;
       
    /**
    * The no aggregate type indicates the statistic value
    * cannot be used with other statistic values regardless
    * if the name is the same.
    */
    public static final int NO_AGGREGATE_TYPE = 8;    
        
        
   /**
    * Returns the name of the statistic, this
    * name should never change onec established
    * so that coding can be done against it
    * @return String statistic name
    */
     String getName();  
     
   /**
    * Returns the display name of the statistic
    * @return String statistic display name
    */
     String getDisplayName();     
     
   /**
    * Returns the description of the statistic
    * @return String description of statistic
    */
     String getDescription();          
    
   /**
    * Returns the statistic value
    * @return Number statistic value
    */
    Object getValue();
    
    
   /**
    * Returns a long value repsenting when this statisic
    * begain gathering information.
    * @return long date time value
    */
    long getStartDateTime();    
    

   /**
    * Returns the aggregate type code that indicates
    * how the statistic value can be used with
    * other statistics
    * @return int aggregate type code
    */        
    int getAggregationType();
    
    /**
     * Returns the <code>Class</code> for which
     * this statistic value represents.  An
     * example is:
     *      Value = Long (datetime)
     *      Class = Date
     * @return Class that represents the statistic value
     */
//   Class getStatClassType();  

}
