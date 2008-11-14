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

package com.metamatrix.common.pooling.impl.statistics;

import com.metamatrix.common.pooling.impl.BasicPoolStatistic;

  /**
  * AvgStat keeps all the values and averages then.
  * The AvgStat is an average over time.
  */
public class AvgStat extends BasicPoolStatistic {

      private long cnt = 0;
      private long sumValue = 0;


    public AvgStat(String statName, String displayName, String description, int aggregateType) {
        super(statName, displayName, description, aggregateType);
        
    }


    public void increment(long value) {
           sumValue += value;
           ++cnt; 
            
    }        


    public synchronized Object getValue() {
        if (cnt > 0) {
          double avg = sumValue / cnt;
          return new Double(avg);
        }
        return new Double(0);

    }

    public synchronized void reset() {
        cnt = 0;
        sumValue = 0;
    }


} 
