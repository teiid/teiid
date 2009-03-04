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

package com.metamatrix.platform.vm.controller;

import java.io.Serializable;

import com.metamatrix.common.queue.WorkerPoolStats;

/**
 * VM statistics including: total memory, free memory, number of threads.
 */
public class ProcessStatistics implements Serializable {
    public String name;
    public long totalMemory = 0;
    public long freeMemory = 0;
    public int threadCount = 0;
    
    
    public SocketListenerStats socketListenerStats = new SocketListenerStats();
    
    public WorkerPoolStats processPoolStats = new WorkerPoolStats();
}


