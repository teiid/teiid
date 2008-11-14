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

package com.metamatrix.common.queue;

import java.io.Serializable;

/**
 * This class is a holder for all the statistics gathered about a worker pool.
 */
public class WorkerPoolStats implements Serializable {

    public String name;

    // Current state    
    public int queued;
    public int threads;
    public int activeThreads;
    public int highestActiveThreads;
    public long totalSubmitted;
    public long totalCompleted;
        
    public String toString() {
        StringBuffer str = new StringBuffer();
        
        str.append("WorkerPoolStats:\n"); //$NON-NLS-1$
        str.append("\tqueued = " + queued); //$NON-NLS-1$
        str.append("\tthreads = " + threads);     //$NON-NLS-1$
        str.append("\tactiveThreads = " + activeThreads);     //$NON-NLS-1$
        str.append("\thighestActiveThreads = " + highestActiveThreads);     //$NON-NLS-1$
        str.append("\ttotalSubmitted = " + totalSubmitted);     //$NON-NLS-1$
        
        return str.toString();
    }

}

