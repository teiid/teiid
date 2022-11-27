/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.adminapi.impl;

import org.teiid.adminapi.WorkerPoolStatistics;

/**
 * This class is a holder for all the statistics gathered about a worker pool.
 */
public class WorkerPoolStatisticsMetadata extends AdminObjectImpl implements WorkerPoolStatistics {

    private static final long serialVersionUID = -4917902925523802295L;

    // Current state
    private int queued;
    private int highestQueued;
    private int maxThreads;
    private int activeThreads;
    private int highestActiveThreads;
    private long totalSubmitted;
    private long totalCompleted;

    @Override
    public int getActiveThreads() {
        return activeThreads;
    }

    @Override
    public int getHighestActiveThreads() {
        return highestActiveThreads;
    }

    @Override
    public long getTotalCompleted() {
        return totalCompleted;
    }

    @Override
    public long getTotalSubmitted() {
        return totalSubmitted;
    }

    @Override
    public String getQueueName() {
        return getName();
    }

    @Override
    public int getQueued() {
        return queued;
    }

    @Override
    public int getHighestQueued() {
        return highestQueued;
    }

    @Override
    public int getMaxThreads() {
        return maxThreads;
    }

    public void setQueued(int queued) {
        this.queued = queued;
    }

    public void setHighestQueued(int highestQueued) {
        this.highestQueued = highestQueued;
    }

    public void setMaxThreads(int maxThreads) {
        this.maxThreads = maxThreads;
    }

    public void setActiveThreads(int activeThreads) {
        this.activeThreads = activeThreads;
    }

    public void setHighestActiveThreads(int highestActiveThreads) {
        this.highestActiveThreads = highestActiveThreads;
    }

    public void setTotalSubmitted(long totalSubmitted) {
        this.totalSubmitted = totalSubmitted;
    }

    public void setTotalCompleted(long totalCompleted) {
        this.totalCompleted = totalCompleted;
    }

    public void setQueueName(String name) {
        setName(name);
    }

    public String toString() {
        StringBuilder str = new StringBuilder();

        str.append("WorkerPoolStats:"); //$NON-NLS-1$
        str.append("  queue-name = " + getName()); //$NON-NLS-1$
        str.append("; queued = " + queued); //$NON-NLS-1$
        str.append("; highestQueued = " + highestQueued); //$NON-NLS-1$
        str.append("; maxThreads = " + maxThreads);     //$NON-NLS-1$
        str.append("; activeThreads = " + activeThreads);     //$NON-NLS-1$
        str.append("; highestActiveThreads = " + highestActiveThreads);     //$NON-NLS-1$
        str.append("; totalSubmitted = " + totalSubmitted);     //$NON-NLS-1$
        str.append("; totalCompleted = " + totalCompleted);     //$NON-NLS-1$
        return str.toString();
    }

}

