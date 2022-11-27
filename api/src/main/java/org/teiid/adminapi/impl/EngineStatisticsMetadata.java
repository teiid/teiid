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

import org.teiid.adminapi.EngineStatistics;

public class EngineStatisticsMetadata extends AdminObjectImpl implements EngineStatistics {
    private static final long serialVersionUID = -6207621797253332528L;

    private int sessionCount;
    private long totalMemoryUsedInKB;
    private long memoryUsedByActivePlansInKB;
    private long diskWriteCount;
    private long diskReadCount;
    private long cacheWriteCount;
    private long cacheReadCount;
    private long diskSpaceUsedInMB;
    private int activePlanCount;
    private int waitPlanCount;
    private int maxWaitPlanCount;

    @Override
    public int getSessionCount() {
        return sessionCount;
    }

    @Override
    public long getTotalMemoryUsedInKB() {
        return totalMemoryUsedInKB;
    }

    @Override
    public long getMemoryUsedByActivePlansInKB() {
        return memoryUsedByActivePlansInKB;
    }

    @Override
    public long getDiskWriteCount() {
        return diskWriteCount;
    }

    @Override
    public long getDiskReadCount() {
        return diskReadCount;
    }

    @Override
    public long getCacheReadCount() {
        return cacheReadCount;
    }

    @Override
    public long getCacheWriteCount() {
        return cacheWriteCount;
    }

    @Override
    public long getDiskSpaceUsedInMB() {
        return diskSpaceUsedInMB;
    }

    @Override
    public int getActivePlanCount() {
        return activePlanCount;
    }

    @Override
    public int getWaitPlanCount() {
        return waitPlanCount;
    }

    @Override
    public int getMaxWaitPlanWaterMark() {
        return maxWaitPlanCount;
    }

    public void setSessionCount(int sessionCount) {
        this.sessionCount = sessionCount;
    }

    public void setTotalMemoryUsedInKB(long totalMemoryUsedInKB) {
        this.totalMemoryUsedInKB = totalMemoryUsedInKB;
    }

    public void setMemoryUsedByActivePlansInKB(long memoryUsedByActivePlansInKB) {
        this.memoryUsedByActivePlansInKB = memoryUsedByActivePlansInKB;
    }

    public void setDiskWriteCount(long diskWriteCount) {
        this.diskWriteCount = diskWriteCount;
    }

    public void setDiskReadCount(long diskReadCount) {
        this.diskReadCount = diskReadCount;
    }

    public void setCacheWriteCount(long cacheWriteCount) {
        this.cacheWriteCount = cacheWriteCount;
    }

    public void setCacheReadCount(long cacheReadCount) {
        this.cacheReadCount = cacheReadCount;
    }

    public void setDiskSpaceUsedInMB(long diskSpaceUsedInMB) {
        this.diskSpaceUsedInMB = diskSpaceUsedInMB;
    }

    public void setActivePlanCount(int activePlanCount) {
        this.activePlanCount = activePlanCount;
    }

    public void setWaitPlanCount(int waitPlanCount) {
        this.waitPlanCount = waitPlanCount;
    }

    public void setMaxWaitPlanWaterMark(int maxWaitPlanCount) {
        this.maxWaitPlanCount = maxWaitPlanCount;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("EngineStatistics:"); //$NON-NLS-1$
        sb.append("sessionCount ").append(sessionCount);//$NON-NLS-1$
        sb.append("; totalMemoryUsedInKB ").append(totalMemoryUsedInKB);//$NON-NLS-1$
        sb.append("; memoryUsedByActivePlansInKB ").append(memoryUsedByActivePlansInKB);//$NON-NLS-1$
        sb.append("; diskWriteCount ").append(diskWriteCount); //$NON-NLS-1$
        sb.append("; diskReadCount ").append(diskReadCount); //$NON-NLS-1$
        sb.append("; cacheWriteCount ").append(cacheWriteCount); //$NON-NLS-1$
        sb.append("; cacheReadCount ").append(cacheReadCount); //$NON-NLS-1$
        sb.append("; diskSpaceUsedInMB ").append(diskSpaceUsedInMB); //$NON-NLS-1$
        sb.append("; activePlanCount ").append(activePlanCount); //$NON-NLS-1$
        sb.append("; waitPlanCount ").append(waitPlanCount); //$NON-NLS-1$
        sb.append("; maxWaitPlanCount ").append(maxWaitPlanCount); //$NON-NLS-1$
        return sb.toString();
    }

    @Override
    public long getBufferHeapInUseKb() {
        return totalMemoryUsedInKB;
    }

    @Override
    public long getBufferHeapReservedByActivePlansKb() {
        return memoryUsedByActivePlansInKB;
    }

    @Override
    public long getStorageReadCount() {
        return cacheReadCount;
    }

    @Override
    public long getStorageWriteCount() {
        return cacheWriteCount;
    }
}
