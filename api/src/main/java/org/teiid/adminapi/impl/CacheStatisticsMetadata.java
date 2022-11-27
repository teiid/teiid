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

import org.teiid.adminapi.CacheStatistics;

public class CacheStatisticsMetadata extends AdminObjectImpl implements CacheStatistics{

    private static final long serialVersionUID = -3514505497661004560L;

    private double hitRatio;
    private int totalEntries;
    private int requestCount;

    @Override
    public int getRequestCount() {
        return requestCount;
    }

    public void setRequestCount(int count) {
        this.requestCount = count;
    }

    @Override
    public double getHitRatio() {
        return this.hitRatio;
    }

    @Override
    public int getTotalEntries() {
        return this.totalEntries;
    }

    public void setHitRatio(double value) {
        this.hitRatio = value;
    }

    public void setTotalEntries(int value) {
        this.totalEntries = value;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("hitRatio=").append(hitRatio);//$NON-NLS-1$
        sb.append("; totalEntries=").append(totalEntries); //$NON-NLS-1$
        sb.append("; requestCount=").append(requestCount); //$NON-NLS-1$
        return sb.toString();
    }
}
