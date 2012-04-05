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
package org.teiid.adminapi.impl;

import java.util.Collection;
import java.util.List;

import org.teiid.adminapi.AdminException;
import org.teiid.client.plan.PlanNode;


public interface DQPManagement {
    List<RequestMetadata> getRequestsForSession(String sessionId) ;
    List<RequestMetadata> getRequests();
    WorkerPoolStatisticsMetadata getWorkerPoolStatistics();
    void terminateSession(String terminateeId);
    boolean cancelRequest(String sessionId, long requestId) throws AdminException;
    Collection<String> getCacheTypes();
    void clearCache(String cacheType);
    void clearCache(String cacheType, String vdbName, int version);
    Collection<SessionMetadata> getActiveSessions() throws AdminException;
    int getActiveSessionsCount() throws AdminException;
    Collection<org.teiid.adminapi.Transaction> getTransactions();
    void terminateTransaction(String xid) throws AdminException ;
    void mergeVDBs(String sourceVDBName, int sourceVDBVersion, String targetVDBName, int targetVDBVersion) throws AdminException;
    List<RequestMetadata> getLongRunningRequests();
    List<RequestMetadata> getRequestsUsingVDB(String vdbName, int vdbVersion) throws AdminException;
    CacheStatisticsMetadata getCacheStatistics(String cacheType);
    List<List> executeQuery(String vdbName, int version, String command, long timoutInMilli) throws AdminException;
    /**
     * 
     * @param sessionId
     * @param requestId
     * @return the plan or null if the request does not exist
     * @throws AdminException
     */
    PlanNode getPlan(String sessionId, long requestId) throws AdminException;
}
