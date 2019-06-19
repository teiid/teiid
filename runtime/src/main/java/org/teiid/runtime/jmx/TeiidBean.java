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

package org.teiid.runtime.jmx;

import java.util.List;

import javax.management.MXBean;

import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.EngineStatisticsBean;
import org.teiid.adminapi.RequestBean;
import org.teiid.adminapi.Session;
import org.teiid.adminapi.SessionBean;
import org.teiid.adminapi.WorkerPoolStatisticsBean;

@MXBean
public interface TeiidBean {

    /**
     * Get the Query Plan for the given session with provided execution id.
     * @param sessionId
     * @param executionId
     * @return
     */
    String getQueryPlan(String sessionId, long executionId);

    /**
     * Get the all Requests that are currently in process
     * @return Collection of {@link RequestBean}
     */
    List<RequestBean> getRequests() throws AdminException;

    /**
     * Get all the current Sessions.
     * @return Collection of {@link Session}
     */
    List<SessionBean> getSessions() throws AdminException;

    /**
     * Terminate the Session
     *
     * @param sessionId  Session Identifier {@link org.teiid.adminapi.Session}.
     * No wild cards currently supported, must be explicit
     * @throws AdminException
     */
    void terminateSession(String sessionId) throws AdminException;

    /**
     * Cancel Request
     *
     * @param sessionId session Identifier for the request.
     * @param executionId request Identifier
     *
     * @throws AdminException
     */
    void cancelRequest(String sessionId, long executionId) throws AdminException;

    /**
     * Mark the given global transaction as rollback only.
     * @param transactionId
     * @throws AdminException
     */
    void terminateTransaction(String transactionId) throws AdminException;

    /**
     * Get the number of requests processed.  This includes all queries
     * regardless of whether they completed successfully.
     * @return
     */
    long getTotalRequestsProcessed();

    /**
     * Get the current number of requests waiting on execution at the engine level.
     * These are plans restricted by their output buffer and max active plans.
     * @return
     */
    int getWaitingRequestsCount();

    /**
     * Get the current number of threads processing engine work, which is
     * typically plan, source, and transaction work.
     * @return
     */
    int getActiveEngineThreadCount();

    /**
     * Get the current number of queued engine work items.
     * @return
     */
    int getQueuedEngineWorkItems();

    /**
     * Get the number of currently long running requests.
     * @return
     */
    int getLongRunningRequestCount();

    /**
     * Get the current percentage of disk space in usage by the buffer manager
     * @return
     */
    double getPercentBufferDiskSpaceInUse();

    /**
     * Get the out of disk error count
     * @return
     */
    int getTotalOutOfDiskErrors();

    /**
     * Get the statistics for the engine thread pool.
     * @return
     */
    WorkerPoolStatisticsBean getWorkerPoolStatisticsBean();

    /**
     * Get the engine statistics related to memory and plans.
     * @return
     */
    EngineStatisticsBean getEngineStatisticsBean();

}
