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

import java.util.ArrayList;
import java.util.List;

import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.EngineStatisticsBean;
import org.teiid.adminapi.RequestBean;
import org.teiid.adminapi.SessionBean;
import org.teiid.adminapi.WorkerPoolStatisticsBean;
import org.teiid.client.plan.PlanNode;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.service.SessionService;
import org.teiid.dqp.service.SessionServiceException;
import org.teiid.runtime.EmbeddedAdminFactory;
import org.teiid.services.BufferServiceImpl;

class Teiid implements TeiidBean {

    private final DQPCore dqp;
    private final SessionService sessionService;
    private final BufferServiceImpl bufferService;

    public Teiid(DQPCore dqp, SessionService sessionService,
            BufferServiceImpl bufferService) {
        this.dqp = dqp;
        this.sessionService = sessionService;
        this.bufferService = bufferService;
    }

    @Override
    public String getQueryPlan(String sessionId, long executionId) {
        PlanNode plan = this.dqp.getPlan(sessionId, executionId);
        if (plan == null) {
            return null;
        }
        return plan.toXml();
    }

    @Override
    public List<RequestBean> getRequests()
            throws AdminException {
        return new ArrayList<>(dqp.getRequests());
    }

    @Override
    public List<SessionBean> getSessions()
            throws AdminException {
        return new ArrayList<>(sessionService.getActiveSessions());
    }

    @Override
    public void terminateSession(String sessionId) throws AdminException {
        this.dqp.terminateSession(sessionId);
    }

    @Override
    public void cancelRequest(String sessionId, long executionId)
            throws AdminException {
        try {
            this.dqp.cancelRequest(sessionId, executionId);
        } catch (TeiidComponentException e) {
            throw new AdminProcessingException(e);
        }
    }

    @Override
    public void terminateTransaction(String transactionId)
            throws AdminException {
        this.dqp.terminateTransaction(transactionId);
    }

    @Override
    public WorkerPoolStatisticsBean getWorkerPoolStatisticsBean() {
        return this.dqp.getWorkerPoolStatistics();
    }

    @Override
    public EngineStatisticsBean getEngineStatisticsBean() {
        try {
            return EmbeddedAdminFactory.createEngineStats(sessionService.getActiveSessionsCount(), bufferService, dqp);
        } catch (SessionServiceException e) {
            throw new TeiidRuntimeException(e);
        }
    }

    @Override
    public double getPercentBufferDiskSpaceInUse() {
        long maxDiskBufferSpaceMb = this.bufferService.getMaxDiskBufferSpaceMb();
        if (maxDiskBufferSpaceMb == 0) {
            return 0;
        }
        return this.bufferService.getUsedDiskBufferSpaceMb()/maxDiskBufferSpaceMb;
    }

    @Override
    public int getTotalOutOfDiskErrors() {
        return this.bufferService.getTotalOutOfDiskErrors();
    }

    @Override
    public int getActiveEngineThreadCount() {
        return this.dqp.getProcessWorkerPool().getActiveCount();
    }

    @Override
    public int getQueuedEngineWorkItems() {
        return this.dqp.getProcessWorkerPool().getQueued();
    }

    @Override
    public int getLongRunningRequestCount() {
        return this.dqp.getLongRunningRequestCount();
    }

    @Override
    public int getWaitingRequestsCount() {
        return this.dqp.getWaitingPlanCount();
    }

    @Override
    public long getTotalRequestsProcessed() {
        return this.dqp.getTotalPlansProcessed();
    }

}