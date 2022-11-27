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
package org.teiid.runtime;

import org.teiid.adminapi.Admin;
import org.teiid.adminapi.impl.EngineStatisticsMetadata;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.services.BufferServiceImpl;

public class EmbeddedAdminFactory {

    private static EmbeddedAdminFactory INSTANCE = new EmbeddedAdminFactory();

    public static EmbeddedAdminFactory getInstance() {
        return INSTANCE;
    }

    public static EngineStatisticsMetadata createEngineStats(
            int activeSessionsCount, BufferServiceImpl bufferService,
            DQPCore dqp) {
        EngineStatisticsMetadata stats = new EngineStatisticsMetadata();
        stats.setSessionCount(activeSessionsCount);
        stats.setTotalMemoryUsedInKB(bufferService.getHeapBufferInUseKb());
        stats.setMemoryUsedByActivePlansInKB(bufferService.getMemoryReservedByActivePlansKb());
        stats.setDiskWriteCount(bufferService.getDiskWriteCount());
        stats.setDiskReadCount(bufferService.getDiskReadCount());
        stats.setCacheReadCount(bufferService.getStorageReadCount());
        stats.setCacheWriteCount(bufferService.getStorageWriteCount());
        stats.setDiskSpaceUsedInMB(bufferService.getUsedDiskBufferSpaceMb());
        stats.setActivePlanCount(dqp.getActivePlanCount());
        stats.setWaitPlanCount(dqp.getWaitingPlanCount());
        stats.setMaxWaitPlanWaterMark(dqp.getMaxWaitingPlanWatermark());
        return stats;
    }

    public Admin createAdmin(EmbeddedServer embeddedServer) {
        return new EmbeddedAdminImpl(embeddedServer);
    }

}
