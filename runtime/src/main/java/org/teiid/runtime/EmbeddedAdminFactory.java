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
		stats.setTotalMemoryUsedInKB(bufferService.getHeapCacheMemoryInUseKB());
		stats.setMemoryUsedByActivePlansInKB(bufferService.getHeapMemoryInUseByActivePlansKB());
		stats.setDiskWriteCount(bufferService.getDiskWriteCount());
		stats.setDiskReadCount(bufferService.getDiskReadCount());
		stats.setCacheReadCount(bufferService.getCacheReadCount());
		stats.setCacheWriteCount(bufferService.getCacheWriteCount());
		stats.setDiskSpaceUsedInMB(bufferService.getUsedDiskBufferSpaceMB());
		stats.setActivePlanCount(dqp.getActivePlanCount());
		stats.setWaitPlanCount(dqp.getWaitingPlanCount());
		stats.setMaxWaitPlanWaterMark(dqp.getMaxWaitingPlanWatermark());
		return stats;
	}

	public Admin createAdmin(EmbeddedServer embeddedServer) {
		return new EmbeddedAdminImpl(embeddedServer);
	}

}
