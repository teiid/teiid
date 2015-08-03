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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import javax.resource.spi.work.WorkException;
import javax.resource.spi.work.WorkManager;

import org.teiid.adminapi.impl.WorkerPoolStatisticsMetadata;
import org.teiid.dqp.internal.process.TeiidExecutor;
import org.teiid.dqp.internal.process.ThreadReuseExecutor;
import org.teiid.security.SecurityHelper;

/**
 * A {@link TeiidExecutor} that allows for a dummy {@link SecurityHelper} to be used
 * and prevents Teiid from owning processing threads.
 */
final class WorkManagerTeiidExecutor implements TeiidExecutor {
	WorkManager workManager;
	
	WorkManagerTeiidExecutor(WorkManager workManager) {
		this.workManager = workManager;
	}

	@Override
	public List<Runnable> shutdownNow() {
		workManager = null;
		return Collections.emptyList();
	}

	@Override
	public WorkerPoolStatisticsMetadata getStats() {
		return null;
	}

	@Override
	public void execute(Runnable command) {
		final ThreadReuseExecutor.RunnableWrapper wrapper = new ThreadReuseExecutor.RunnableWrapper(command);
		executeDirect(wrapper);
	}

	private void executeDirect(
			final ThreadReuseExecutor.RunnableWrapper wrapper) {
		try {
			workManager.scheduleWork(wrapper);
		} catch (WorkException e) {
			throw new RejectedExecutionException(e);
		}
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit)
			throws InterruptedException {
		return false;
	}
}