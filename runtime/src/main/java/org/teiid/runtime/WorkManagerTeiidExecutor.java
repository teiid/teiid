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