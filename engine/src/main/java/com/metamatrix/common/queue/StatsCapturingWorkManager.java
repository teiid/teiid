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

package com.metamatrix.common.queue;

import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.resource.spi.work.ExecutionContext;
import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkEvent;
import javax.resource.spi.work.WorkException;
import javax.resource.spi.work.WorkListener;
import javax.resource.spi.work.WorkManager;
import javax.resource.spi.work.WorkRejectedException;

import org.teiid.adminapi.impl.WorkerPoolStatisticsMetadata;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogConstants;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.NamedThreadFactory;

/**
 * StatsCapturingWorkManager acts as a wrapper to the passed in {@link WorkManager} to 
 * capture statistics and implement an unbounded queue of work.
 */
public class StatsCapturingWorkManager {
		
	private static class WorkContext {
		ExecutionContext context;
		long startTimeout;
		long submitted = System.currentTimeMillis();
		
		public WorkContext(ExecutionContext context, long startTimeout) {
			this.context = context;
			this.startTimeout = startTimeout;
		}
		
		long getStartTimeout() {
			if (startTimeout == 0) {
				return 0;
			}
			return Math.max(1, startTimeout + submitted - System.currentTimeMillis());
		}
		
	}
	
	private final class WorkWrapper implements Work {
		private final WorkManager delegate;
		private final Work work;
		private final WorkContext workContext;

		private WorkWrapper(WorkManager delegate, Work work, WorkContext workContext) {
			this.delegate = delegate;
			this.work = work;
			this.workContext = workContext;
		}

		@Override
		public void run() {
			Thread t = Thread.currentThread();
			synchronized (poolLock) {
				threads.add(t);
			}
			String name = t.getName();
			t.setName(name + "_" + poolName + threadCounter.getAndIncrement()); //$NON-NLS-1$
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_POOLING, MessageLevel.TRACE)) {
				LogManager.logTrace(LogConstants.CTX_POOLING, "Beginning work with virtual worker", t.getName()); //$NON-NLS-1$ 
			}
			boolean success = false;
			try {
				work.run();
				success = true;
			} finally {
				synchronized (poolLock) {
					WorkWrapper next = null;
					if (success) {
						completedCount++;
						next = queue.poll();		
					}
					threads.remove(t);
					if (next == null) {
						activeCount--;
						if (activeCount == 0 && terminated) {
							poolLock.notifyAll();
						}		
					} else {
						try {
							if (next.workContext == null) {
								delegate.scheduleWork(next);
							} else {
								delegate.scheduleWork(next, next.workContext.getStartTimeout(), next.workContext.context, next.work instanceof WorkListener?(WorkListener)next.work:null);
							}
						} catch (WorkException e) {
							handleException(next.work, e);
						}
					}
				}
				t.setName(name);
			}
		}

		@Override
		public void release() {
			this.work.release();
		}
	}
	
	private static void handleException(Work work, WorkException e) {
		if (work instanceof WorkListener) {
			((WorkListener)work).workRejected(new WorkEvent(work, WorkEvent.WORK_REJECTED, work, new WorkRejectedException(e)));
		} else if (LogManager.isMessageToBeRecorded(LogConstants.CTX_POOLING, MessageLevel.DETAIL)) {
			LogManager.logDetail(LogConstants.CTX_POOLING, e, "Exception adding work to the WorkManager"); //$NON-NLS-1$ 
		}
	}
		
	private static ScheduledThreadPoolExecutor stpe = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("Scheduler")); //$NON-NLS-1$

	private volatile int activeCount;
	private volatile int highestActiveCount;
	private volatile int highestQueueSize;
	private volatile boolean terminated;
	private volatile int submittedCount;
	private volatile int completedCount;
	private Object poolLock = new Object();
	private AtomicInteger threadCounter = new AtomicInteger();
	private String poolName;
	private int maximumPoolSize;
	private Queue<WorkWrapper> queue = new LinkedList<WorkWrapper>();
	private Set<Thread> threads = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<Thread, Boolean>()));
	private Map<Integer, ScheduledFuture<?>> futures = new HashMap<Integer, ScheduledFuture<?>>();
	private int idCounter;
	
	public StatsCapturingWorkManager(String name, int maximumPoolSize) {
		this.maximumPoolSize = maximumPoolSize;
		this.poolName = name;
	}
	
	public void scheduleWork(final WorkManager delegate, final Work arg0) throws WorkException {
		scheduleWork(delegate, arg0, (WorkContext)null);
	}

	private void scheduleWork(final WorkManager delegate, final Work work, WorkContext workContext) 
	throws WorkRejectedException, WorkException {
		boolean atMaxThreads = false;
		boolean newMaxQueueSize = false;
		synchronized (poolLock) {
			checkForTermination();
			submittedCount++;
			atMaxThreads = activeCount == maximumPoolSize;
			if (atMaxThreads) {
				queue.add(new WorkWrapper(delegate, work, workContext));
				int queueSize = queue.size();
				if (queueSize > highestQueueSize) {
					newMaxQueueSize = true;
					highestQueueSize = queueSize;
				}
			} else {
				activeCount++;
				highestActiveCount = Math.max(activeCount, highestActiveCount);
			}
		}
		if (atMaxThreads) {
			if (newMaxQueueSize && maximumPoolSize > 1) {
				LogManager.logWarning(LogConstants.CTX_POOLING, CommonPlugin.Util.getString("WorkerPool.Max_thread", maximumPoolSize, poolName, highestQueueSize)); //$NON-NLS-1$
			}
			return;
		}
		if (workContext == null) {
			delegate.scheduleWork(new WorkWrapper(delegate, work, null));
		} else {
			delegate.scheduleWork(new WorkWrapper(delegate, work, null), workContext.getStartTimeout(), workContext.context, work instanceof WorkListener?(WorkListener)work:null);
		}
	}

	public void scheduleWork(final WorkManager delegate, final Work arg0, final ExecutionContext arg2, long delay) throws WorkException {
		if (delay < 1) {
			scheduleWork(delegate, arg0, new WorkContext(arg2, WorkManager.INDEFINITE));
		} else {
			synchronized (futures) {
				final int id = idCounter++;
				ScheduledFuture<?> sf = stpe.schedule(new Runnable() {
					
					@Override
					public void run() {
						try {
							futures.remove(id);
							scheduleWork(delegate, arg0, new WorkContext(arg2, WorkManager.INDEFINITE));
						} catch (WorkException e) {
							handleException(arg0, e);
						}
					}
				}, delay, TimeUnit.MILLISECONDS);
				this.futures.put(id, sf);
			}			
		}
	}
	
	private void checkForTermination() throws WorkRejectedException {
		if (terminated) {
			throw new WorkRejectedException("Queue has been terminated"); //$NON-NLS-1$
		}
	}
		
	public WorkerPoolStatisticsMetadata getStats() {
		WorkerPoolStatisticsMetadata stats = new WorkerPoolStatisticsMetadata();
		stats.setName(poolName);
		stats.setQueued(queue.size());
		stats.setHighestQueued(highestQueueSize);
		stats.setActiveThreads(this.activeCount);
		stats.setMaxThreads(this.maximumPoolSize);
		stats.setTotalSubmitted(this.submittedCount);
		stats.setHighestActiveThreads(this.highestActiveCount);
		stats.setTotalCompleted(this.completedCount);
		return stats;		
	}
	
	public void shutdown() {
		this.terminated = true;
	}
	
	public void shutdownNow() {
		this.shutdown();
		synchronized (poolLock) {
			for (Thread t : threads) {
				t.interrupt();
			}
			queue.clear();
		}
		synchronized (futures) {
			for (ScheduledFuture<?> future : futures.values()) {
				future.cancel(true);
			}
			futures.clear();
		}
	}
	
	public boolean isTerminated() {
		return terminated;
	}
	
	public boolean awaitTermination(long timeout, TimeUnit unit)
			throws InterruptedException {
		long timeoutMillis = unit.toMillis(timeout);
		long finalMillis = System.currentTimeMillis() + timeoutMillis;
		synchronized (poolLock) {
			while (this.activeCount > 0 || !terminated) {
				if (timeoutMillis < 1) {
					return false;
				}
				poolLock.wait(timeoutMillis);
				timeoutMillis = finalMillis - System.currentTimeMillis();
			}
		}
		return true;
	}

}
