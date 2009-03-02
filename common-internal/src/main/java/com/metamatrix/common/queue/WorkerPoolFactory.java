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

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.NamedThreadFactory;

/**
 * Creates WorkPools based upon {@link ThreadPoolExecutor}
 */
public class WorkerPoolFactory {

	/**
	 * Attempts to detect when new threads should be created without blocking.
	 * 
	 * IMPORTANT NOTE: actual execution ordering is not guaranteed.
	 */
	static final class ThreadReuseLinkedBlockingQueue extends
			LinkedBlockingQueue<Runnable> {
		private StatsCapturingThreadPoolExecutor executor;

		void setExecutor(StatsCapturingThreadPoolExecutor executor) {
			this.executor = executor;
		}
		
		@Override
		public boolean offer(Runnable o) {
			if (executor.getPoolSize() + executor.getCompletedCount() >= executor.getSubmittedCount()) {
				/*
				 * TODO: this strategy suffers from the same possible flaws as the previous implementation of
				 * WorkerPool.  If the available threads are in the process of dying, we run the risk of
				 * queuing without starting another thread (fortunately the ThreadPoolExecutor itself will 
				 * try to mitigate this case as well).
				 * 
				 * Since this was never observed to be a problem before, we'll not initially worry about it with
				 * this implementation.
				 */
				return super.offer(o);
			}
			return false; //trigger thread creation if possible
		}

		@Override
		public boolean add(Runnable arg0) {
			if (super.offer(arg0)) {
		        return true;
			}
		    throw new IllegalStateException("Queue full"); //$NON-NLS-1$
		}
	}

	static class StatsCapturingThreadPoolExecutor extends ThreadPoolExecutor {
		
		private AtomicInteger activeCount = new AtomicInteger(0);
		private AtomicInteger submittedCount = new AtomicInteger(0);
		private AtomicInteger completedCount = new AtomicInteger(0);
		
		public StatsCapturingThreadPoolExecutor(int corePoolSize,
                              int maximumPoolSize,
                              long keepAliveTime,
                              TimeUnit unit,
                              BlockingQueue<Runnable> workQueue,
                              ThreadFactory threadFactory) {
			super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
		}
		
		@Override
		protected void beforeExecute(Thread t, Runnable r) {
			activeCount.getAndIncrement();
		}
		
		@Override
		protected void afterExecute(Runnable r, Throwable t) {
			if (t != null) {
				LogManager.logError(LogCommonConstants.CTX_POOLING, t, CommonPlugin.Util.getString("WorkerPool.uncaughtException")); //$NON-NLS-1$
			}
			activeCount.getAndDecrement();
			completedCount.getAndIncrement();
		}
		
		@Override
		public void execute(Runnable command) {
			submittedCount.getAndIncrement();
			super.execute(command);
		}
		
		@Override
		public int getActiveCount() {
			return activeCount.get();
		}
		
		public int getSubmittedCount() {
			return submittedCount.get();
		}
		
		public int getCompletedCount() {
			return completedCount.get();
		}
		
	}
	
	public static class DefaultThreadFactory extends NamedThreadFactory {
		
		public DefaultThreadFactory(String name) {
			super(name);
		}

		public Thread newThread(Runnable r) {
			Thread result = super.newThread(r);
			if (LogManager.isMessageToBeRecorded(LogCommonConstants.CTX_POOLING, MessageLevel.TRACE)) {
				LogManager.logTrace(LogCommonConstants.CTX_POOLING, CommonPlugin.Util.getString("WorkerPool.New_thread", result.getName())); //$NON-NLS-1$
			}
			return result;
		}
	}
	
	static class ExecutorWorkerPool implements WorkerPool {
		
		private String name;
		private StatsCapturingThreadPoolExecutor executor;

		public ExecutorWorkerPool(final String name, StatsCapturingThreadPoolExecutor executor) {
			this.name = name;
			this.executor = executor;
		}
		
		public void execute(Runnable r) {
			this.executor.execute(r);
		}

		public void awaitTermination(long timeout, TimeUnit unit)
				throws InterruptedException {
			this.executor.awaitTermination(timeout, unit);
		}

		public WorkerPoolStats getStats() {
			WorkerPoolStats stats = new WorkerPoolStats();
			stats.name = name;
			stats.queued = executor.getQueue().size();
			stats.threads = executor.getPoolSize();
			stats.activeThreads = executor.getActiveCount();
			stats.totalSubmitted = executor.getSubmittedCount();
			//TODO: highestActiveThreads is misleading for pools that prefer to use new threads
			stats.highestActiveThreads = executor.getLargestPoolSize();
			stats.totalCompleted = executor.getCompletedCount();
			return stats;
		}

		public boolean isTerminated() {
			return this.executor.isTerminated();
		}

		public void shutdown() {
			this.executor.shutdown();
		}

		public boolean hasWork() {
			return this.executor.getSubmittedCount() - this.executor.getCompletedCount() > 0 && !this.executor.isTerminated();
		}

		@Override
		public List<Runnable> shutdownNow() {
			return this.executor.shutdownNow();
		}
	}
	
	/**
	 * Creates a WorkerPool that prefers thread reuse over thread creation based upon the given parameters
	 * 
	 * @param name
	 * @param numThreads the maximum number of worker threads allowed
	 * @param keepAlive keepAlive time in milliseconds - NOT supported until JDK 1.6 for pools that don't prefer existing threads
	 * @return 
	 */
	public static WorkerPool newWorkerPool(String name, int numThreads, long keepAlive) {
		return newWorkerPool(name, numThreads, keepAlive, true);
	}
	
    public static WorkerPool newWorkerPool(String name, final int numThreads, long keepAlive, boolean preferExistingThreads) {
		if (preferExistingThreads && numThreads > 1) {
			final ThreadReuseLinkedBlockingQueue queue = new ThreadReuseLinkedBlockingQueue();
			StatsCapturingThreadPoolExecutor executor = 
				new StatsCapturingThreadPoolExecutor(0, numThreads, keepAlive, TimeUnit.MILLISECONDS, queue, new DefaultThreadFactory(name)) {
				
				@Override
				public void execute(Runnable arg0) {
					if (this.isShutdown()) {
						//bypass the rejection handler
						throw new RejectedExecutionException();
					}
					super.execute(arg0);
				}
				
			};
			queue.setExecutor(executor);
			executor.setRejectedExecutionHandler(new RejectedExecutionHandler() {
				public void rejectedExecution(Runnable arg0,
						ThreadPoolExecutor arg1) {
					try {
						queue.add(arg0);
					} catch (IllegalStateException e) {
						throw new RejectedExecutionException(e);
					}
				}
			});
			return new ExecutorWorkerPool(name, executor);
		}
		StatsCapturingThreadPoolExecutor executor = new StatsCapturingThreadPoolExecutor(numThreads, numThreads, keepAlive, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), new DefaultThreadFactory(name));
		executor.allowCoreThreadTimeOut(true);
		return new ExecutorWorkerPool(name, executor);
	}
	
}
