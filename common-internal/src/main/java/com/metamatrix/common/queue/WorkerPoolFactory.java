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

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogConstants;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.NamedThreadFactory;

/**
 * Creates named, queued, daemon Thread pools.
 * <br/>
 * The requirements are:
 * <ol>
 * <li>minimize thread creation</li>
 * <li>allow for proper timeout of idle threads</li>
 * <li>allow for queuing</li>
 * </ol>
 * <br/>
 * A non-fifo (lifo) {@link SynchronousQueue} based {@link ThreadPoolExecutor} satisfies 1 and 2, but not 3.
 * A bounded or unbound queue based {@link ThreadPoolExecutor} allows for 3, but will tend to create 
 * up to the maximum number of threads and makes no guarantee on thread scheduling.
 * <br/>
 * So the approach here is to use virtual thread pools off of single shared {@link SynchronousQueue}
 * backed {@link ThreadPoolExecutor}.
 * <br/>
 * There is also only a single master scheduling thread with actual executions deferred to the calling
 * WorkerPool.
 * 
 * TODO: this probably needs to be re-thought, especially since the lifo ordering of a {@link SynchronousQueue} 
 * is not guaranteed behavior.  also there's a race condition between previously retiring threads and new work - 
 * prior to being returned to the shared pool we can create extra threads if the shared pool is exhausted.
 * TODO: bounded queuing - we never bothered bounding in the past with our worker pools, but reasonable
 * defaults would be a good idea.
 */
public class WorkerPoolFactory {

	private static ThreadPoolExecutor tpe = new ThreadPoolExecutor(0,
			Integer.MAX_VALUE, 2, TimeUnit.MINUTES,
			new SynchronousQueue<Runnable>(), new NamedThreadFactory("Worker")) { //$NON-NLS-1$ 
		@Override
		protected void afterExecute(Runnable r, Throwable t) {
			if (t != null) {
				LogManager.logError(LogConstants.CTX_POOLING, t, CommonPlugin.Util.getString("WorkerPool.uncaughtException")); //$NON-NLS-1$
			}
		}
	}; 
	
	private static ScheduledThreadPoolExecutor stpe = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("Scheduler")); //$NON-NLS-1$
	
	static class StatsCapturingSharedThreadPoolExecutor implements WorkerPool {
		
		class ScheduledFutureTask extends FutureTask<Void> implements ScheduledFuture<Void> {
			private ScheduledFuture<?> scheduledFuture;
			private boolean periodic;
			private volatile boolean running;
			
			public ScheduledFutureTask(Runnable runnable, boolean periodic) {
				super(runnable, null);
				this.periodic = periodic;
			}
			
			public void setScheduledFuture(ScheduledFuture<?> scheduledFuture) {
				scheduledTasks.add(this);
				this.scheduledFuture = scheduledFuture;
			}
			
			@Override
			public long getDelay(TimeUnit unit) {
				return this.scheduledFuture.getDelay(unit);
			}

			@Override
			public int compareTo(Delayed o) {
				return this.scheduledFuture.compareTo(o);
			}
			
			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				this.scheduledFuture.cancel(false);
				scheduledTasks.remove(this);
				return super.cancel(mayInterruptIfRunning);
			}
			
			public Runnable getParent() {
				return new Runnable() {
					@Override
					public void run() {
						if (running || terminated) {
							return;
						}
						running = periodic;
						execute(ScheduledFutureTask.this);
					}
				};
			}
			
			@Override
			public void run() {
				if (periodic) {
					if (!this.runAndReset()) {
						this.scheduledFuture.cancel(false);
						scheduledTasks.remove(this);
					}
					running = false;
				} else {
					scheduledTasks.remove(this);
					super.run();
				}
			}
		}
		
		private volatile int activeCount;
		private volatile int highestActiveCount;
		private volatile int highestQueueSize;
		private volatile boolean terminated;
		private volatile int submittedCount;
		private volatile int completedCount;
		private Object poolLock = new Object();
		private AtomicInteger threadCounter = new AtomicInteger();
		private Set<Thread> threads = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<Thread, Boolean>()));
		private Set<ScheduledFutureTask> scheduledTasks = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<ScheduledFutureTask, Boolean>()));
		
		private String poolName;
		private int maximumPoolSize;
		private Queue<Runnable> queue = new LinkedList<Runnable>();
		
		public StatsCapturingSharedThreadPoolExecutor(String name, int maximumPoolSize) {
			this.maximumPoolSize = maximumPoolSize;
			this.poolName = name;
		}
		
		@Override
		public void execute(final Runnable command) {
			boolean atMaxThreads = false;
			boolean newMaxQueueSize = false;
			synchronized (poolLock) {
				checkForTermination();
				submittedCount++;
				atMaxThreads = activeCount == maximumPoolSize;
				if (atMaxThreads) {
					queue.add(command);
					int queueSize = queue.size();
					if (queueSize > highestQueueSize) {
						atMaxThreads = true;
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
			tpe.execute(new Runnable() {
				@Override
				public void run() {
					Thread t = Thread.currentThread();
					threads.add(t);
					String name = t.getName();
					t.setName(name + "_" + poolName + threadCounter.getAndIncrement()); //$NON-NLS-1$
					if (LogManager.isMessageToBeRecorded(LogConstants.CTX_POOLING, MessageLevel.TRACE)) {
						LogManager.logTrace(LogConstants.CTX_POOLING, "Beginning work with virtual worker", t.getName()); //$NON-NLS-1$ 
					}
					Runnable r = command;
					while (r != null) {
						boolean success = false;
						try {
							r.run();
							success = true;
						} finally {
							synchronized (poolLock) {
								if (success) {
									completedCount++;
									r = queue.poll();		
								}
								if (!success || r == null) {
									threads.remove(t);
									activeCount--;
									if (activeCount == 0 && terminated) {
										poolLock.notifyAll();
									}		
								}
							}
							t.setName(name);
						}
					}
				};
			});
		}

		private void checkForTermination() {
			if (terminated) {
				throw new RejectedExecutionException();
			}
		}
		
		public int getActiveCount() {
			return activeCount;
		}
		
		public int getSubmittedCount() {
			return submittedCount;
		}
		
		public int getCompletedCount() {
			return completedCount;
		}
		
		public int getPoolSize() {
			return activeCount;
		}
		
		public boolean isTerminated() {
			return terminated;
		}
		
		public void shutdown() {
			this.terminated = true;
			synchronized (scheduledTasks) {
				for (ScheduledFuture<?> future : new ArrayList<ScheduledFuture<?>>(scheduledTasks)) {
					future.cancel(false);
				}
				scheduledTasks.clear();
			}
		}
		
		public int getLargestPoolSize() {
			return this.highestActiveCount;
		}
		
		@Override
		public WorkerPoolStats getStats() {
			WorkerPoolStats stats = new WorkerPoolStats();
			stats.name = poolName;
			stats.queued = queue.size();
			stats.highestQueued = highestQueueSize;
			stats.activeThreads = getActiveCount();
			stats.maxThreads = this.maximumPoolSize;
			stats.totalSubmitted = getSubmittedCount();
			stats.highestActiveThreads = getLargestPoolSize();
			stats.totalCompleted = getCompletedCount();
			return stats;
		}
		
		@Override
		public boolean hasWork() {
			synchronized (poolLock) {
				return this.getSubmittedCount() - this.getCompletedCount() > 0 && !this.isTerminated();
			}
		}

		@Override
		public List<Runnable> shutdownNow() {
			this.shutdown();
			synchronized (poolLock) {
				synchronized (threads) {
					for (Thread t : threads) {
						t.interrupt();
					}
				}
				List<Runnable> result = new ArrayList<Runnable>(queue);
				queue.clear();
				return result;
			}
		}
		
		@Override
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

		@Override
		public ScheduledFuture<?> schedule(final Runnable command, long delay,
				TimeUnit unit) {
			checkForTermination();
			ScheduledFutureTask sft = new ScheduledFutureTask(command, false);
			synchronized (scheduledTasks) {
				ScheduledFuture<?> future = stpe.schedule(sft.getParent(), delay, unit);
				sft.setScheduledFuture(future);
				return sft;
			}
		}

		@Override
		public ScheduledFuture<?> scheduleAtFixedRate(final Runnable command,
				long initialDelay, long period, TimeUnit unit) {
			checkForTermination();
			ScheduledFutureTask sft = new ScheduledFutureTask(command, true);
			synchronized (scheduledTasks) {
				ScheduledFuture<?> future = stpe.scheduleAtFixedRate(sft.getParent(), initialDelay, period, unit);
				sft.setScheduledFuture(future);
				return sft;
			}		
		}
	}
			
	/**
	 * Creates a WorkerPool that prefers thread reuse over thread creation based upon the given parameters
	 * 
	 * @param name
	 * @param numThreads the maximum number of worker threads allowed
	 * @return 
	 */
	public static WorkerPool newWorkerPool(String name, int numThreads) {
		return new StatsCapturingSharedThreadPoolExecutor(name, numThreads);
	}
    
}
