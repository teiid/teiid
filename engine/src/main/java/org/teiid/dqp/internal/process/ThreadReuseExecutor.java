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

package org.teiid.dqp.internal.process;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.adminapi.impl.WorkerPoolStatisticsMetadata;
import org.teiid.core.util.NamedThreadFactory;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.QueryPlugin;

/**
 * An Executor that:
 * <ol>
 * <li>minimizes thread creation</li>
 * <li>allows for proper timeout of idle threads</li>
 * <li>allows for queuing</li>
 * </ol>
 * <br/>
 * A non-fifo (lifo) {@link SynchronousQueue} based {@link ThreadPoolExecutor} satisfies 1 and 2, but not 3.
 * A bounded or unbound queue based {@link ThreadPoolExecutor} allows for 3, but will tend to create 
 * up to the maximum number of threads and makes no guarantee on thread scheduling.
 * <br/>
 * So the approach here is to use a virtual thread pool off of a {@link SynchronousQueue}
 * backed {@link ThreadPoolExecutor}.
 * <br/>
 * There is also only a single master scheduling thread with actual executions deferred.
 * 
 * TODO: there is a race condition between retiring threads and adding work, which may create extra threads.  
 * That is a flaw with attempting to reuse, rather than create threads.  
 * TODO: bounded queuing - we never bothered bounding in the past with our worker pools, but reasonable
 * defaults would be a good idea.
 */
public class ThreadReuseExecutor implements Executor {
	
	public interface PrioritizedRunnable extends Runnable {
		
		final static int NO_WAIT_PRIORITY = 0;
		
		int getPriority();
		
		long getCreationTime();
		
		DQPWorkContext getDqpWorkContext();
		
	}
	
	static class RunnableWrapper implements PrioritizedRunnable {
		Runnable r;
		DQPWorkContext workContext = DQPWorkContext.getWorkContext();
		long creationTime;
		int priority;
		
		public RunnableWrapper(Runnable r) {
			if (r instanceof PrioritizedRunnable) {
				PrioritizedRunnable pr = (PrioritizedRunnable)r;
				creationTime = pr.getCreationTime();
				priority = pr.getPriority();
				workContext = pr.getDqpWorkContext();
			} else {
				creationTime = System.currentTimeMillis();
				priority = Integer.MAX_VALUE;
			}
			this.r = r;
		}
		
		@Override
		public long getCreationTime() {
			return creationTime;
		}

		@Override
		public int getPriority() {
			return priority;
		}

		@Override
		public void run() {
			if (workContext.getSecurityHelper() != null) {
				//if using the inheritable thread local security or if unassocation has been sloppy, there may a security context associated
				workContext.getSecurityHelper().clearSecurityContext();
			}			
			workContext.runInContext(r);
		}
		
		public DQPWorkContext getDqpWorkContext() {
			return workContext;
		}
		
	}
	
	private final ThreadPoolExecutor tpe; 
	
	private ScheduledThreadPoolExecutor stpe = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("Scheduler")); //$NON-NLS-1$
	
	class ScheduledFutureTask extends FutureTask<Void> implements ScheduledFuture<Void>, PrioritizedRunnable {
		private ScheduledFuture<?> scheduledFuture;
		private boolean periodic;
		private volatile boolean running;
		private PrioritizedRunnable runnable;
		
		public ScheduledFutureTask(PrioritizedRunnable runnable, boolean periodic) {
			super(runnable, null);
			this.periodic = periodic;
			this.runnable = runnable;
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
					executeDirect(ScheduledFutureTask.this);
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

		@Override
		public long getCreationTime() {
			return runnable.getCreationTime();
		}

		@Override
		public int getPriority() {
			return runnable.getPriority();
		}
		
		@Override
		public DQPWorkContext getDqpWorkContext() {
			return runnable.getDqpWorkContext();
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
	private Queue<PrioritizedRunnable> queue = new PriorityQueue<PrioritizedRunnable>(11, new Comparator<PrioritizedRunnable>() {
		@Override
		public int compare(PrioritizedRunnable pr1, PrioritizedRunnable pr2) {
			int result = pr1.getPriority() - pr2.getPriority();
			if (result == 0) {
				return Long.signum(pr1.getCreationTime() - pr2.getCreationTime());
			}
			return result;
		}
	});
	private long warnWaitTime = 500;
	
	public ThreadReuseExecutor(String name, int maximumPoolSize) {
		this.maximumPoolSize = maximumPoolSize;
		this.poolName = name;
		
		tpe = new ThreadPoolExecutor(0,
				maximumPoolSize, 2, TimeUnit.MINUTES,
				new SynchronousQueue<Runnable>(), new NamedThreadFactory("Worker")) { //$NON-NLS-1$ 
			@Override
			protected void afterExecute(Runnable r, Throwable t) {
				if (t != null) {
					LogManager.logError(LogConstants.CTX_RUNTIME, t, QueryPlugin.Util.getString("WorkerPool.uncaughtException")); //$NON-NLS-1$
				}
			}
			
		};
	}
	
	public void execute(final Runnable command) {
		executeDirect(new RunnableWrapper(command));
	}

	private void executeDirect(final PrioritizedRunnable command) {
		boolean atMaxThreads = false;
		synchronized (poolLock) {
			checkForTermination();
			submittedCount++;
			atMaxThreads = activeCount == maximumPoolSize;
			if (atMaxThreads) {
				queue.add(command);
				int queueSize = queue.size();
				if (queueSize > highestQueueSize) {
					highestQueueSize = queueSize;
				}
			} else {
				activeCount++;
				highestActiveCount = Math.max(activeCount, highestActiveCount);
			}
		}
		if (atMaxThreads) {
			return;
		}
		tpe.execute(new Runnable() {
			@Override
			public void run() {
				Thread t = Thread.currentThread();
				threads.add(t);
				String name = t.getName();
				t.setName(name + "_" + poolName + threadCounter.getAndIncrement()); //$NON-NLS-1$
				if (LogManager.isMessageToBeRecorded(LogConstants.CTX_RUNTIME, MessageLevel.TRACE)) {
					LogManager.logTrace(LogConstants.CTX_RUNTIME, "Beginning work with virtual worker", t.getName()); //$NON-NLS-1$ 
				}
				PrioritizedRunnable r = command;
				while (r != null) {
					boolean success = false;
					try {
						r.run();
						success = true;
					} finally {
						synchronized (poolLock) {
							if (success) {
								completedCount++;
								//we only poll if successful, to let the exception handling happen immediately otherwise
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
						if (success) {
							long warnTime = warnWaitTime;
							if (r != null && System.currentTimeMillis() - r.getCreationTime() > warnTime) {
								LogManager.logWarning(LogConstants.CTX_RUNTIME, QueryPlugin.Util.getString("WorkerPool.Max_thread", maximumPoolSize, poolName, highestQueueSize, warnTime)); //$NON-NLS-1$
								warnWaitTime*=2; //we don't really care if this is synchronized
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
	
	public WorkerPoolStatisticsMetadata getStats() {
		WorkerPoolStatisticsMetadata stats = new WorkerPoolStatisticsMetadata();
		stats.setName(poolName);
		stats.setQueued(queue.size());
		stats.setHighestQueued(highestQueueSize);
		stats.setActiveThreads(getActiveCount());
		stats.setMaxThreads(this.maximumPoolSize);
		stats.setTotalSubmitted(getSubmittedCount());
		stats.setHighestActiveThreads(getLargestPoolSize());
		stats.setTotalCompleted(getCompletedCount());
		return stats;
	}
	
	public boolean hasWork() {
		synchronized (poolLock) {
			return this.getSubmittedCount() - this.getCompletedCount() > 0 && !this.isTerminated();
		}
	}

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

	public ScheduledFuture<?> schedule(final Runnable command, long delay,
			TimeUnit unit) {
		checkForTermination();
		ScheduledFutureTask sft = new ScheduledFutureTask(new RunnableWrapper(command), false);
		synchronized (scheduledTasks) {
			ScheduledFuture<?> future = stpe.schedule(sft.getParent(), delay, unit);
			sft.setScheduledFuture(future);
			return sft;
		}
	}

	public ScheduledFuture<?> scheduleAtFixedRate(final Runnable command,
			long initialDelay, long period, TimeUnit unit) {
		checkForTermination();
		ScheduledFutureTask sft = new ScheduledFutureTask(new RunnableWrapper(command), true);
		synchronized (scheduledTasks) {
			ScheduledFuture<?> future = stpe.scheduleAtFixedRate(sft.getParent(), initialDelay, period, unit);
			sft.setScheduledFuture(future);
			return sft;
		}		
	}
			
}
