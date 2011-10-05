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

package org.teiid.jdbc;

import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.jboss.logging.Logger;
import org.teiid.core.util.ExecutorUtils;

/**
 * Specialized timer that can purge tasks in lg(n) time
 * Uses lock escalation to minimize contention for adding/removing tasks.
 * Will only hold a thread while there are pending tasks.
 */
public class EnhancedTimer {
	
	private static final Logger LOGGER = Logger.getLogger(EnhancedTimer.class);
	private static AtomicLong id = new AtomicLong();
	
	public class Task implements Comparable<Task>, Runnable {
		final long endTime;
		final long seqId = id.getAndIncrement();
		final Runnable task;
		
		public Task(Runnable task, long delay) {
			this.endTime = System.currentTimeMillis() + delay;
			this.task = task;
		}
		
		@Override
		public void run() {
			this.task.run();
		}
		
		@Override
		public int compareTo(Task o) {
			int result = Long.signum(this.endTime - o.endTime);
			if (result == 0) {
				return Long.signum(seqId - o.seqId);
			}
			return result;
		}
		
		public boolean cancel() {
			lock.readLock().lock();
			try {
				return queue.remove(this);
			} finally {
				lock.readLock().unlock();
			}
		}
	}
	
	private final ConcurrentSkipListSet<Task> queue = new ConcurrentSkipListSet<Task>();
	private final Executor taskExecutor;
	private final Executor bossExecutor;
	private boolean running;
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	
	/**
	 * Constructs a new Timer that directly executes tasks off of a single-thread thread pool.
	 * @param name
	 */
	public EnhancedTimer(final String name) {
		this.taskExecutor = ExecutorUtils.getDirectExecutor();
		this.bossExecutor = ExecutorUtils.newFixedThreadPool(1, name);
	}
	
	public EnhancedTimer(Executor bossExecutor, Executor taskExecutor) {
		this.taskExecutor = taskExecutor;
		this.bossExecutor = bossExecutor;
	}

	private void start() {
		bossExecutor.execute(new Runnable() {
			
			@Override
			public void run() {
				try {
					while (doCancellations()) {
					}
				} catch (InterruptedException e) {
				}
			}
		});
		running = true;
	}
	
	private boolean doCancellations() throws InterruptedException {
		Task task = null;
		lock.writeLock().lock();
		try {
			if (queue.isEmpty()) {
				synchronized (this) {
					lock.writeLock().unlock();
					running = false;
					return false;
				}
			}
			task = queue.first();
			long toWait = task.endTime - System.currentTimeMillis();
			if (toWait > 0) {
				synchronized (this) {
					lock.writeLock().unlock();
					this.wait(toWait);
					return true; //try again (guards against spurious wake-ups)
				}
			}
			queue.pollFirst();
		} finally {
			if (lock.writeLock().isHeldByCurrentThread()) {
				lock.writeLock().unlock();
			}
		}
		try {
			taskExecutor.execute(task);
		} catch (Throwable t) {
			LOGGER.error("Unexpected exception running task", t); //$NON-NLS-1$
		}
		return true;
	}
	
	/**
	 * Add a delayed task
	 * @param task
	 * @param delay in ms
	 * @return a cancellable Task
	 */
	public Task add(Runnable task, long delay) {
		Task result = new Task(task, delay);
		lock.readLock().lock();
		try {
			if (this.queue.add(result) 
					&& this.queue.first() == result) {
				//only need to synchronize when this is the first task
				synchronized (this) {
					if (!running) {
						start();
					}
					this.notifyAll();
				}
			}
		} catch (NoSuchElementException e) {
			//shouldn't happen
		} finally {
			lock.readLock().unlock();
		}
		return result;
	}
	
	public int getQueueSize() {
		return queue.size();
	}

}
