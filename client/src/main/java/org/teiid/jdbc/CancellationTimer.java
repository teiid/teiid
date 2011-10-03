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

import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.logging.Logger;

/**
 * Specialized timer that actively purges tasks in lg(n) time
 */
public class CancellationTimer {
	
	private static AtomicLong id = new AtomicLong();
	
	static abstract class CancelTask implements Runnable, Comparable<CancelTask> {
		final long endTime;
		final long seqId = id.getAndIncrement();
		
		public CancelTask(long delay) {
			this.endTime = System.currentTimeMillis() + delay;
		}
		
		@Override
		public int compareTo(CancelTask o) {
			int result = Long.signum(this.endTime - o.endTime);
			if (result == 0) {
				return Long.signum(seqId - o.seqId);
			}
			return result;
		}
		public boolean equals(Object obj) {
			return obj == this;
		}
	}
	
	private TreeSet<CancelTask> cancelQueue = new TreeSet<CancelTask>();
	private Thread thread;
	
	public CancellationTimer(String name) {
		thread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				while (true) {
					try {
						doCancellations();
					} catch (InterruptedException e) {
						break;
					}
				}
			}
		}, name);
		thread.setDaemon(true);
		thread.start();
	}

	private void doCancellations() throws InterruptedException {
		CancelTask task = null;
		synchronized (this) {
			if (cancelQueue.isEmpty()) {
				this.wait();
				return;
			}
			task = cancelQueue.first();
			long toWait = task.endTime - System.currentTimeMillis();
			if (toWait > 0) {
				this.wait(toWait);
				return;
			}
			cancelQueue.pollFirst();
		}
		try {
			task.run();
		} catch (Throwable t) {
			Logger.getLogger(CancellationTimer.class).error("Unexpected exception running task", t); //$NON-NLS-1$
		}
	}
	
	public void add(CancelTask task) {
		synchronized (this) {
			this.cancelQueue.add(task);
			this.notifyAll();
		}
	}
	
	public void remove(CancelTask task) {
		synchronized (this) {
			this.cancelQueue.remove(task);
			this.notifyAll();
		}
	}
	
	synchronized int getQueueSize() {
		return cancelQueue.size();
	}

}
