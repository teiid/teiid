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

package org.teiid.client.util;

import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;



/**
 * Implements a call back based future that can also have
 * completion listeners.
 */
public class ResultsFuture<T> implements Future<T> {
	
	public static ResultsFuture<Void> NULL_FUTURE = new ResultsFuture<Void>();
	
	static {
		NULL_FUTURE.getResultsReceiver().receiveResults(null);
	}
	
	public interface CompletionListener<T> {
		void onCompletion(ResultsFuture<T> future);
	}
	
	private LinkedList<CompletionListener<T>> listeners = new LinkedList<CompletionListener<T>>();
	
	private T result;
	private Throwable exception;
	private boolean done;
	private ResultsReceiver<T> resultsReceiver = new ResultsReceiver<T> () {
		public void exceptionOccurred(Throwable e) {
			synchronized (ResultsFuture.this) {
				if (done) {
					throw new IllegalStateException("Already sent results"); //$NON-NLS-1$
				}
				exception = e;
				done = true;
				ResultsFuture.this.notifyAll();
			}
			done();
		}
		public void receiveResults(T results) {
			synchronized (ResultsFuture.this) {
				if (done) {
					throw new IllegalStateException("Already sent results"); //$NON-NLS-1$
				}
				result = results;
				done = true;
				ResultsFuture.this.notifyAll();
			}
			done();
		}
	}; 
	
	public ResultsFuture() {
		
	}
	
	public ResultsReceiver<T> getResultsReceiver() {
		return resultsReceiver; 
	}
	
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	public synchronized T get() throws InterruptedException, ExecutionException {
		while (!this.done) {
			this.wait();
		}
		return convertResult();
	}
	
	protected T convertResult() throws ExecutionException {
		if (exception != null) {
			throw new ExecutionException(exception);
		}
		return result;
	}

	public synchronized T get(long timeout, TimeUnit unit) throws InterruptedException,
			ExecutionException, TimeoutException {
		long millis = unit.toMillis(timeout);
		long start = System.currentTimeMillis();
		while (!this.done) {
			long waitTill = start + millis - System.currentTimeMillis();
			if (waitTill <= 0) {
				throw new TimeoutException();
			}
			this.wait(waitTill);
		}
		return convertResult();
	}

	public boolean isCancelled() {
		return false;
	}

	public synchronized boolean isDone() {
		return done;
	}
	
	private void done() {
		synchronized (this.listeners) {
			for (CompletionListener<T> completionListener : this.listeners) {
				completionListener.onCompletion(this);
			}
			this.listeners.clear();
		}
	}
	
	public void addCompletionListener(CompletionListener<T> listener) {
		synchronized (this) {
			if (done) {
				listener.onCompletion(this);
				return;
			}
			synchronized (this.listeners) {
				this.listeners.add(listener);
			}
		}
	}
	
}
