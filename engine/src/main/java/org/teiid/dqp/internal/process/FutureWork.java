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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import javax.resource.spi.work.Work;

import org.teiid.dqp.internal.process.DQPCore.CompletionListener;
import org.teiid.dqp.internal.process.ThreadReuseExecutor.PrioritizedRunnable;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;

public final class FutureWork<T> extends FutureTask<T> implements PrioritizedRunnable, Work {
	private int priority;
	private long creationTime = System.currentTimeMillis();
	private DQPWorkContext workContext = DQPWorkContext.getWorkContext();
	private List<CompletionListener<T>> completionListeners = new LinkedList<CompletionListener<T>>();
	private String parentName;

	public FutureWork(final Callable<T> processor, int priority) {
		super(processor);
		this.parentName = Thread.currentThread().getName();
		this.priority = priority;
	}
	
	public FutureWork(final Runnable processor, T result, int priority) {
		super(processor, result);
		this.priority = priority;
	}
	
	@Override
	public void run() {
		LogManager.logDetail(LogConstants.CTX_DQP, "Running task for parent thread", parentName); //$NON-NLS-1$
		super.run();
	}
	
	@Override
	public int getPriority() {
		return priority;
	}
	
	@Override
	public long getCreationTime() {
		return creationTime;
	}
	
	@Override
	public DQPWorkContext getDqpWorkContext() {
		return workContext;
	}
	
	@Override
	public void release() {
		
	}
	
	synchronized void addCompletionListener(CompletionListener<T> completionListener) {
		if (this.isDone()) {
			completionListener.onCompletion(this);
			return;
		}
		this.completionListeners.add(completionListener);
	}
	
	@Override
	protected synchronized void done() {
		for (CompletionListener<T> listener : this.completionListeners) {
			try {
				listener.onCompletion(this);
			} catch (Throwable t) {
				LogManager.logError(LogConstants.CTX_DQP, t, "Uncaught throwable from completion listener"); //$NON-NLS-1$
			}
		}
		completionListeners.clear();
	}
	
}