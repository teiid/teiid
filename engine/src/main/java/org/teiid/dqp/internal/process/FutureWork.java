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

package org.teiid.dqp.internal.process;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.teiid.dqp.internal.process.DQPCore.CompletionListener;
import org.teiid.dqp.internal.process.ThreadReuseExecutor.PrioritizedRunnable;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;

public final class FutureWork<T> extends FutureTask<T> implements PrioritizedRunnable {
    private int priority;
    private long creationTime = System.currentTimeMillis();
    private DQPWorkContext workContext = DQPWorkContext.getWorkContext();
    private List<CompletionListener<T>> completionListeners = new LinkedList<CompletionListener<T>>();
    private String parentName;
    private String requestId;

    public FutureWork(final Callable<T> processor, int priority) {
        super(processor);
        this.parentName = Thread.currentThread().getName();
        this.priority = priority;
    }

    public FutureWork(final Runnable processor, T result, int priority) {
        super(processor, result);
        this.priority = priority;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    @Override
    public void run() {
        LogManager.logDetail(LogConstants.CTX_DQP, "Running task for parent thread", parentName); //$NON-NLS-1$
        LogManager.putMdc(RequestWorkItem.REQUEST_KEY, requestId);
        try {
            super.run();
        } finally {
            LogManager.removeMdc(RequestWorkItem.REQUEST_KEY);
        }
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