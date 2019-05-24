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

package org.teiid.client.util;

import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.teiid.jdbc.JDBCPlugin;


/**
 * Implements a call back based future that can also have
 * completion listeners.
 */
public class ResultsFuture<T> implements Future<T> {

    public static final ResultsFuture<Void> NULL_FUTURE = new ResultsFuture<Void>();

    static {
        NULL_FUTURE.getResultsReceiver().receiveResults(null);
    }
    private static final Logger logger = Logger.getLogger("org.teiid"); //$NON-NLS-1$

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
                try {
                    completionListener.onCompletion(this);
                } catch (Throwable t) {
                    logger.log(Level.SEVERE, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20031), t);
                }
            }
            this.listeners.clear();
        }
    }

    public void addCompletionListener(CompletionListener<T> listener) {
        synchronized (this) {
            if (done) {
                try {
                    listener.onCompletion(this);
                } catch (Throwable t) {
                    logger.log(Level.SEVERE, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20031), t);
                }
                return;
            }
            synchronized (this.listeners) {
                this.listeners.add(listener);
            }
        }
    }

}
