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

import org.teiid.logging.LogManager;



/**
 * Represents a task that performs work that may take more than one processing pass to complete.
 * During processing the WorkItem may receive events asynchronously through the moreWork method.
 */
public abstract class AbstractWorkItem implements Runnable {

    enum ThreadState {
        MORE_WORK, WORKING, IDLE, DONE
    }

    private ThreadState threadState = ThreadState.MORE_WORK;
    private volatile boolean isProcessing;
    private Object processLock = new Object();

    public void run() {
        startProcessing();
        try {
            synchronized (processLock) {
                process();
            }
        } finally {
            endProcessing();
        }
    }

    synchronized ThreadState getThreadState() {
        return this.threadState;
    }

    public boolean isProcessing() {
        return isProcessing;
    }

    private synchronized void startProcessing() {
        isProcessing = true;
        logTrace("start processing"); //$NON-NLS-1$
        if (this.threadState != ThreadState.MORE_WORK) {
            throw new IllegalStateException("Must be in MORE_WORK"); //$NON-NLS-1$
        }
        this.threadState = ThreadState.WORKING;
    }

    private synchronized void endProcessing() {
        isProcessing = false;
        logTrace("end processing"); //$NON-NLS-1$
        switch (this.threadState) {
            case WORKING:
                if (isDoneProcessing()) {
                    logTrace("done processing"); //$NON-NLS-1$
                    this.threadState = ThreadState.DONE;
                } else {
                    this.threadState = ThreadState.IDLE;
                    pauseProcessing();
                }
                break;
            case MORE_WORK:
                if (isDoneProcessing()) {
                    logTrace("done processing - ignoring more"); //$NON-NLS-1$
                    this.threadState = ThreadState.DONE;
                } else {
                    resumeProcessing();
                }
                break;
            default:
                throw new IllegalStateException("Should not END on " + this.threadState); //$NON-NLS-1$
        }
    }

    public void moreWork() {
        moreWork(true);
    }

    protected synchronized void moreWork(boolean ignoreDone) {
        logTrace("more work"); //$NON-NLS-1$
        this.notifyAll();
        switch (this.threadState) {
            case WORKING:
                this.threadState = ThreadState.MORE_WORK;
                break;
            case MORE_WORK:
                break;
            case IDLE:
                this.threadState = ThreadState.MORE_WORK;
                resumeProcessing();
                break;
            default:
                if (!ignoreDone) {
                    throw new IllegalStateException("More work is not valid once DONE"); //$NON-NLS-1$
                }
                LogManager.logDetail(org.teiid.logging.LogConstants.CTX_DQP, this, "ignoring more work, since the work item is done"); //$NON-NLS-1$
        }
    }

    private void logTrace(String msg) {
        LogManager.logTrace(org.teiid.logging.LogConstants.CTX_DQP, this, msg, this.threadState);
    }

    protected abstract void process();

    protected void pauseProcessing() {
    }

    protected abstract void resumeProcessing();

    protected abstract boolean isDoneProcessing();

    public abstract String toString();

}
