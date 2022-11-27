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

package org.teiid.jdbc;

import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.teiid.core.util.ExecutorUtils;

/**
 * Specialized timer that can purge tasks in lg(n) time
 * Will only hold a thread while there are pending tasks.
 */
public class EnhancedTimer {

    private static final Logger LOGGER = Logger.getLogger("org.teiid.jdbc"); //$NON-NLS-1$
    private static AtomicLong id = new AtomicLong();

    public class Task extends FutureTask<Void> implements Comparable<Task> {
        final long endTime;
        final long seqId = id.getAndIncrement();

        public Task(Runnable task, long delay) {
            super(task, null);
            this.endTime = System.currentTimeMillis() + delay;
        }

        @Override
        public int compareTo(Task o) {
            int result = Long.signum(this.endTime - o.endTime);
            if (result == 0) {
                return Long.signum(seqId - o.seqId);
            }
            return result;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (!isDone()) {
                queue.remove(this);
            }
            return super.cancel(mayInterruptIfRunning);
        }

        public void cancel() {
            cancel(false);
        }

    }

    private final ConcurrentSkipListSet<Task> queue = new ConcurrentSkipListSet<Task>();
    private final Executor taskExecutor;
    private final Executor bossExecutor;
    private boolean running;

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
                    while (doTasks()) {
                    }
                } catch (InterruptedException e) {
                }
            }
        });
        running = true;
    }

    private boolean doTasks() throws InterruptedException {
        Task task = null;
        try {
            task = queue.first();
        } catch (NoSuchElementException e) {
            synchronized (this) {
                if (queue.isEmpty()) {
                    running = false;
                    return false;
                }
                return true;
            }
        }
        long toWait = task.endTime - System.currentTimeMillis();
        if (toWait > 0) {
            synchronized (this) {
                this.wait(toWait);
                return true; //try again (guards against spurious wake-ups)
            }
        }
        if (task.isCancelled()) {
            return true;
        }
        queue.remove(task);
        try {
            taskExecutor.execute(task);
        } catch (Throwable t) {
            LOGGER.log(Level.SEVERE, "Unexpected exception running task", t); //$NON-NLS-1$
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
        }
        return result;
    }

    public int getQueueSize() {
        return queue.size();
    }

}
