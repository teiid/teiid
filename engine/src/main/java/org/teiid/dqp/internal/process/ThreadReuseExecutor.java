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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
 * <br>
 * A non-fifo (lifo) {@link SynchronousQueue} based {@link ThreadPoolExecutor} satisfies 1 and 2, but not 3.
 * A bounded or unbound queue based {@link ThreadPoolExecutor} allows for 3, but will tend to create
 * up to the maximum number of threads and makes no guarantee on thread scheduling.
 * <br>
 * So the approach here is to use a virtual thread pool off of a {@link SynchronousQueue}
 * backed {@link ThreadPoolExecutor}.
 * <br>
 * There is also only a single master scheduling thread with actual executions deferred.
 *
 * TODO: there is a race condition between retiring threads and adding work, which may create extra threads.
 * That is a flaw with attempting to reuse, rather than create threads.
 * TODO: bounded queuing - we never bothered bounding in the past with our worker pools, but reasonable
 * defaults would be a good idea.
 *
 * TODO: a {@link ForkJoinPool} is a simple replacement, but we'd loose the prioritization queue.
 */
public class ThreadReuseExecutor implements TeiidExecutor {

    public interface PrioritizedRunnable extends Runnable {

        final static int NO_WAIT_PRIORITY = 0;

        /**
         * The execution priority - higher is lower
         */
        int getPriority();

        long getCreationTime();

        DQPWorkContext getDqpWorkContext();

    }

    private static AtomicLong ID_GEN = new AtomicLong();

    public static class RunnableWrapper implements PrioritizedRunnable, Comparable<RunnableWrapper> {
        Runnable r;
        DQPWorkContext workContext = DQPWorkContext.getWorkContext();
        long creationTime;
        int priority;
        long id = ID_GEN.getAndIncrement();

        public RunnableWrapper(Runnable r) {
            if (r instanceof PrioritizedRunnable) {
                PrioritizedRunnable pr = (PrioritizedRunnable)r;
                creationTime = pr.getCreationTime();
                priority = pr.getPriority();
                workContext = pr.getDqpWorkContext();
            } else {
                //this will be considered optional work that will only get completed
                //when the queue is drained
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

        @Override
        public int compareTo(RunnableWrapper o) {
            int comp = Integer.compare(this.priority, o.priority);
            if (comp != 0) {
                return comp;
            }
            //don't use creation time, only the id as that will get reset with each queuing
            return Long.compare(this.id, o.id);
        }

    }

    private final ThreadPoolExecutor tpe;

    private volatile int activeCount;
    private volatile int highestActiveCount;
    private volatile int highestQueueSize;
    private volatile boolean terminated;
    private volatile int submittedCount;
    private volatile int completedCount;
    private Object poolLock = new Object();
    private AtomicInteger threadCounter = new AtomicInteger();
    private Set<Thread> threads = Collections.newSetFromMap(new ConcurrentHashMap<Thread, Boolean>());

    private String poolName;
    private int maximumPoolSize;
    private Queue<RunnableWrapper> queue = new PriorityBlockingQueue<RunnableWrapper>(11);

    private long warnWaitTime = 500;

    public ThreadReuseExecutor(String name, int maximumPoolSize) {
        this.maximumPoolSize = maximumPoolSize;
        this.poolName = name;

        tpe = new ThreadPoolExecutor(0,
                Integer.MAX_VALUE, 2, TimeUnit.MINUTES,
                new SynchronousQueue<Runnable>(), new NamedThreadFactory("Worker")) { //$NON-NLS-1$
            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                if (t != null) {
                    LogManager.logError(LogConstants.CTX_RUNTIME, t, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30021));
                }
            }

        };
    }

    public void execute(final Runnable command) {
        executeDirect(new RunnableWrapper(command));
    }

    private void executeDirect(final RunnableWrapper command) {
        checkForTermination();
        synchronized (poolLock) {
            submittedCount++;
            boolean atMaxThreads = activeCount == maximumPoolSize;
            if (atMaxThreads) {
                queue.add(command);
                int queueSize = queue.size();
                if (queueSize > highestQueueSize) {
                    highestQueueSize = queueSize;
                }
                return;
            }
            activeCount++;
            highestActiveCount = Math.max(activeCount, highestActiveCount);
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
                                logWaitMessage(warnTime, maximumPoolSize, poolName, highestQueueSize);
                                warnWaitTime*=2; //we don't really care if this is synchronized
                            }
                        }
                        t.setName(name);
                    }
                }
            }

        });
    }

    protected void logWaitMessage(long warnTime, int maximumPoolSize, String poolName, int highestQueueSize) {
        LogManager.logWarning(LogConstants.CTX_RUNTIME, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30009, maximumPoolSize, poolName, highestQueueSize, warnTime));
    }

    private void checkForTermination() {
        if (terminated) {
            throw new RejectedExecutionException();
        }
    }

    public int getActiveCount() {
        return activeCount;
    }

    public long getSubmittedCount() {
        return submittedCount;
    }

    public long getCompletedCount() {
        return completedCount;
    }

    public boolean isTerminated() {
        return terminated;
    }

    public void shutdown() {
        this.terminated = true;
    }

    public int getLargestPoolSize() {
        return this.highestActiveCount;
    }

    public int getQueued() {
        return queue.size();
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

    public List<Runnable> shutdownNow() {
        this.shutdown();
        synchronized (poolLock) {
            for (Thread t : threads) {
                t.interrupt();
            }
            List<Runnable> result = new ArrayList<Runnable>(queue);
            queue.clear();
            result.addAll(this.tpe.shutdownNow());
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

}
