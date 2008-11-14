/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.cdk;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.commandshell.CommandShell;
import com.metamatrix.core.util.DateUtil;

/**
 * Adds testing capabilities to the base class.
 */
public class MetaMatrixCommandTarget extends BaseMetaMatrixCommandTarget implements Cloneable {
  
    // State held in the launched shell during a load test, then copied back into the main shell at the end
    private int queryCount = 0;    
    private long latencyTimerElapsed = 0;

    //Number of queries since last display.
    private volatile int intervalQueryCount = 0;
    private static int queryDisplayInterval = Integer.MAX_VALUE;
    
    private List loadTestResults = new ArrayList();
    private int testThreadCount = 0;
    private String loadTestName;
    
    private Object lock = new Object();
    private int waitingThreadCount = 0;
    
    // State for current load test
    private long loadTestStart = 0;
    private long loadTestEnd = 0;

    private void processLoadTestSummary() {
        double averageLatency = (this.queryCount == 0 ? 0 : ((this.latencyTimerElapsed +0.0d)/ this.queryCount));
                
        long duration = loadTestEnd - loadTestStart;
        double throughput = 0;
        if (duration != 0) {
            throughput = queryCount * 1000.0 / duration;
        }        
        recordLoadTestResults(queryCount, averageLatency, duration, throughput);        
        printThroughputAndLatency(averageLatency, throughput);
    }

    private void recordLoadTestResults(int queryCount, double averageLatency, long duration, double throughput) {
        loadTestResults.add(new LoadTestResult(testThreadCount, duration, queryCount, throughput, averageLatency));
    }

    private void printThroughputAndLatency(double averageLatency, double throughput) {
        System.out.println( "===========" ); //$NON-NLS-1$
        System.out.println( "Throughput= " + throughput + " QPS"); //$NON-NLS-1$ //$NON-NLS-2$
        System.out.println( "Average Query Latency= " + averageLatency + " ms"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void loadTest(String command, int[] threadCounts) {
        loadTestResults = new ArrayList();
        loadTestName = DateUtil.getCurrentDateAsString().replace(':', '-');
        for (int i=0; i<threadCounts.length; i++) {
            resetTargetLoadTotals(this.shell);
            singleLoadTest(null, command, threadCounts[i]);
            processLoadTestSummary();
        }
     }
        
    public void loadTestWithSetup(String setupCommand, String command, int[] threadCounts) {
        loadTestResults = new ArrayList();
        loadTestName = setupCommand + "-" + command + "_" + DateUtil.getCurrentDateAsString().replace(':', '-'); //$NON-NLS-1$ //$NON-NLS-2$
        for (int i=0; i<threadCounts.length; i++) {
            resetTargetLoadTotals(this.shell);
            singleLoadTest(setupCommand, command, threadCounts[i]);
            processLoadTestSummary();
        }
     }
    
    public void singleLoadTest(String setupCommand, String command, int threadCount) {
        loadDriver();
        testThreadCount = threadCount;
        intervalQueryCount = 0;
        loadTestStart = 0;
        loadTestEnd = 0;
        latencyTimerElapsed = 0;
        if (setupCommand == null) {
            loadTestStart = System.currentTimeMillis();
        }
        LoadTestState state = null;
        try {
            waitingThreadCount = 0;
            state = executeOnThreads(setupCommand, command, threadCount);
            if (setupCommand != null) {
                waitForAllThreadsToPause(threadCount);
                loadTestStart = System.currentTimeMillis();
                signalThreads();     
                waitForThreads(state);       
            }
        } finally {
            if(loadTestStart > 0) {
                loadTestEnd = System.currentTimeMillis();
            }
            
            collectSubShellState(state);
        }
    }

    private void collectSubShellState(LoadTestState state) {
        // Collect counts from all shells used in the load test
        for(int i=0; i<state.shells.size(); i++) {
            CommandShell shell = (CommandShell) state.shells.get(i);
            MetaMatrixCommandTarget target = (MetaMatrixCommandTarget) shell.getTarget();
            
            this.queryCount += target.queryCount;
            this.latencyTimerElapsed += target.latencyTimerElapsed;
        }
    }
    
    private void waitForAllThreadsToPause(int threadCount) {
        while (true) {
            synchronized (lock) {
                log("waitForAllThreadsToPause.count=" + waitingThreadCount + " waiting for " + threadCount); //$NON-NLS-1$ //$NON-NLS-2$
                if (waitingThreadCount == threadCount) {
                    return;
                }
                try {
                    log("waitForAllThreadsToPause.wait"); //$NON-NLS-1$
                    lock.wait();
                } catch (InterruptedException e) {
                    //ignore
                }
            }
        }
    }
    
    private void signalThreads() {
        synchronized(lock) {
            log("signalThreads.notifyAll"); //$NON-NLS-1$
            waitingThreadCount = -1;
            lock.notifyAll();
        }
    }

    private LoadTestState executeOnThreads(String setupCommand, String command, int threadCount) {
        LoadTestState state = new LoadTestState();
        startThreads(setupCommand, command, threadCount, state);
        if (setupCommand == null) {
            waitForThreads(state);
        }
        return state;
    }

    private void startThreads(String setupCommand, String command, int threadCount, LoadTestState state) {
        for (int i=0; i<threadCount; i++) {
            // Defect 15973 - Make a copy of the target before starting the thread. If this target is called
            // after the creation of the thread, it's possible that the target (especially the script file stack)
            // can be in a bad state.
            MetaMatrixCommandTarget newTarget = this.copy();
            CommandShell newShell = shell.copy(newTarget);
            state.shells.add(newShell);
            newTarget.setShell(newShell);
            Thread testThread = new Thread(getRunnable(newShell, setupCommand, command));
            state.threadList.add(testThread);
            testThread.start();
        }
    }
    
    private void waitForThreads(LoadTestState state) {
        for (Iterator i = state.threadList.iterator(); i.hasNext(); ) {
            Thread testThread = (Thread) i.next();
            boolean joined = false;
            while (!joined) {
                try {
                    testThread.join();
                    joined = true;
                } catch (InterruptedException e) {
                    //ignore
                }
            }
        }
    }

    private Runnable getRunnable(final CommandShell commandShell, final String setupCommand, final String command) {
        return new Runnable() {  
            public void run() {
                runInNewShell(commandShell, setupCommand, command);
            }
        };
    }

    private void runInNewShell(CommandShell newShell, String setupCommand, String command) {
        if (setupCommand != null) {
            newShell.execute(setupCommand);
            waitForSignal();

            resetTargetLoadTotals(newShell);
        }
        newShell.execute(command);
    }
    
    private void resetTargetLoadTotals(CommandShell newShell) {
        // Reset query count to 0 to ignore setup queries
        MetaMatrixCommandTarget target = (MetaMatrixCommandTarget) newShell.getTarget();            
        target.queryCount = 0;
        target.latencyTimerElapsed = 0;
        target.intervalQueryCount = 0;
    }
    
    private void waitForSignal() {
        synchronized(lock) {
            waitingThreadCount = waitingThreadCount + 1;
            log("waitForSignal.notify"); //$NON-NLS-1$
            lock.notifyAll();
            boolean waiting = true;
            while (waiting) {
                try {
                    log("waitForSignal.wait"); //$NON-NLS-1$
                    lock.wait();
                    log("waitForSignal.count=" + waitingThreadCount); //$NON-NLS-1$
                    if (waitingThreadCount < 0) {
                        waiting = false;
                    }
                } catch (InterruptedException e) {
                    //ignore
                }
            }
        }
    }
    
    private void log(String text) {
        //System.out.println(text);
    }
    
    private MetaMatrixCommandTarget copy() {
        try {
            MetaMatrixCommandTarget result = (MetaMatrixCommandTarget) clone();
            return result;
        } catch (CloneNotSupportedException e) {
            throw new MetaMatrixRuntimeException(e);
        }
    }
    
    protected Object clone() throws CloneNotSupportedException {
        MetaMatrixCommandTarget result = (MetaMatrixCommandTarget) super.clone();
        if (result.connection != null) {
            result.connection = null;
            result.connect();
        }
        return result;
    } 
    
    // Display a query counter after every n queries.
    public void setQueryDisplayInterval(int n) {
        queryDisplayInterval = n;
    }
    
    /* 
     * @see com.metamatrix.cdk.BaseMetaMatrixCommandTarget#executeQueryDirect(com.metamatrix.cdk.BaseMetaMatrixCommandTarget.SqlRunnable)
     */
    protected boolean executeQueryDirect(SqlRunnable runnable) throws SQLException {
        long latencyTimerStart = System.currentTimeMillis();
        try {
            return super.executeQueryDirect(runnable);
        } finally {
            long latencyTimerEnd = System.currentTimeMillis();
            long elapsed = latencyTimerEnd-latencyTimerStart;
            latencyTimerElapsed += elapsed;            
            queryCount++;
            intervalQueryCount++;
            if (intervalQueryCount >= queryDisplayInterval) {
                intervalQueryCount = 0;
                System.out.println( "query count=" + queryCount); //$NON-NLS-1$
            }
        }
    }

}

class LoadTestResult {
    public int threadCount;
    public double throughput;
    public double latency;
    public int queryCount;
    public long duration;
    
    LoadTestResult(int threadCount, long duration, int queryCount, double throughput, double latency) {
        this.threadCount = threadCount;
        this.throughput = throughput;
        this.latency = latency;
        this.queryCount = queryCount;
        this.duration = duration;
    }
    
    public String toString() {
        return threadCount + "\t" + duration + "\t" + queryCount + "\t" + throughput + "\t" + latency; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }
    
    public static String getHeaderString() {
        return "ThreadCount\tDuration (ms)\tQueryCount\tThroughput (QPS)\tAverageLatency (ms)"; //$NON-NLS-1$
    }
}

class LoadTestState {
    List threadList = new ArrayList();
    List shells = new ArrayList();
    
}
