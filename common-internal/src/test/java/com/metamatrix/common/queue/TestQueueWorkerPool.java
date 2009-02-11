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

package com.metamatrix.common.queue;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import junit.framework.*;

/**
 */
public class TestQueueWorkerPool extends TestCase {

    /**
     * Constructor for TestQueueWorkerPool.
     * @param arg0
     */
    public TestQueueWorkerPool(String arg0) {
        super(arg0);
    }
    
    public void testQueuing() throws Exception {
        final long SINGLE_WAIT = 50;
        final int WORK_ITEMS = 10;
        final int MAX_THREADS = 5;

        final WorkerPool pool = WorkerPoolFactory.newWorkerPool("test", MAX_THREADS, 120000); //$NON-NLS-1$
        
        for(int i=0; i<WORK_ITEMS; i++) {
            pool.execute(new FakeWorkItem(SINGLE_WAIT));
        }
        
        pool.shutdown();        
        pool.awaitTermination(1000, TimeUnit.MILLISECONDS);
        assertTrue(pool.isTerminated());
        WorkerPoolStats stats = pool.getStats();
        assertEquals(10, stats.totalCompleted);
        assertEquals("Expected threads to be maxed out", MAX_THREADS, stats.highestActiveThreads); //$NON-NLS-1$
    }

    public void testThreadReuse() throws Exception {
        final long SINGLE_WAIT = 50;
        final long NUM_THREADS = 5;

        final WorkerPool pool = WorkerPoolFactory.newWorkerPool("test", 5, 120000); //$NON-NLS-1$
        
        for(int i=0; i<NUM_THREADS; i++) {            
        	pool.execute(new FakeWorkItem(SINGLE_WAIT));
            
            try {
                Thread.sleep(SINGLE_WAIT*2);
            } catch(InterruptedException e) {                
            }
        }
        
        pool.shutdown();                
        
        WorkerPoolStats stats = pool.getStats();
        assertEquals("Expected 1 thread for serial execution", 1, stats.highestActiveThreads); //$NON-NLS-1$
        
        pool.awaitTermination(1000, TimeUnit.MILLISECONDS);
    }
    
    public void testShutdown() throws Exception {
    	final WorkerPool pool = WorkerPoolFactory.newWorkerPool("test", 5, 120000); //$NON-NLS-1$
        pool.shutdown();
        try {
        	pool.execute(new FakeWorkItem(1));
        	fail("expected exception"); //$NON-NLS-1$
        } catch (RejectedExecutionException e) {
        	
        }
    }
        
}
