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

package org.teiid.common.queue;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.resource.spi.work.Work;

import org.junit.Test;
import org.teiid.adminapi.impl.WorkerPoolStatisticsMetadata;
import org.teiid.dqp.internal.process.ThreadReuseExecutor;
import org.teiid.dqp.internal.process.DQPCore.FutureWork;

/**
 */
public class TestThreadReuseExecutor {
	
    @Test public void testQueuing() throws Exception {
        final long SINGLE_WAIT = 50;
        final int WORK_ITEMS = 10;
        final int MAX_THREADS = 5;

        final ThreadReuseExecutor pool = new ThreadReuseExecutor("test", MAX_THREADS); //$NON-NLS-1$
        
        for(int i=0; i<WORK_ITEMS; i++) {
            pool.execute(new FakeWorkItem(SINGLE_WAIT));
        }
        
        pool.shutdown();        
        pool.awaitTermination(1000, TimeUnit.MILLISECONDS);
        assertTrue(pool.isTerminated());
        WorkerPoolStatisticsMetadata stats = pool.getStats();
        assertEquals(10, stats.getTotalCompleted());
        assertEquals("Expected threads to be maxed out", MAX_THREADS, stats.getHighestActiveThreads()); //$NON-NLS-1$
    }

    @Test public void testThreadReuse() throws Exception {
        final long SINGLE_WAIT = 50;
        final long NUM_THREADS = 5;

        ThreadReuseExecutor pool = new ThreadReuseExecutor("test", 5); //$NON-NLS-1$
        
        for(int i=0; i<NUM_THREADS; i++) {            
        	pool.execute(new FakeWorkItem(SINGLE_WAIT));
            
            try {
                Thread.sleep(SINGLE_WAIT*3);
            } catch(InterruptedException e) {                
            }
        }
        
        pool.shutdown();                
        
        WorkerPoolStatisticsMetadata stats = pool.getStats();
        assertEquals("Expected 1 thread for serial execution", 1, stats.getHighestActiveThreads()); //$NON-NLS-1$
        
        pool.awaitTermination(1000, TimeUnit.MILLISECONDS);
    }
    
    @Test(expected=RejectedExecutionException.class) public void testShutdown() throws Exception {
    	ThreadReuseExecutor pool = new ThreadReuseExecutor("test", 5); //$NON-NLS-1$
        pool.shutdown();
    	pool.execute(new FakeWorkItem(1));
    }
    
    @Test public void testScheduleCancel() throws Exception {
    	ThreadReuseExecutor pool = new ThreadReuseExecutor("test", 5); //$NON-NLS-1$
    	ScheduledFuture<?> future = pool.scheduleAtFixedRate(new Runnable() {
    		@Override
    		public void run() {
    		}
    	}, 0, 5, TimeUnit.MILLISECONDS);
    	future.cancel(true);
    	assertFalse(future.cancel(true));    	
    }
    
    @Test public void testSchedule() throws Exception {
    	ThreadReuseExecutor pool = new ThreadReuseExecutor("test", 5); //$NON-NLS-1$
        final ArrayList<String> result = new ArrayList<String>(); 
    	pool.schedule(new Work() {
			
			@Override
			public void run() {
    			result.add("hello"); //$NON-NLS-1$
			}
			
			@Override
			public void release() {
				
			}
		}, 5, TimeUnit.MILLISECONDS);
    	Thread.sleep(100);
    	pool.shutdown();
    	pool.awaitTermination(1000, TimeUnit.MILLISECONDS);
    	assertEquals(1, result.size());
    }
    
    @Test(expected=ExecutionException.class) public void testScheduleException() throws Exception {
    	ThreadReuseExecutor pool = new ThreadReuseExecutor("test", 5); //$NON-NLS-1$
    	ScheduledFuture<?> future = pool.schedule(new Runnable() {
    		@Override
    		public void run() {
    			throw new RuntimeException();
    		}
    	}, 0, TimeUnit.MILLISECONDS);
    	future.get();
    }
    
    /**
     * Here each execution exceeds the period
     */
    @Test public void testScheduleRepeated() throws Exception {
    	ThreadReuseExecutor pool = new ThreadReuseExecutor("test", 5); //$NON-NLS-1$
    	final ArrayList<String> result = new ArrayList<String>();
    	ScheduledFuture<?> future = pool.scheduleAtFixedRate(new Runnable() {
    		@Override
    		public void run() {
    			result.add("hello"); //$NON-NLS-1$
    			try {
					Thread.sleep(70);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
    		}
    	}, 0, 30, TimeUnit.MILLISECONDS);
    	Thread.sleep(120);
    	future.cancel(true);
    	assertTrue(result.size() < 3);
    }
    
    @Test public void testFailingWork() throws Exception {
    	ThreadReuseExecutor pool = new ThreadReuseExecutor("test", 5); //$NON-NLS-1$
    	final AtomicInteger count = new AtomicInteger();
    	pool.execute(new Work() {
    		@Override
    		public void run() {
    			count.getAndIncrement();
    			throw new RuntimeException();
    		}
    		
    		@Override
    		public void release() {
    			
    		}
    	});
    	Thread.sleep(100);
    	assertEquals(1, count.get());
    }
    
    @Test public void testPriorities() throws Exception {
    	final ThreadReuseExecutor pool = new ThreadReuseExecutor("test", 1); //$NON-NLS-1$
    	FutureWork<Boolean> work1 = new FutureWork<Boolean>(new Callable<Boolean>() {
    		public Boolean call() throws Exception {
    			synchronized (pool) {
    				while (pool.getSubmittedCount() < 4) {
    					pool.wait();
    				}
				}
    			return true;
    		}
		}, 0);
    	final ConcurrentLinkedQueue<Integer> order = new ConcurrentLinkedQueue<Integer>();
    	FutureWork<Boolean> work2 = new FutureWork<Boolean>(new Callable<Boolean>() {
    		public Boolean call() throws Exception {
    			order.add(2);
    			return true;
    		}
		}, 2);
    	FutureWork<Boolean> work3 = new FutureWork<Boolean>(new Callable<Boolean>() {
    		public Boolean call() throws Exception {
    			order.add(3);
    			return false;
    		}
		}, 1);
    	Thread.sleep(20); //ensure a later timestamp
    	FutureWork<Boolean> work4 = new FutureWork<Boolean>(new Callable<Boolean>() {
    		public Boolean call() throws Exception {
    			order.add(4);
    			return false;
    		}
		}, 2);
    	pool.execute(work1);
    	pool.execute(work2);
    	pool.execute(work3);
    	pool.execute(work4);
    	synchronized (pool) {
        	pool.notifyAll();
		}
    	work1.get();
    	work2.get();
    	work3.get();
    	work4.get();
    	assertEquals(Integer.valueOf(3), order.remove());
    	assertEquals(Integer.valueOf(2), order.remove());
    	assertEquals(Integer.valueOf(4), order.remove());
    }
        
}
