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

import java.sql.Timestamp;

import javax.resource.spi.work.Work;

/**
 */
public class FakeWorkItem implements Work {

	private static boolean DEBUG = false;
	
    long begin = 0;
    long end = 0;
    private long waitTime;

    /**
     * Constructor for FakeWorker.
     */
    public FakeWorkItem(long waitTime) {
        this.waitTime = waitTime;
    }

    /**
     * @see com.metamatrix.common.queue.QueueWorker#process(Object)
     */
    public void run() {
        if(begin == 0) {
            begin = System.currentTimeMillis();
        }

        log("Processing"); //$NON-NLS-1$
        
        // Sleep for time       
        try { 
            Thread.sleep(waitTime);
        } catch(Exception e) {
        }
        
        end = System.currentTimeMillis();
        log("Done");    //$NON-NLS-1$
    }
    
    private void log(String msg) {
    	if (DEBUG) {
    		System.out.println((new Timestamp(System.currentTimeMillis())).toString() + " " +  //$NON-NLS-1$
    				Thread.currentThread().getName() + ": " + msg);     //$NON-NLS-1$
    	}
    }

	@Override
	public void release() {
		
	}

}
