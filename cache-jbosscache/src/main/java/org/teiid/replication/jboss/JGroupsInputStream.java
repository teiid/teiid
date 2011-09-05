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

package org.teiid.replication.jboss;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class JGroupsInputStream extends InputStream {
	
	static long TIME_OUT = 15000; //TODO make configurable
	
    private volatile byte[] buf;
    private volatile int index=0;
    private ReentrantLock lock = new ReentrantLock();
    private Condition write = lock.newCondition();
    private Condition doneReading = lock.newCondition();
    
    @Override
    public int read() throws IOException {
        if (index < 0) {
        	return -1;
        }
        if (buf == null) {
        	lock.lock();
            try {
                write.await(TIME_OUT, TimeUnit.MILLISECONDS);
                if (index < 0) {
                	return -1;
                }
                if (buf == null) {
                	throw new IOException(new TimeoutException());
                }
            } catch(InterruptedException e) {
            	throw new IOException(e);
            } finally {
            	lock.unlock();
            }
        }
        if (index == buf.length) {
        	lock.lock();
        	try {
	        	buf = null;
	        	index = 0;
	        	doneReading.signal();
        	} finally {
        		lock.unlock();
        	}
        	return read();
        }
        return buf[index++] & 0xff;
    }
    
    @Override
    public void close() {
    	lock.lock();
    	try {
    		buf = null;
    		index = -1;
    		doneReading.signal();
    	} finally {
    		lock.unlock();
    	}
    }
    
    public void receive(byte[] bytes) throws InterruptedException {
    	lock.lock();
    	try {	
    		if (index == -1) {
    			return;
    		}
    		if (buf != null) {
    			doneReading.await();
    		}
    		if (index == -1) {
    			return;
    		}
    		buf = bytes;
    		if (bytes == null) {
    			index = -1;
    		}
    		write.signal();
    	} finally {
    		lock.unlock();
    	}
    }

}