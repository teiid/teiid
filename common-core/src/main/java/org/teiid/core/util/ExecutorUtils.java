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

package org.teiid.core.util;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExecutorUtils {
    
	/**
	 * Creates a fixed thread pool with named daemon threads that will expire after 60 seconds of
	 * inactivity.
	 * @param nThreads
	 * @param name
	 * @return
	 */
    public static ExecutorService newFixedThreadPool(int nThreads, String name) {
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(nThreads, nThreads,
                                      60L, TimeUnit.SECONDS,
                                      new LinkedBlockingQueue<Runnable>(), new NamedThreadFactory(name));
        tpe.allowCoreThreadTimeOut(true);
        return tpe;
    }
    
    private static Executor direct = new Executor() {
		
		@Override
		public void execute(Runnable command) {
			command.run();			
		}
	};
    
    public static Executor getDirectExecutor() {
    	return direct;
    }
}
