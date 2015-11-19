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
package org.teiid.jboss;

import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.teiid.dqp.internal.process.TeiidExecutor;
import org.teiid.dqp.internal.process.ThreadReuseExecutor;

public class ThreadExecutorService implements Service<TeiidExecutor> {

    private int threadCount;
    private ThreadReuseExecutor threadExecutor;
    
    public ThreadExecutorService(int threadCount) {
        this.threadCount = threadCount;
    }
    
    @Override
    public TeiidExecutor getValue() throws IllegalStateException,
            IllegalArgumentException {
        return this.threadExecutor;
    }

    @Override
    public void start(StartContext context) throws StartException {
        this.threadExecutor = new ThreadReuseExecutor("async-teiid-threads", this.threadCount);
    }

    @Override
    public void stop(StopContext context) {
        this.threadExecutor.shutdown();
    }
}
