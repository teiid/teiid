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
package org.teiid.jboss;

import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.teiid.dqp.internal.process.TeiidExecutor;
import org.teiid.dqp.internal.process.ThreadReuseExecutor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;

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
        this.threadExecutor = new ThreadReuseExecutor("async-teiid-threads", this.threadCount) {  //$NON-NLS-1$
            @Override
            protected void logWaitMessage(long warnTime, int maximumPoolSize,
                    String poolName, int highestQueueSize) {
                LogManager.logWarning(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50116, maximumPoolSize, poolName, highestQueueSize, warnTime));
            }
        };
    }

    @Override
    public void stop(StopContext context) {
        this.threadExecutor.shutdown();
    }
}
