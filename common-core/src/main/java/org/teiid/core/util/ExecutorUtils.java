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

package org.teiid.core.util;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExecutorUtils {

    /**
     * Creates a fixed thread pool with named daemon threads that will expire after 60 seconds of
     * inactivity.
     * @param nThreads
     * @param name
     */
    public static ExecutorService newFixedThreadPool(int nThreads, String name) {
        return newFixedThreadPool(nThreads, Integer.MAX_VALUE, name);
    }

    public static ExecutorService newFixedThreadPool(int nThreads, int maxQueue, String name) {
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(nThreads, nThreads,
                                      60L, TimeUnit.SECONDS,
                                      maxQueue==0?new SynchronousQueue<Runnable>():new LinkedBlockingQueue<Runnable>(maxQueue), new NamedThreadFactory(name));
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
