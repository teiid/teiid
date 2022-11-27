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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {

    private AtomicInteger threadNumber = new AtomicInteger();
    private String threadBaseName;

    public NamedThreadFactory(String name) {
        this.threadBaseName = (name != null ? name : "Worker_"); //$NON-NLS-1$
    }

    public Thread newThread(Runnable r) {
        String threadName = threadBaseName + threadNumber.getAndIncrement();
        Thread t = new Thread(r, threadName);
        t.setDaemon(true);
        return t;
    }
}


