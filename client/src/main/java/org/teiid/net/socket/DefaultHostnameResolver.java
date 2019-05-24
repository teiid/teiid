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

package org.teiid.net.socket;

import java.net.InetAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.teiid.core.util.ExecutorUtils;

/**
 * Due to https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6450279 and similar,
 * attempts to call hostname resolution may block for considerable amounts of time
 * rather than require workarounds on the host system, this class will handle making
 * the call asynchronous.
 */
public class DefaultHostnameResolver {

    private class Resolver implements Callable<String> {
        private InetAddress addr;

        private Resolver(InetAddress addr) {
            this.addr = addr;
        }

        @Override
        public String call() throws Exception {
            String hostName = addr.getCanonicalHostName();
            addr = null;
            return hostName;
        }
    }

    //as this is only used on the client side, we aren't yet worried about
    //how many addresses will be resolved
    private ConcurrentHashMap<String, Future<String>> resolved = new ConcurrentHashMap<String, Future<String>>();
    private ExecutorService executor = ExecutorUtils.newFixedThreadPool(1, "resolver"); //$NON-NLS-1$

    /**
     * Resolve the given address in the given milliseconds or return null if it's not possible
     * @param addr
     * @param timeoutMillis
     * @return
     */
    public String resolve(final InetAddress addr, int timeoutMillis) {
        if (addr.isLoopbackAddress()) {
            return addr.getCanonicalHostName();
        }
        final String hostAddress = addr.getHostAddress();
        Future<String> hostName = resolved.get(hostAddress);
        if (hostName == null) {
            synchronized (this) {
                hostName = resolved.get(hostAddress);
                if (hostName == null) {
                    hostName = executor.submit(new Resolver(addr));
                    resolved.put(hostAddress, hostName);
                }
            }
        }

        try {
            return hostName.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.interrupted();
        } catch (ExecutionException e) {
        } catch (TimeoutException e) {
        }

        return null;
    }

}
