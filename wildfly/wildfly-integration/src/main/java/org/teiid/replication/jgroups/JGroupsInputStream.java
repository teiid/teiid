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

package org.teiid.replication.jgroups;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class JGroupsInputStream extends InputStream {

    private long timeout = 15000;
    private volatile byte[] buf;
    private volatile int index=0;
    private ReentrantLock lock = new ReentrantLock();
    private Condition write = lock.newCondition();
    private Condition doneReading = lock.newCondition();

    public JGroupsInputStream(long timeout) {
        this.timeout = timeout;
    }

    @Override
    public int read() throws IOException {
        if (index < 0) {
            return -1;
        }
        if (buf == null) {
            lock.lock();
            try {
                long waitTime = TimeUnit.MILLISECONDS.toNanos(timeout);
                while (buf == null) {
                    waitTime = write.awaitNanos(waitTime);
                    if (waitTime <= 0) {
                        throw new IOException(new TimeoutException());
                    }
                }
                if (index < 0) {
                    return -1;
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
            while (buf != null) {
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