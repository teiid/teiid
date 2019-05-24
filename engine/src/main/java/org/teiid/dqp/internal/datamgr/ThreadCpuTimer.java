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

package org.teiid.dqp.internal.datamgr;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;

/**
 * Timer class that uses the ThreadMXBean for CPU timing
 */
public class ThreadCpuTimer {

    private static ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    private int totalTime = -1;
    private long lastTime = -1;
    private boolean active;

    public ThreadCpuTimer() {
        active = threadMXBean.isThreadCpuTimeSupported()
                && threadMXBean.isThreadCpuTimeEnabled()
                && LogManager.isMessageToBeRecorded(LogConstants.CTX_COMMANDLOGGING, MessageLevel.DETAIL);
    }

    public void start() {
        if (!active) {
            return;
        }
        try {
            lastTime = threadMXBean.getCurrentThreadCpuTime();
        } catch (UnsupportedOperationException e) {
            inactivate();
        }

    }

    private void inactivate() {
        active = false;
        lastTime = -1;
        totalTime = -1;
    }

    public long stop() {
        if (!active) {
            return -1;
        }
        if (lastTime != -1) {
            try {
                long time = threadMXBean.getCurrentThreadCpuTime();
                if (time != -1) {
                    if (totalTime == -1) {
                        totalTime = 0;
                    }
                    totalTime += (time - lastTime);
                } else {
                    inactivate();
                }
            } catch (UnsupportedOperationException e) {
                inactivate();
            }
        }
        lastTime = -1;
        return totalTime;
    }

}
