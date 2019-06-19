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

package org.teiid.common.queue;

import java.sql.Timestamp;

/**
 */
public class FakeWorkItem implements Runnable {

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

}
