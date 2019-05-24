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

package org.teiid.jdbc;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.jdbc.EnhancedTimer.Task;

@SuppressWarnings("nls")
public class TestEnhancedTimer {

    private final class SimpleCancelTask implements Runnable {
        @Override
        public void run() {
        }
    }

    @Test public void testRemove() {
        EnhancedTimer ct = new EnhancedTimer("foo");
        SimpleCancelTask sct = new SimpleCancelTask();
        Task tt = ct.add(sct, 20000);
        Task tt1 = ct.add(sct, 20000);
        assertTrue(tt.compareTo(tt1) < 0);
        Task tt2 = ct.add(sct, 10000);
        assertEquals(3, ct.getQueueSize());
        tt.cancel();
        tt1.cancel();
        tt2.cancel();
        assertEquals(0, ct.getQueueSize());
    }

}
