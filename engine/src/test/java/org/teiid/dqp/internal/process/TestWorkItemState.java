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

package org.teiid.dqp.internal.process;

import static org.junit.Assert.*;

import org.junit.Test;


public class TestWorkItemState {

    private class TestWorkItem extends AbstractWorkItem {

        private boolean isDone;
        private boolean callMoreWork;
        private boolean resumed;

        private TestWorkItem() {
            this(false, false);
        }

        private TestWorkItem(boolean done, boolean callMoreWork) {
            this.isDone = done;
            this.callMoreWork = callMoreWork;
        }

        @Override
        protected boolean isDoneProcessing() {
            return isDone;
        }

        @Override
        protected void process() {
            assertWorkingState();
            if (callMoreWork) {
                this.moreWork();
            }
        }

        @Override
        protected void resumeProcessing() {
            this.resumed = true;
        }

        @Override
        public String toString() {
            return "TestItem"; //$NON-NLS-1$
        }

        private void checkState(ThreadState expectedState) {
            assertEquals(expectedState, getThreadState());
        }

        private void assertIdleState() {
            checkState(ThreadState.IDLE);
        }

        private void assertMoreWorkState() {
            checkState(ThreadState.MORE_WORK);
        }

        private void assertWorkingState() {
            checkState(ThreadState.WORKING);
        }

        private void assertDoneState() {
            checkState(ThreadState.DONE);
        }
    }

    @Test public void testInitialState() {
        TestWorkItem item = new TestWorkItem();
        item.assertMoreWorkState();
    }

    @Test public void testGotoIdleState() {
        TestWorkItem item = new TestWorkItem();
        item.run();
        item.assertIdleState();
    }

    @Test public void testGotoMoreWorkState() {
        TestWorkItem item = new TestWorkItem();
        item.run();
        item.moreWork();
        item.assertMoreWorkState();
    }

    @Test public void testGotoWorkingState() {
        TestWorkItem item = new TestWorkItem();
        item.run();
        item.moreWork();
        item.run();
    }

    @Test public void testResume() {
        TestWorkItem item = new TestWorkItem();
        item.run();
        assertFalse(item.resumed);
        item.moreWork();
        assertTrue(item.resumed);
    }

    @Test public void testResumeDuringWorking() {
        TestWorkItem item = new TestWorkItem(false, true);
        assertFalse(item.resumed);
        item.run();
        assertTrue(item.resumed);
    }

    @Test public void testRunAfterDone() {
        TestWorkItem item = new TestWorkItem(true, false);
        item.run();
        item.assertDoneState();
        try {
            item.run();
            fail("exception expected"); //$NON-NLS-1$
        } catch (IllegalStateException e) {

        }
    }

    @Test public void testRunDuringIdle() {
        TestWorkItem item = new TestWorkItem();
        item.run();
        item.assertIdleState();
        try {
            item.run();
            fail("exception expected"); //$NON-NLS-1$
        } catch (IllegalStateException e) {

        }
    }

}
