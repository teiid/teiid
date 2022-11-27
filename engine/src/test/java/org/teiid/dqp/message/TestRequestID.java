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

package org.teiid.dqp.message;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.dqp.message.RequestID;

import junit.framework.TestCase;


/**
 */
public class TestRequestID extends TestCase {

    /**
     * Constructor for TestRequestID.
     * @param name
     */
    public TestRequestID(String name) {
        super(name);
    }

    public void testGetters1() {
        String connID = "100"; //$NON-NLS-1$
        long executionID = 200;
        RequestID r = new RequestID(connID, executionID);
        assertEquals("Lost connectionID", connID, r.getConnectionID()); //$NON-NLS-1$
        assertEquals("Lost executionID", executionID, r.getExecutionID()); //$NON-NLS-1$
        assertEquals("Wrong string representation", "100.200", r.toString()); //$NON-NLS-1$ //$NON-NLS-2$

    }

    public void testGetters2() {
        long executionID = 200;
        RequestID r = new RequestID(executionID);
        assertEquals("Lost connectionID", null, r.getConnectionID()); //$NON-NLS-1$
        assertEquals("Lost executionID", executionID, r.getExecutionID()); //$NON-NLS-1$
        assertEquals("Wrong string representation", "C.200", r.toString()); //$NON-NLS-1$ //$NON-NLS-2$

    }

    public void testEquivalence1() {
        RequestID r1 = new RequestID("100", 200); //$NON-NLS-1$
        UnitTestUtil.helpTestEquivalence(0, r1, r1);
    }

    public void testEquivalence2() {
        RequestID r1 = new RequestID("100", 200); //$NON-NLS-1$
        RequestID r2 = new RequestID("100", 200); //$NON-NLS-1$
        UnitTestUtil.helpTestEquivalence(0, r1, r2);
    }

    public void testEquivalence3() {
        RequestID r1 = new RequestID("101", 200); //$NON-NLS-1$
        RequestID r2 = new RequestID("100", 200); //$NON-NLS-1$
        UnitTestUtil.helpTestEquivalence(1, r1, r2);
    }

    public void testEquivalence4() {
        RequestID r1 = new RequestID("100", 200); //$NON-NLS-1$
        RequestID r2 = new RequestID("100", 201); //$NON-NLS-1$
        UnitTestUtil.helpTestEquivalence(1, r1, r2);
    }

    public void testEquivalence7() {
        RequestID r1 = new RequestID(200);
        RequestID r2 = new RequestID("100", 200); //$NON-NLS-1$
        UnitTestUtil.helpTestEquivalence(1, r1, r2);
    }

    public void testSerialize1() throws Exception {
        RequestID copy = UnitTestUtil.helpSerialize(new RequestID("1", 100)); //$NON-NLS-1$

        assertEquals("1", copy.getConnectionID()); //$NON-NLS-1$
        assertEquals(100, copy.getExecutionID());
        assertEquals("1.100", copy.toString()); //$NON-NLS-1$
    }

    public void testSerialize2() throws Exception {
        RequestID copy = UnitTestUtil.helpSerialize(new RequestID(100));

        assertEquals(null, copy.getConnectionID());
        assertEquals(100, copy.getExecutionID());
        assertEquals("C.100", copy.toString()); //$NON-NLS-1$
    }

}
