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

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;

/**
 */
public class TestExecutionContextImpl {

    public ExecutionContextImpl createContext(long requestID, String partID) {
        return new ExecutionContextImpl("vdb", 1, null,   //$NON-NLS-1$
                                        "Connection", "Connector", requestID, partID, "0"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testEqivalenceSemanticsSame() {
        UnitTestUtil.helpTestEquivalence(0, createContext(100, "1"), createContext(100, "1")); //$NON-NLS-2$ //$NON-NLS-1$
    }

    @Test public void testEqivalenceSemanticsDifferentPart() {
        UnitTestUtil.helpTestEquivalence(1, createContext(100, "1"), createContext(100, "2")); //$NON-NLS-2$ //$NON-NLS-1$
    }

    @Test public void testEqivalenceSemanticsDifferentRequest() {
        UnitTestUtil.helpTestEquivalence(1, createContext(100, "1"), createContext(200, "1")); //$NON-NLS-2$ //$NON-NLS-1$
    }

}
