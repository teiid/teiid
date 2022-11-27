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

package org.teiid.translator.jdbc.derby;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.util.Version;

public class TestDerbyCapabilities {

    @Test public void testLimitSupport() {
        DerbyExecutionFactory derbyCapabilities = new DerbyExecutionFactory();
        derbyCapabilities.setDatabaseVersion(Version.DEFAULT_VERSION);
        assertFalse(derbyCapabilities.supportsRowLimit());
        derbyCapabilities.setDatabaseVersion(DerbyExecutionFactory.TEN_5.toString());
        assertTrue(derbyCapabilities.supportsRowLimit());
    }

    @Test public void testFunctionSupport() {
        DerbyExecutionFactory derbyCapabilities = new DerbyExecutionFactory();
        derbyCapabilities.setDatabaseVersion(Version.DEFAULT_VERSION);
        assertEquals(27, derbyCapabilities.getSupportedFunctions().size());
        derbyCapabilities.setDatabaseVersion(DerbyExecutionFactory.TEN_4.toString());
        assertEquals(44, derbyCapabilities.getSupportedFunctions().size());
    }

}
