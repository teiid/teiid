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

package org.teiid.query.optimizer.capabilities;

import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;

import junit.framework.*;

/**
 */
public class TestBasicSourceCapabilities extends TestCase {

    public TestBasicSourceCapabilities(String name) {
        super(name);
    }

    public void testPutGet() {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();

        // Check initial state
        assertEquals("Default value for capability should be false", false, caps.supportsCapability(Capability.QUERY_FROM_JOIN_INNER)); //$NON-NLS-1$

        // Change state
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);

        // Check current state
        assertEquals("Changed value for capability should be true", true, caps.supportsCapability(Capability.QUERY_FROM_JOIN_INNER));         //$NON-NLS-1$
    }


}
