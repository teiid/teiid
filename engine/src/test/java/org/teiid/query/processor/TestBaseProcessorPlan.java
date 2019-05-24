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

package org.teiid.query.processor;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.teiid.core.TeiidException;
import org.teiid.query.util.CommandContext;


public class TestBaseProcessorPlan {

    @Test public void testGetAndClearWarnings() {
        FakeProcessorPlan plan = new FakeProcessorPlan(Collections.emptyList(), Collections.emptyList());
        CommandContext cc = new CommandContext();
        plan.initialize(cc, null, null);
        TeiidException warning = new TeiidException("test"); //$NON-NLS-1$
        plan.addWarning(warning);

        List<Exception> warnings = cc.getAndClearWarnings();
        assertEquals("Did not get expected number of warnings", 1, warnings.size()); //$NON-NLS-1$
        assertEquals("Did not get expected warning", warning, warnings.get(0)); //$NON-NLS-1$
        assertNull("Did not clear warnings from plan", cc.getAndClearWarnings());         //$NON-NLS-1$
    }
}
