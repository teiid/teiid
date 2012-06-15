/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
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
