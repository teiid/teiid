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

package org.teiid.dqp.internal.process.multisource;

import java.util.ArrayList;
import java.util.List;

import org.teiid.dqp.internal.process.multisource.MultiSourceCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.AllCapabilities;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;

import junit.framework.TestCase;



/** 
 * @since 4.2
 */
public class TestMultiSourceCapabilitiesFinder extends TestCase {

    public void test1() throws Exception {
        // Set up a "real" finder
        final String SINGLE_MODEL = "singleModel"; //$NON-NLS-1$
        final String MULTI_MODEL = "multiModel";                //$NON-NLS-1$
        FakeCapabilitiesFinder realFinder = new FakeCapabilitiesFinder();
        realFinder.addCapabilities(SINGLE_MODEL, new AllCapabilities());
        realFinder.addCapabilities(MULTI_MODEL, new AllCapabilities());

        // Set up the multi source finder, which will dynamically override a few capabilities
        List models = new ArrayList();
        models.add(MULTI_MODEL);
        MultiSourceCapabilitiesFinder finder = new MultiSourceCapabilitiesFinder(realFinder, models);
        
        // Test the single model to show that it is not affected
        SourceCapabilities singleCaps = finder.findCapabilities(SINGLE_MODEL);
        assertEquals(true, singleCaps.supportsCapability(Capability.QUERY_UNION));
        assertEquals(true, singleCaps.supportsCapability(Capability.QUERY_ORDERBY));
        assertEquals(true, singleCaps.supportsCapability(Capability.QUERY_SELECT_DISTINCT));
        assertEquals(true, singleCaps.supportsCapability(Capability.QUERY_AGGREGATES_AVG));

        // Test the multi model to show that it IS affected
        SourceCapabilities multiCaps = finder.findCapabilities(MULTI_MODEL);
        assertEquals(false, multiCaps.supportsCapability(Capability.QUERY_UNION));
        assertEquals(false, multiCaps.supportsCapability(Capability.QUERY_ORDERBY));
        assertEquals(false, multiCaps.supportsCapability(Capability.QUERY_SELECT_DISTINCT));
        assertEquals(false, multiCaps.supportsCapability(Capability.QUERY_AGGREGATES_AVG));

    }
}
