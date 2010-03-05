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

package com.metamatrix.connector.loopback;

import java.util.List;

import org.mockito.Mockito;
import org.teiid.connector.api.ConnectorLogger;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.ConnectorHost;
import com.metamatrix.cdk.unittest.FakeTranslationFactory;


/** 
 * @since 4.3
 */
public class TestLoobackAsynch extends TestCase {

    public void test() throws Exception {
        LoopbackConnector connector = new LoopbackConnector();

        LoopbackManagedConnectionFactory config = Mockito.mock(LoopbackManagedConnectionFactory.class);
        Mockito.stub(config.getWaitTime()).toReturn(200);
        Mockito.stub(config.getRowCount()).toReturn(1000);
        Mockito.stub(config.getPollIntervalInMilli()).toReturn(100L);
        Mockito.stub(config.getLogger()).toReturn(Mockito.mock(ConnectorLogger.class));
        
        ConnectorHost host = new ConnectorHost(connector, config, FakeTranslationFactory.getInstance().getBQTTranslationUtility());
        List results = host.executeCommand("SELECT intkey from bqt1.smalla"); //$NON-NLS-1$
        assertEquals(1000, results.size());
    }

}
