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

package org.teiid.translator.loopback;

import java.util.List;

import junit.framework.TestCase;

import org.teiid.cdk.api.ConnectorHost;
import org.teiid.cdk.unittest.FakeTranslationFactory;
import org.teiid.translator.loopback.LoopbackExecutionFactory;



/** 
 * @since 4.3
 */
public class TestLoobackAsynch extends TestCase {

    public void test() throws Exception {
        LoopbackExecutionFactory connector = new LoopbackExecutionFactory();
        connector.setWaitTime(200);
        connector.setRowCount(1000);
        connector.setPollIntervalInMilli(100L);
        
        ConnectorHost host = new ConnectorHost(connector, null, FakeTranslationFactory.getInstance().getBQTTranslationUtility());
        List results = host.executeCommand("SELECT intkey from bqt1.smalla"); //$NON-NLS-1$
        assertEquals(1000, results.size());
    }

}
