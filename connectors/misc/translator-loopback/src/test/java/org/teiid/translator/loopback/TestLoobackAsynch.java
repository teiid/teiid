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
