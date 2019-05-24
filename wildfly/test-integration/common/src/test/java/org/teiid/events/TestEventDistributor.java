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
package org.teiid.events;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.VDB;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.FakeServer;

@SuppressWarnings("nls")
public class TestEventDistributor {
    private static final String VDB = "PartsSupplier"; //$NON-NLS-1$

    @Test
    public void testEvents() throws Exception {
        FakeServer server = null;
        try {
            server = new FakeServer(true);
            EventListener events = Mockito.mock(EventListener.class);
            server.getEventDistributor().register(events);

            server.deployVDB(VDB, UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");

            Mockito.verify(events).vdbDeployed(VDB, "1");
            Mockito.verify(events).vdbLoaded((VDB)Mockito.any());

            server.undeployVDB(VDB);

            Mockito.verify(events).vdbDeployed(VDB, "1");
            Mockito.verify(events).vdbLoaded((VDB)Mockito.any());
            Mockito.verify(events).vdbUndeployed(VDB, "1");
        } finally {
            if (server != null) {
                server.stop();
            }
        }
    }

}
