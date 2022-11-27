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

package org.teiid.deployers;

import static org.junit.Assert.*;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.vdb.runtime.VDBKey;

@SuppressWarnings("nls")
public class TestVDBRepository {

    @Test public void testVDBRespositoryGetActive() throws Exception {
        VDBRepository repo = new VDBRepository();
        CompositeVDB mock1 = Mockito.mock(CompositeVDB.class);
        VDBMetaData vdb = new VDBMetaData();
        vdb.setName("name");
        vdb.setVersion(1);
        vdb.setConnectionType(ConnectionType.NONE);
        Mockito.stub(mock1.getVDB()).toReturn(vdb);
        repo.getVdbRepo().put(new VDBKey("name", 1), mock1);
        CompositeVDB mock2 = Mockito.mock(CompositeVDB.class);
        VDBMetaData vdb2 = new VDBMetaData();
        vdb2.setName("name");
        vdb2.setVersion(2);
        vdb2.setConnectionType(ConnectionType.NONE);
        Mockito.stub(mock2.getVDB()).toReturn(vdb2);
        repo.getVdbRepo().put(new VDBKey("name", 2), mock2);

        VDBMetaData live = repo.getLiveVDB("name");
        assertEquals("2", live.getVersion());
    }


}
