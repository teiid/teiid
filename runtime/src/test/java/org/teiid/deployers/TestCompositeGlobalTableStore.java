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

import java.util.LinkedHashMap;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.tempdata.GlobalTableStore;
import org.teiid.query.tempdata.GlobalTableStoreImpl;
import org.teiid.vdb.runtime.VDBKey;

@SuppressWarnings("nls")
public class TestCompositeGlobalTableStore {

    @Test public void testCompositeGlobalTableStore() throws VirtualDatabaseException {
        CompositeVDB vdb = TestCompositeVDB.createCompositeVDB(new MetadataStore(), "foo");
        GlobalTableStore gts = CompositeGlobalTableStore.createInstance(vdb, BufferManagerFactory.getStandaloneBufferManager(), null);
        assertTrue(gts instanceof GlobalTableStoreImpl);

        vdb.children = new LinkedHashMap<VDBKey, CompositeVDB>();
        MetadataStore ms = new MetadataStore();
        Schema s = new Schema();
        s.setName("x");
        ms.addSchema(s);
        CompositeVDB imported = TestCompositeVDB.createCompositeVDB(ms, "foo");
        GlobalTableStore gts1 = Mockito.mock(GlobalTableStore.class);
        imported.getVDB().addAttachment(GlobalTableStore.class, gts1);
        vdb.getChildren().put(new VDBKey("foo1", 1), imported);

        CompositeGlobalTableStore cgts = (CompositeGlobalTableStore)CompositeGlobalTableStore.createInstance(vdb, BufferManagerFactory.getStandaloneBufferManager(), null);
        assertEquals(gts1, cgts.getStoreForTable(RelationalPlanner.MAT_PREFIX + "X.Y"));
        assertEquals(cgts.getPrimary(), cgts.getStore("Z"));
    }

}
