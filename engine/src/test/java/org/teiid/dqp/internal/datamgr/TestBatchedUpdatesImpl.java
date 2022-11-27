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

package org.teiid.dqp.internal.datamgr;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.teiid.language.BatchedUpdates;
import org.teiid.language.Delete;
import org.teiid.language.Insert;
import org.teiid.language.Update;
import org.teiid.query.sql.lang.BatchedUpdateCommand;



/**
 * @since 4.2
 */
@SuppressWarnings("nls")
public class TestBatchedUpdatesImpl {

    public static BatchedUpdateCommand helpExample() {
        List updates = new ArrayList();
        updates.add(TestInsertImpl.helpExample("a.b")); //$NON-NLS-1$
        updates.add(TestUpdateImpl.helpExample());
        updates.add(TestDeleteImpl.helpExample());
        return new BatchedUpdateCommand(updates);
    }

    public static BatchedUpdates example() throws Exception {
        return TstLanguageBridgeFactory.factory.translate(helpExample());
    }

    @Test
    public void testGetUpdateCommands() throws Exception {
        List updates = example().getUpdateCommands();
        assertEquals(3, updates.size());
        assertTrue(updates.get(0) instanceof Insert);
        assertTrue(updates.get(1) instanceof Update);
        assertTrue(updates.get(2) instanceof Delete);
    }

    @Test
    public void testToString() throws Exception {
        assertEquals("INSERT INTO b (e1, e2, e3, e4) VALUES (1, 2, 3, 4);\n"
                + "UPDATE g1 SET e1 = 1, e2 = 1, e3 = 1, e4 = 1 WHERE 1 = 1;\n"
                + "DELETE FROM g1 WHERE 100 >= 200 AND 500 < 600;", example().toString());
    }

}
