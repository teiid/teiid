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

package org.teiid.metadata;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.mockito.Mockito;

public class TestForeignKey {

    @Test
    public void testReferenceTableName() {
        Table table = Mockito.mock(Table.class);
        Mockito.stub(table.getName()).toReturn("table"); //$NON-NLS-1$

        KeyRecord pk = Mockito.mock(KeyRecord.class);
        Mockito.stub(pk.getParent()).toReturn(table);

        ForeignKey fk = new ForeignKey();
        fk.setPrimaryKey(pk);

        assertEquals("table", fk.getReferenceTableName()); //$NON-NLS-1$
    }
}
