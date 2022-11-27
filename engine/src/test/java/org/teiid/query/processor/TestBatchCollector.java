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

package org.teiid.query.processor;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.processor.relational.FakeRelationalNode;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("nls")
public class TestBatchCollector {

    @Test public void testCollect() throws Exception {
        FakeRelationalNode sourceNode = new FakeRelationalNode(1, new List[] {
                Arrays.asList(1),
                Arrays.asList(1),
                Arrays.asList(1)
            }, 1);
        sourceNode.setElements(Arrays.asList(new ElementSymbol("x", null, DataTypeManager.DefaultDataClasses.INTEGER)));
        BatchCollector bc = new BatchCollector(sourceNode, BufferManagerFactory.getStandaloneBufferManager(), new CommandContext(), false);
        bc.collectTuples(true);
        assertEquals(1, bc.getTupleBuffer().getManagedRowCount());
        assertEquals(3, bc.collectTuples().getRowCount());
    }

}
