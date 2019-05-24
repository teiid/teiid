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

package org.teiid.query.tempdata;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.concurrent.Callable;

import org.junit.Test;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.processor.CollectionTupleSource;
import org.teiid.query.util.CommandContext;

public class TestAsyncTupleSource {

    @Test public void testTupleSource() throws TeiidComponentException, TeiidProcessingException {
        AsyncTupleSource ats = new AsyncTupleSource(new Callable<TupleSource>() {

            @Override
            public TupleSource call() throws Exception {
                return new CollectionTupleSource(Arrays.asList(Arrays.asList(1), Arrays.asList(2)).iterator());
            }
        }, new CommandContext());
        for (int i = 0; i < 20; i++) {
            try {
                assertEquals(Arrays.asList(1), ats.nextTuple());
                break;
            } catch (BlockedException e) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e1) {
                }
            }
        }
        assertEquals(Arrays.asList(2), ats.nextTuple());
        assertNull(ats.nextTuple());
    }

}
