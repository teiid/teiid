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

package org.teiid.common.buffer;

import java.util.ArrayList;
import java.util.List;

import org.teiid.common.buffer.TupleBatch;

import junit.framework.TestCase;


public class TestTupleBatch extends TestCase {

    public TestTupleBatch(String name) {
        super(name);
    }

    public TupleBatch exampleBatch(int rowBegin, int numRows, int numColumns) {
        List rows = new ArrayList();
        for(int i=0; i<numRows; i++) {
            List row = new ArrayList();
            for(int j=0; j<numColumns; j++) {
                row.add("data-" + (rowBegin + i) + "-" + j); //$NON-NLS-1$ //$NON-NLS-2$
            }
            rows.add(row);
        }
        return new TupleBatch(rowBegin, rows);
    }

    /*
     * Test for void TupleBatch(int, List)
     */
    public void testTupleBatch() {
        exampleBatch(0, 10, 2);
    }

}
