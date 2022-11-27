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

package org.teiid.core.util;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

public class TestMultiArrayOutputStream {

    @Test public void testArrayWrites() throws IOException {
        MultiArrayOutputStream maos = new MultiArrayOutputStream(2);
        for (int i = 0; i < 10; i++) {
            int len = 1 << i;
            maos.write(new byte[len], 0, len);
        }
        assertEquals((1<<10)-1, maos.getCount());
        assertEquals(1, maos.getIndex());
    }

    @Test public void testCount() throws IOException {
        MultiArrayOutputStream maos = new MultiArrayOutputStream(2);
        for (int i = 0; i < 4; i++) {
            maos.write(i);
        }
        assertEquals(4, maos.getCount());
        assertEquals(2, maos.getIndex());

        maos = new MultiArrayOutputStream(2);
        for (int i = 0; i < 4; i++) {
            int len = 3;
            maos.write(new byte[len]);
        }
        assertEquals(12, maos.getCount());
        assertEquals(6, maos.getIndex());
    }

}
