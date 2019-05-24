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
package org.teiid.sizing;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestSizingApplication {

    private Caculation tool = null;

    @Test
    public void testHeapCacualtion_1() {
        tool = new Caculation(1, 200, true);
        assertEquals(2, tool.heapCaculation());
    }

    @Test
    public void testHeapCacualtion_2() {
        tool = new Caculation(2, 200, true);
        assertEquals(3, tool.heapCaculation());
    }

    @Test
    public void testHeapCacualtion_3() {
        tool = new Caculation(4, 200, true);
        assertEquals(5, tool.heapCaculation());
    }

    @Test
    public void testHeapCacualtion_4() {
        tool = new Caculation(5, 200, true);
        assertEquals(6, tool.heapCaculation());
    }

    @Test
    public void testHeapCacualtion_5() {
        tool = new Caculation(1, 300, true);
        assertEquals(2, tool.heapCaculation());
    }

    @Test
    public void testHeapCacualtion_6() {
        tool = new Caculation(2, 300, true);
        assertEquals(4, tool.heapCaculation());
    }

    @Test
    public void testHeapCacualtion_7() {
        tool = new Caculation(4, 300, true);
        assertEquals(7, tool.heapCaculation());
    }

    @Test
    public void testHeapCacualtion_8() {
        tool = new Caculation(5, 300, true);
        assertEquals(8, tool.heapCaculation());
    }

    @Test
    public void testHeapCacualtion_9() {
        tool = new Caculation(1, 400, true);
        assertEquals(3, tool.heapCaculation());
    }

    @Test
    public void testHeapCacualtion_10() {
        tool = new Caculation(2, 400, true);
        assertEquals(5, tool.heapCaculation());
    }

    @Test
    public void testHeapCacualtion_11() {
        tool = new Caculation(4, 400, true);
        assertEquals(9, tool.heapCaculation());
    }

    @Test
    public void testHeapCacualtion_12() {
        tool = new Caculation(5, 400, true);
        assertEquals(11, tool.heapCaculation());
    }

    @Test
    public void testHeapCacualtion_13() {
        tool = new Caculation(5, 400, false);
        assertEquals(16, tool.heapCaculation());
    }

    @Test
    public void testCoreSizeCaculation_1() {
        tool = new Caculation(2, 500, 100, 10000, 100, 1000, 10000, 1000, 2000, false);
        assertEquals(22, tool.coreCaculation());
    }

    @Test
    public void testCoreSizeCaculation_2() {
        tool = new Caculation(2, 500, 100, 10000, 100, 500, 10000, 1000, 2000, false);
        assertEquals(50, tool.coreCaculation());
    }

    @Test
    public void testCoreSizeCaculation_3() {
        tool = new Caculation(2, 400, 200, 10000, 100, 500, 10000, 1000, 2000, true);
        assertEquals(128, tool.coreCaculation());
    }

    @Test
    public void testCoreSizeCaculation_4() {
        tool = new Caculation(2, 500, 500, 10000, 100, 500, 10000, 1000, 2000, false);
        assertEquals(128, tool.coreCaculation());
    }

}
