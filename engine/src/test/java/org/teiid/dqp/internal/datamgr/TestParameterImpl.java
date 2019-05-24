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

import junit.framework.TestCase;

import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.language.Argument.Direction;

public class TestParameterImpl extends TestCase {

    /**
     * Constructor for TestParameterImpl.
     * @param name
     */
    public TestParameterImpl(String name) {
        super(name);
    }

    public static Argument example(int index) throws Exception {
        Call procImpl = TestProcedureImpl.example();
        return procImpl.getArguments().get(index);
    }

    public void testGetDirection() throws Exception {
        assertEquals(Direction.IN, example(0).getDirection());
        assertEquals(Direction.IN, example(1).getDirection());
    }

    public void testGetType() throws Exception {
        assertTrue(example(0).getType().equals(String.class));
        assertTrue(example(1).getType().equals(Integer.class));
    }

    public void testGetValue() throws Exception {
        assertEquals("x", example(0).getArgumentValue().getValue()); //$NON-NLS-1$
        assertEquals(new Integer(1), example(1).getArgumentValue().getValue());
    }

}
